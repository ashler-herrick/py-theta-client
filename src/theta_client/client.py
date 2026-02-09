import logging
import time
import threading
from pathlib import Path
from logging.handlers import RotatingFileHandler
from typing import Optional

from theta_client.file_writer import FileWriter, MinIOConfig
from theta_client.http_worker import HTTPWorker
from theta_client.response_processor import ResponseProcessor
from theta_client.requests import OptionRequest, StockRequest
from theta_client.job import FileWriteJob, Job
from theta_client.metrics import MetricsCollector
from theta_client.progress_display import ProgressDisplay

Request = OptionRequest | StockRequest

logger = logging.getLogger(__name__)


class ThetaClient:
    _instance = None
    _lock = threading.Lock()

    def __init__(
        self,
        num_threads: int,
        storage_config: MinIOConfig,
        log_level: str = "INFO",
        show_progress: bool = True,
    ):
        """Initialize the ThetaClient.

        Args:
            num_threads: Number of HTTP worker threads to use
            storage_config: MinIO storage configuration
            log_level: Log level for theta_client modules. Valid values:
                "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL".
                Default is "INFO".
            show_progress: Whether to show live progress display. Default is True.
        """
        # Configure logging for theta_client modules
        self._configure_logging(log_level, console=not show_progress)

        self.num_threads = num_threads
        self.show_progress = show_progress

        # Initialize metrics collector if progress display is enabled
        self._metrics: Optional[MetricsCollector] = (
            MetricsCollector() if show_progress else None
        )
        self._progress_display: Optional[ProgressDisplay] = (
            ProgressDisplay(self._metrics) if self._metrics else None
        )

        # Initialize workers with optional metrics
        self.file_writer = FileWriter(storage_config, metrics=self._metrics)
        self.http_worker = HTTPWorker(num_threads=num_threads, metrics=self._metrics)
        self.response_processor = ResponseProcessor(metrics=self._metrics)
        self._running = False
        self.http_worker.chain_to(self.response_processor).chain_to(self.file_writer)

    # Might want to break this out honestly
    def _configure_logging(
        self,
        log_level: str,
        log_dir: str | Path = "./logs",
        console: bool = True,
        max_bytes: int = 10 * 1024 * 1024,  # 10MB default
        backup_count: int = 5,
    ) -> None:
        """Configure logging for all theta_client modules.

        Args:
            log_level: Console log level as a string
            log_dir: Directory for log files (default: ./logs)
            console: Whether to log to console (default: True)
            max_bytes: Maximum size per log file before rotation (default: 10MB)
            backup_count: Number of backup files to keep (default: 5)
        """
        # Convert string to logging level, default to INFO if invalid
        level = getattr(logging, log_level.upper(), None)
        if level is None:
            level = logging.INFO
            logging.warning(
                f"Invalid log level '{log_level}', defaulting to INFO. "
                f"Valid levels: DEBUG, INFO, WARNING, ERROR, CRITICAL"
            )

        # Create log directory if it doesn't exist
        log_path = Path(log_dir)
        log_path.mkdir(parents=True, exist_ok=True)

        log_file = log_path / "theta-client-debug.log"

        # Get the theta_client root logger
        theta_logger = logging.getLogger("theta_client")
        theta_logger.setLevel(
            logging.DEBUG
        )  # Set to DEBUG to allow all messages through

        # Clear existing handlers to avoid duplicates
        theta_logger.handlers.clear()

        # Create formatter
        formatter = logging.Formatter(
            "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # Add console handler with user-specified level
        if console:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(level)
            console_handler.setFormatter(formatter)
            theta_logger.addHandler(console_handler)

        # Add rotating file handler - ALWAYS at WARNING level
        file_handler = RotatingFileHandler(
            log_file, maxBytes=max_bytes, backupCount=backup_count
        )
        file_handler.setLevel(logging.DEBUG)  # File always captures DEBUG+
        file_handler.setFormatter(formatter)
        theta_logger.addHandler(file_handler)

        # Prevent propagation to root logger
        theta_logger.propagate = False

    # Singleton to prevent too many requests being sent.
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def _start(self) -> None:
        if self._running:
            return

        self._running = True
        logger.info(f"Starting BetEdge client with {self.num_threads} worker threads")
        self.http_worker.start()
        self.response_processor.start()
        self.file_writer.start()

    def _stop(self) -> None:
        self._running = False
        self.http_worker.stop()
        self.response_processor.stop()
        self.file_writer.stop()

    def request_data(self, request: Request) -> None:
        self._start()
        logger.info(f"Processing {type(request).__name__}. Parameters: {request}")
        start_time = time.time()
        key_map = request.get_key_map()
        schema = request.get_schema()

        # Calculate total HTTP requests and files for metrics
        total_http_requests = 0
        total_files = 0
        files_to_process: list[tuple[str, list[str]]] = []

        for object_key, url_list in key_map.items():
            if not request.force_refresh and self.file_writer.file_exists(object_key):
                logger.debug(f"Skipping existing file: {object_key}")
                continue
            else:
                files_to_process.append((object_key, url_list))
                total_http_requests += len(url_list)
                total_files += 1

        # Initialize metrics tracking
        if self._metrics:
            self._metrics.start_request(
                request_type=type(request).__name__,
                symbol=request.symbol,
                start_date=request.start_date,
                end_date=request.end_date,
                endpoint=request.endpoint.value,
                total_http_requests=total_http_requests,
                total_files=total_files,
            )

        # Start progress display
        if self._progress_display:
            self._progress_display.start()

        # Start queue monitoring thread for diagnostics
        monitoring_active = threading.Event()
        monitoring_active.set()

        def monitor_queues():
            while monitoring_active.is_set():
                logger.debug(
                    f"Queue depths - HTTP: {self.http_worker.input_queue.qsize()}, "
                    f"Processor: {self.response_processor.input_queue.qsize()}, "
                    f"Writer: {self.file_writer.input_queue.qsize()}"
                )
                time.sleep(1.0)

        monitor_thread = threading.Thread(target=monitor_queues, daemon=True)
        monitor_thread.start()

        try:
            # Create jobs for files that need processing
            for object_key, url_list in files_to_process:
                file_write_job = FileWriteJob(
                    object_key=object_key, total_items=len(url_list)
                )
                logger.debug(
                    f"Creating {len(url_list)} HTTP jobs for file: {object_key}"
                )
                for url in url_list:
                    job = Job(
                        url=url,
                        schema=schema,
                        csv_buffer=None,
                        file_write_job=file_write_job,
                    )
                    self.http_worker.add_job(job)

            # Wait for all jobs to flow through the pipeline and complete
            logger.info("Waiting for all jobs to complete...")
            self.http_worker.wait_for_completion()
            self.http_worker.check_for_errors()
            self.response_processor.wait_for_completion()
            self.response_processor.check_for_errors()
            self.file_writer.wait_for_completion()
            self.file_writer.check_for_errors()
            duration_s = time.time() - start_time
            logger.info(f"All jobs completed successfully in {duration_s:.2f} seconds")

        except Exception as e:
            logger.error(f"Error during job processing: {e}")
            self._stop()
            raise

        finally:
            # Stop queue monitoring
            monitoring_active.clear()
            monitor_thread.join(timeout=2.0)

            # Stop progress display and show summary
            if self._progress_display:
                self._progress_display.stop()
            if self._metrics:
                self._metrics.end_request()
