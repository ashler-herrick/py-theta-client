"""Thread-safe metrics collection for ThetaClient progress tracking."""

import threading
import time
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class RequestMetrics:
    """Metrics for a single request being processed."""

    # Request identification
    request_type: str = ""
    symbol: str = ""
    start_date: int = 0
    end_date: int = 0
    endpoint: str = ""

    # Counters
    total_http_requests: int = 0
    http_completed: int = 0
    total_files: int = 0
    files_written: int = 0
    rows_processed: int = 0

    # Timing
    start_time: float = 0.0
    http_response_times: list[float] = field(default_factory=list)

    @property
    def elapsed_time(self) -> float:
        """Elapsed time in seconds since request started."""
        if self.start_time == 0:
            return 0.0
        return time.time() - self.start_time

    @property
    def avg_response_time_ms(self) -> float:
        """Average HTTP response time in milliseconds."""
        if not self.http_response_times:
            return 0.0
        return sum(self.http_response_times) / len(self.http_response_times)

    @property
    def http_progress_pct(self) -> float:
        """HTTP completion percentage."""
        if self.total_http_requests == 0:
            return 0.0
        return (self.http_completed / self.total_http_requests) * 100

    @property
    def rows_per_second(self) -> float:
        """Rows processed per second."""
        elapsed = self.elapsed_time
        if elapsed == 0:
            return 0.0
        return self.rows_processed / elapsed


class MetricsCollector:
    """Thread-safe singleton for collecting request metrics."""

    _instance: Optional["MetricsCollector"] = None
    _lock = threading.Lock()
    _metrics_lock = threading.Lock()

    def __new__(cls) -> "MetricsCollector":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    instance = super().__new__(cls)
                    instance._metrics = RequestMetrics()
                    instance._metrics_lock = threading.Lock()
                    cls._instance = instance
        return cls._instance

    def start_request(
        self,
        request_type: str,
        symbol: str,
        start_date: int,
        end_date: int,
        endpoint: str,
        total_http_requests: int,
        total_files: int,
    ) -> None:
        """Initialize tracking for a new request.

        Args:
            request_type: Type of request (e.g., "OptionRequest")
            symbol: Stock/option symbol being requested
            start_date: Start date in YYYYMMDD format
            end_date: End date in YYYYMMDD format
            endpoint: API endpoint being queried
            total_http_requests: Total number of HTTP requests to make
            total_files: Total number of files to write
        """
        with self._metrics_lock:
            self._metrics = RequestMetrics(
                request_type=request_type,
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                endpoint=endpoint,
                total_http_requests=total_http_requests,
                total_files=total_files,
                start_time=time.time(),
            )

    def record_http_response(self, duration_ms: float) -> None:
        """Record an HTTP response completion.

        Args:
            duration_ms: Response time in milliseconds
        """
        with self._metrics_lock:
            self._metrics.http_completed += 1
            self._metrics.http_response_times.append(duration_ms)

    def record_rows_processed(self, row_count: int) -> None:
        """Record rows processed from a response.

        Args:
            row_count: Number of rows processed
        """
        with self._metrics_lock:
            self._metrics.rows_processed += row_count

    def record_file_written(self) -> None:
        """Record a file write completion."""
        with self._metrics_lock:
            self._metrics.files_written += 1

    def get_snapshot(self) -> RequestMetrics:
        """Get a thread-safe snapshot of current metrics.

        Returns:
            A copy of the current RequestMetrics
        """
        with self._metrics_lock:
            return RequestMetrics(
                request_type=self._metrics.request_type,
                symbol=self._metrics.symbol,
                start_date=self._metrics.start_date,
                end_date=self._metrics.end_date,
                endpoint=self._metrics.endpoint,
                total_http_requests=self._metrics.total_http_requests,
                http_completed=self._metrics.http_completed,
                total_files=self._metrics.total_files,
                files_written=self._metrics.files_written,
                rows_processed=self._metrics.rows_processed,
                start_time=self._metrics.start_time,
                http_response_times=list(self._metrics.http_response_times),
            )

    def end_request(self) -> RequestMetrics:
        """End the current request and return final metrics.

        Returns:
            Final RequestMetrics snapshot
        """
        return self.get_snapshot()

    def reset(self) -> None:
        """Reset metrics to initial state."""
        with self._metrics_lock:
            self._metrics = RequestMetrics()

    @classmethod
    def reset_instance(cls) -> None:
        """Reset the singleton instance (primarily for testing)."""
        with cls._lock:
            cls._instance = None
