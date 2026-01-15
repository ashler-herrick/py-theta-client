"""Base class for queue-based worker patterns."""

import logging
import threading
from abc import ABC, abstractmethod
from queue import Queue, Empty
from typing import Optional

from theta_client.job import Job


logger = logging.getLogger(__name__)


class QueueWorker(ABC):
    """Abstract base class for queue-based worker patterns.

    Provides common functionality for workers that process jobs from an input queue
    and optionally output results to an output queue. Supports both single-threaded
    and multi-threaded operation.

    Subclasses must implement the `process()` method to define their specific
    processing logic.
    """

    def __init__(self, num_threads: int = 1) -> None:
        """Initialize the queue worker.

        Args:
            num_threads: Number of worker threads to spawn. Default is 1.
            outputs_results: Whether this worker outputs results to an output queue.
                Terminal workers (like FileWriter) should set this to False.
        """
        self.input_queue: Queue[Job] = Queue()
        self._running: bool = False
        self.num_threads: int = num_threads
        self._threads: list[threading.Thread] = []
        self._exception: Optional[Exception] = None
        self._exception_lock = threading.Lock()
        self._chained_worker: Optional["QueueWorker"] = None

    @abstractmethod
    def process(self, job: Job) -> Optional[Job]:
        """Process a single job."""
        pass

    def add_job(self, job: Job) -> None:
        """Add a job to the input queue"""
        self.input_queue.put(job)

    def wait_for_completion(self) -> None:
        """Wait for all jobs in the input queue to be processed."""
        self.input_queue.join()

    def chain_to(self, next_worker: "QueueWorker") -> "QueueWorker":
        """Chain this worker's output to another worker's input."""
        self._chained_worker = next_worker
        return next_worker

    def check_for_errors(self) -> None:
        """Check if any worker thread encountered an exception and raise it."""
        with self._exception_lock:
            if self._exception is not None:
                raise self._exception

    def _work(self) -> None:
        """Worker loop that processes jobs from the input queue."""
        while self._running:
            try:
                job = self.input_queue.get(timeout=0.1)
            except Empty:
                continue

            try:
                processed_job = self.process(job)

                # Forward to chained worker if configured
                if self._chained_worker is not None and processed_job is not None:
                    self._chained_worker.add_job(processed_job)

            except Exception as e:
                logger.error(f"Error processing job: {e}", exc_info=True)
                with self._exception_lock:
                    if self._exception is None:  # Store first exception only
                        self._exception = e
                self._running = False  # Stop all workers
                break
            finally:
                self.input_queue.task_done()

    def start(self) -> None:
        """Start the worker threads."""
        if self._running:
            return

        self._running = True
        self._exception = None
        logger.info(
            f"Starting {self.__class__.__name__} with {self.num_threads} worker thread(s)."
        )

        for i in range(self.num_threads):
            thread = threading.Thread(
                target=self._work,
                daemon=True,
                name=f"{self.__class__.__name__}-{i}",
            )
            thread.start()
            self._threads.append(thread)

    def clear_queue(self, q) -> None:
        if not q:
            return
        while not q.empty():
            try:
                q.get_nowait()
            except:
                break

    def stop(self) -> None:
        """Stop the worker threads gracefully."""
        logger.info(f"Stopping {self.__class__.__name__}...")
        self._running = False

        for thread in self._threads:
            thread.join(timeout=2.0)

        self._threads.clear()
        self.clear_queue(self.input_queue)
