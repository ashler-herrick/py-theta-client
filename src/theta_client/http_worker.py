import logging
import time
from io import BytesIO
from typing import Optional

import httpx

from theta_client.job import Job
from theta_client.queue_worker import QueueWorker

logger = logging.getLogger(__name__)


class HTTPWorker(QueueWorker):
    """Worker that fetches data from HTTP endpoints concurrently."""

    def __init__(self, num_threads: int = 4, max_retries: int = 2) -> None:
        """Initialize the HTTP worker.

        Args:
            num_threads: Number of concurrent HTTP worker threads. Default is 4.
            max_retries: Max retries for transient errors (5xx, timeouts). Default is 2.
        """
        super().__init__(num_threads=num_threads)
        self.max_retries = max_retries
        self.httpx_client: Optional[httpx.Client] = None

    def start(self) -> None:
        """Start the HTTP worker threads and create a fresh HTTP client."""
        self.httpx_client = httpx.Client(
            timeout=httpx.Timeout(300.0, connect=30.0),
            limits=httpx.Limits(
                max_connections=self.num_threads,  # Must match server connection limit
                max_keepalive_connections=self.num_threads,
            ),
            transport=httpx.HTTPTransport(retries=0),
            # Use HTTP/1.1 instead of HTTP/2 to ensure strict concurrency control.
            # The server enforces a 4 concurrent request limit. HTTP/2 multiplexing
            # allows multiple streams per connection, which could exceed this limit.
            # HTTP/1.1 with max_connections=4 guarantees exactly 4 concurrent requests.
            http2=False,
        )
        super().start()

    def process(self, job: Job) -> Optional[Job]:
        """Fetch data from the HTTP endpoint specified in the job.

        Args:
            job: The job containing the URL to fetch

        Returns:
            The job with csv_buffer populated (or None if no data/timeout)
        """
        start_time = time.time()
        logger.debug(f"Processing job for URL: {job.url}")

        for attempt in range(1 + self.max_retries):
            try:
                logger.debug(f"Acquiring connection for {job.url}")
                response = self.httpx_client.get(job.url)
                logger.debug(f"Connection acquired and request completed for {job.url}")
                duration_ms = (time.time() - start_time) * 1000

                # If no data just return None for the buffer
                if response.status_code == 472:
                    response_text = response.text
                    if "No data found for your request" in response_text:
                        logger.warning(f"No data response from {job.url} ({duration_ms:.1f}ms)")
                        job.csv_buffer = None
                        if self.counters:
                            self.counters.inc_http()
                        return job

                response.raise_for_status()
                job.csv_buffer = BytesIO(response.content)
                logger.debug(f"Processing complete for URL: {job.url} in {duration_ms:.1f}ms")

                if self.counters:
                    self.counters.inc_http()
                return job

            except (httpx.HTTPStatusError, httpx.TimeoutException) as e:
                # Don't retry 4xx errors — they're client errors, not transient
                if isinstance(e, httpx.HTTPStatusError) and e.response.status_code < 500:
                    raise

                if attempt < self.max_retries:
                    delay = 2 ** attempt  # 1s, 2s
                    logger.warning(
                        f"Attempt {attempt + 1}/{1 + self.max_retries} failed for {job.url}: {e}. "
                        f"Retrying in {delay}s..."
                    )
                    time.sleep(delay)
                else:
                    duration_ms = (time.time() - start_time) * 1000
                    logger.warning(
                        f"All {1 + self.max_retries} attempts failed for {job.url} "
                        f"({duration_ms:.1f}ms). Treating as no data."
                    )
                    job.csv_buffer = None
                    if self.counters:
                        self.counters.inc_http()
                    return job

            except httpx.ConnectError as e:
                duration_ms = (time.time() - start_time) * 1000
                logger.error(
                    f"Connection failed for {job.url} after {duration_ms:.1f}ms: {e}"
                )
                raise

    def stop(self) -> None:
        """Stop the HTTP worker and close the HTTP client."""
        super().stop()
        if self.httpx_client is not None:
            self.httpx_client.close()
            self.httpx_client = None
