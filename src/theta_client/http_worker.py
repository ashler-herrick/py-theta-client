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

    def __init__(self, num_threads: int = 4) -> None:
        """Initialize the HTTP worker.

        Args:
            num_threads: Number of concurrent HTTP worker threads. Default is 4.
        """
        super().__init__(num_threads=num_threads)
        self.httpx_client = httpx.Client(
            timeout=120,
            limits=httpx.Limits(
                max_connections=self.num_threads,
                max_keepalive_connections=self.num_threads,
            ),
            transport=httpx.HTTPTransport(retries=0),
            http2=True,
        )

    def process(self, job: Job) -> Optional[Job]:
        """Fetch data from the HTTP endpoint specified in the job.

        Args:
            job: The job containing the URL to fetch

        Returns:
            The job with csv_buffer populated (or None if no data)
        """
        start_time = time.time()
        logger.debug(f"Processing job for URL: {job.url}")

        response = self.httpx_client.get(job.url)
        duration_ms = (time.time() - start_time) * 1000

        # If no data just return None for the buffer
        if response.status_code == 472:
            response_text = response.text
            if (
                "No data found for your request" in response_text
            ):
                logger.warning(f"No data response from {job.url} ({duration_ms:.1f}ms)")
                job.csv_buffer = None
                return job

        response.raise_for_status()
        job.csv_buffer = BytesIO(response.content)
        logger.debug(f"Processing complete for URL: {job.url} in {duration_ms:.1f}ms")
        return job

    def stop(self) -> None:
        """Stop the HTTP worker and close the HTTP client."""
        super().stop()
        self.httpx_client.close()
