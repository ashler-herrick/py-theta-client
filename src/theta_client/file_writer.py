import logging
from io import BytesIO
from dataclasses import dataclass
from typing import Optional, TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq
from theta_client.job import Job
from theta_client.queue_worker import QueueWorker
from minio import Minio, S3Error

if TYPE_CHECKING:
    from theta_client.metrics import MetricsCollector

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class MinIOConfig:
    """Configuration for MinIO S3-compatible object storage."""

    endpoint: str = "localhost:9000"
    access_key: str = "minioadmin"
    secret_key: str = "minioadmin123"
    raw_bucket: str = "theta-client-data"
    secure: bool = False


class FileWriter(QueueWorker):
    """Terminal worker that writes PyArrow tables to MinIO as Parquet files."""

    def __init__(
        self, config: MinIOConfig, metrics: Optional["MetricsCollector"] = None
    ) -> None:
        """Initialize the file writer.

        Args:
            config: MinIO configuration for S3-compatible object storage
            metrics: Optional MetricsCollector for progress tracking.
        """
        # FileWriter is a terminal worker - it doesn't output results
        super().__init__(num_threads=1)
        self.config = config
        self._metrics = metrics
        self.minio_client = Minio(
            endpoint=self.config.endpoint,
            access_key=self.config.access_key,
            secret_key=self.config.secret_key,
            secure=self.config.secure,
            region="us-east-1",
        )

    def process(self, job: Job) -> None:
        """Write the job's PyArrow tables to MinIO as a Parquet file.

        Args:
            job: The job containing tables to write

        Returns:
            None (terminal worker doesn't output results)

        Raises:
            RuntimeError: If job is not marked as completed
        """
        if not job.file_write_job.completed:
            return

        if job.file_write_job.skipped_items:
            logger.warning(
                f"Incomplete items found for file {job.file_write_job.object_key}. Skipping write."
            )
            if self._metrics:
                self._metrics.record_file_skipped()
            return

        if job.file_write_job.tables:
            table = pa.concat_tables(job.file_write_job.tables)

            buffer = BytesIO()
            pq.write_table(table, buffer)
            buffer.seek(0)

            size = len(buffer.getvalue())
            self.minio_client.put_object(
                bucket_name=self.config.raw_bucket,
                object_name=job.file_write_job.object_key,
                data=buffer,
                length=size,
                content_type="application/octet-stream",
            )

            logger.debug(
                f"File writer successfully uploaded object to MinIO: {job.file_write_job.object_key}"
            )

            if self._metrics:
                self._metrics.record_file_written()

    def file_exists(self, object_key: str) -> bool:
        """Check if an object exists in MinIO.

        Args:
            object_key: The object key to check

        Returns:
            True if the object exists, False otherwise
        """
        try:
            self.minio_client.stat_object(self.config.raw_bucket, object_key)
            return True
        except S3Error:
            return False
