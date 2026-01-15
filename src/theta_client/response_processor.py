import time
import logging
from typing import Optional

import pyarrow.csv as pv

from theta_client.job import Schema, Job
from theta_client.schemas import SCHEMAS
from theta_client.queue_worker import QueueWorker

logger = logging.getLogger(__name__)


class ResponseProcessor(QueueWorker):
    """Worker that processes HTTP responses into PyArrow tables."""

    def __init__(self) -> None:
        """Initialize the response processor.

        This worker is single-threaded by default since PyArrow CSV parsing
        is already optimized.
        """
        super().__init__(num_threads=1)

    def get_convert_options(self, schema: Schema) -> pv.ConvertOptions:
        """Get PyArrow convert options for this schema.

        Args:
            schema: The schema to use for type conversion

        Returns:
            PyArrow ConvertOptions with appropriate column types
        """
        return pv.ConvertOptions(
            column_types={field.name: field.type for field in SCHEMAS[schema.value]}
        )

    def process(self, job: Job) -> Optional[Job]:
        """Process HTTP result into PyArrow table.

        Args:
            job: The job containing CSV buffer to process

        Returns:
            The job with table added to file_write_job
        """
        # Handle jobs with no data
        if job.csv_buffer is None:
            job.file_write_job.mark_item_skipped()
            return job

        start_time = time.time()

        parse_start = time.time()
        table = pv.read_csv(
            job.csv_buffer, convert_options=self.get_convert_options(job.schema)
        )
        parse_duration_ms = (time.time() - parse_start) * 1000

        duration_ms = (time.time() - start_time) * 1000
        row_count = len(table)

        logger.debug(
            f"{job.schema.value} processing completed: "
            f"{row_count} rows in {duration_ms:.1f}ms "
            f"(parse: {parse_duration_ms:.1f}ms)"
        )
        job.file_write_job.add_table(table)

        return job
