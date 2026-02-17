"""Terminal worker that collects completed jobs as Polars DataFrames."""

import logging
from dataclasses import dataclass
from queue import Queue
from typing import Optional, TYPE_CHECKING

import polars as pl
import pyarrow as pa

from theta_client.job import Job
from theta_client.queue_worker import QueueWorker

if TYPE_CHECKING:
    from theta_client.metrics import MetricsCollector

logger = logging.getLogger(__name__)

_SENTINEL = object()


@dataclass(slots=True)
class DataFrameResult:
    """Result yielded by stream_dataframes() for each logical file."""

    key: str
    df: Optional[pl.DataFrame]


class DataFrameCollector(QueueWorker):
    """Terminal worker that converts completed jobs into Polars DataFrames.

    Puts DataFrameResult objects into an external result_queue that the
    stream_dataframes() generator reads from.
    """

    def __init__(
        self,
        result_queue: Queue,
        metrics: Optional["MetricsCollector"] = None,
    ) -> None:
        super().__init__(num_threads=1)
        self._result_queue = result_queue
        self._metrics = metrics

    def process(self, job: Job) -> None:
        if not job.file_write_job.completed:
            return

        key = job.file_write_job.object_key

        if job.file_write_job.skipped_items:
            logger.warning(
                f"Incomplete items for {key}. Yielding None DataFrame."
            )
            if self._metrics:
                self._metrics.record_file_skipped()
            self._result_queue.put(DataFrameResult(key=key, df=None))
            return

        if job.file_write_job.tables:
            table = pa.concat_tables(job.file_write_job.tables)
            df: pl.DataFrame = pl.from_arrow(table)  # type: ignore[assignment]
            self._result_queue.put(DataFrameResult(key=key, df=df))
            logger.debug(
                f"Collected DataFrame for {key}: {len(df)} rows"
            )

            if self._metrics:
                self._metrics.record_file_written()
