import threading
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List
from io import BytesIO
from enum import Enum

import pyarrow as pa


class Schema(Enum):
    OPTION_EOD = "option_eod"
    OPTION_QUOTE = "option_quote"
    OPTION_TRADE = "option_trade"
    OPTION_TRADE_QUOTE = "option_trade_quote"
    GREEK_FIRST_ORDER = "greek_first_order"
    GREEK_EOD = "greek_eod"
    STOCK_EOD = "stock_eod"
    STOCK_QUOTE = "stock_quote"
    STOCK_TRADE = "stock_trade"


@dataclass(slots=True)
class FileWriteJob:
    """
    The FileWriteJob represents a request for a new file coming from the client, as the name implies.
    It contains a object_key to ultimate use when writing and a BytesIO wrapped parquet file.
    """

    object_key: str
    total_items: int
    skipped_items: bool = False
    completed_items: int = 0
    completed: bool = False
    tables: List[pa.table] = field(default_factory=list)
    byte_wrapper: Optional[BytesIO] = None
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def _increment(self) -> None:
        self.completed_items += 1
        if self.completed_items == self.total_items:
            self.completed = True

    def add_table(self, table: pa.table) -> None:
        with self._lock:
            self.tables.append(table)
            self._increment()

    def mark_item_skipped(self) -> None:
        """Mark an item as skipped without adding a table."""
        with self._lock:
            self.skipped_items = True
            self._increment()


@dataclass(slots=True)
class Job:
    url: str
    schema: Schema
    csv_buffer: Optional[BytesIO]
    file_write_job: FileWriteJob


class PipelineCounters:
    __slots__ = ("http_completed", "files_completed", "_lock", "_notify")

    def __init__(self):
        self.http_completed = 0
        self.files_completed = 0
        self._lock = threading.Lock()
        self._notify = threading.Event()

    def inc_http(self):
        with self._lock:
            self.http_completed += 1

    def inc_files(self):
        with self._lock:
            self.files_completed += 1
        self._notify.set()
