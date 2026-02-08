"""Rich-based live progress display for ThetaClient."""

import threading
import time
from typing import TYPE_CHECKING

from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.progress import Progress, BarColumn, TextColumn, TaskProgressColumn
from rich.table import Table

if TYPE_CHECKING:
    from theta_client.metrics import MetricsCollector


class ProgressDisplay:
    """Live updating progress display using Rich."""

    def __init__(self, metrics: "MetricsCollector", refresh_rate: float = 4.0) -> None:
        """Initialize the progress display.

        Args:
            metrics: MetricsCollector instance to read metrics from.
            refresh_rate: Display refresh rate in Hz (default: 4Hz).
        """
        self._metrics = metrics
        self._refresh_interval = 1.0 / refresh_rate
        self._running = False
        self._thread: threading.Thread | None = None
        self._console = Console()
        self._live: Live | None = None

    def _format_number(self, num: int | float) -> str:
        """Format a number with thousands separators."""
        if isinstance(num, float):
            return f"{num:,.1f}"
        return f"{num:,}"

    def _format_duration(self, seconds: float) -> str:
        """Format duration in human-readable format."""
        if seconds < 60:
            return f"{seconds:.1f}s"
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m {secs:.1f}s"

    def _create_display(self) -> Panel:
        """Create the Rich display panel."""
        snapshot = self._metrics.get_snapshot()

        # Create main info table
        info_table = Table.grid(padding=(0, 2))
        info_table.add_column(style="cyan", justify="right")
        info_table.add_column(style="white")

        info_table.add_row("Request Type", snapshot.request_type or "-")
        info_table.add_row("Symbol", snapshot.symbol or "-")
        info_table.add_row(
            "Date Range",
            f"{snapshot.start_date} - {snapshot.end_date}"
            if snapshot.start_date
            else "-",
        )
        info_table.add_row("Endpoint", snapshot.endpoint or "-")
        info_table.add_row("", "")  # Spacer

        # Progress metrics
        http_pct = snapshot.http_progress_pct
        info_table.add_row(
            "HTTP Progress",
            f"{snapshot.http_completed}/{snapshot.total_http_requests} ({http_pct:.1f}%)",
        )
        info_table.add_row(
            "Files Written",
            f"{snapshot.files_written}/{snapshot.total_files}",
        )
        info_table.add_row(
            "Rows Processed",
            self._format_number(snapshot.rows_processed),
        )
        info_table.add_row("", "")  # Spacer

        # Performance metrics
        info_table.add_row(
            "Elapsed Time",
            self._format_duration(snapshot.elapsed_time),
        )
        info_table.add_row(
            "Avg Response Time",
            f"{snapshot.avg_response_time_ms:.1f}ms"
            if snapshot.avg_response_time_ms > 0
            else "-",
        )
        info_table.add_row(
            "Rows/Second",
            self._format_number(int(snapshot.rows_per_second))
            if snapshot.rows_per_second > 0
            else "-",
        )
        info_table.add_row("", "")  # Spacer

        # Missing data
        info_table.add_row(
            "Missing Data Count", self._format_number(snapshot.missing_data_count)
        )

        info_table.add_row("Files Skipped", self._format_number(snapshot.files_skipped))

        # Create progress bar
        progress = Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(bar_width=40),
            TaskProgressColumn(),
            expand=False,
        )
        task = progress.add_task(
            "Progress",
            total=snapshot.total_http_requests,
            completed=snapshot.http_completed,
        )

        # Combine into a table
        layout = Table.grid(expand=True)
        layout.add_row(info_table)
        layout.add_row("")
        layout.add_row(progress)

        title = (
            f"ThetaClient Progress - {snapshot.symbol}"
            if snapshot.symbol
            else "ThetaClient Progress"
        )
        return Panel(
            layout,
            title=title,
            border_style="blue",
            padding=(1, 2),
        )

    def _display_loop(self) -> None:
        """Background thread loop for updating the display."""
        with Live(
            self._create_display(),
            console=self._console,
            refresh_per_second=1.0 / self._refresh_interval,
            transient=True,
        ) as live:
            self._live = live
            while self._running:
                live.update(self._create_display())
                time.sleep(self._refresh_interval)

    def start(self) -> None:
        """Start the live progress display."""
        if self._running:
            return

        self._running = True
        self._thread = threading.Thread(target=self._display_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop the live progress display and show final summary."""
        if not self._running:
            return

        self._running = False
        if self._thread:
            self._thread.join(timeout=1.0)
            self._thread = None

        # Print final summary
        self._print_summary()

    def _print_summary(self) -> None:
        """Print final summary after request completes."""
        snapshot = self._metrics.get_snapshot()

        summary_table = Table(title="Request Complete", show_header=False, box=None)
        summary_table.add_column(style="cyan", justify="right")
        summary_table.add_column(style="green")

        summary_table.add_row("Symbol", snapshot.symbol)
        summary_table.add_row("Duration", self._format_duration(snapshot.elapsed_time))
        summary_table.add_row(
            "HTTP Requests", f"{snapshot.http_completed}/{snapshot.total_http_requests}"
        )
        summary_table.add_row(
            "Files Written", f"{snapshot.files_written}/{snapshot.total_files}"
        )
        summary_table.add_row(
            "Rows Processed", self._format_number(snapshot.rows_processed)
        )
        summary_table.add_row(
            "Avg Response Time", f"{snapshot.avg_response_time_ms:.1f}ms"
        )
        summary_table.add_row(
            "Rows/Second", self._format_number(int(snapshot.rows_per_second))
        )
        if snapshot.missing_data_count > 0:
            summary_table.add_row("Missing Data", str(snapshot.missing_data_count))
        if snapshot.files_skipped > 0:
            summary_table.add_row("Files Skipped", str(snapshot.files_skipped))

        self._console.print()
        self._console.print(summary_table)
        self._console.print()
