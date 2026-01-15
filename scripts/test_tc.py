"""
Drop-in memory tracking runner for your ThetaClient loop.

What it prints (every CHECKPOINT_EVERY symbols):
- RSS / USS (process memory from OS)
- Python tracemalloc delta since last checkpoint (top allocating lines)
- Optional GC stats

Install deps:
  pip install psutil

Notes:
- tracemalloc tracks Python allocations (not native like NumPy/SSL buffers).
- USS is best on Linux; if unavailable it will show n/a.
"""

from __future__ import annotations

import os
import time
import gc
import signal
from contextlib import suppress

import psutil
import tracemalloc

from theta_client.client import ThetaClient, MinIOConfig
from theta_client.requests import OptionRequest, DataType, Endpoint, FileGranularity
from theta_client.utils import get_symbol_universe


# -----------------------------
# Config knobs
# -----------------------------
CHECKPOINT_EVERY = 1          # print a report every N symbols
TRACEMALLOC_FRAMES = 25        # traceback depth for tracemalloc
TOP_ALLOCS = 12                # top lines to show per checkpoint
DO_GC_AT_CHECKPOINT = True    # set True to test whether growth is Python-retained
SLEEP_BETWEEN_SYMBOLS = 0.0    # diagnostic throttle (0.0 = off)


# -----------------------------
# Memory reporting utilities
# -----------------------------
PROC = psutil.Process(os.getpid())
START_TIME = time.time()


def _fmt_mb(nbytes: int | None) -> str:
    if nbytes is None:
        return "   n/a"
    return f"{nbytes / (1024 * 1024):8.1f} MB"


def proc_mem_line(tag: str) -> str:
    """
    Returns a one-line process memory report.
    """
    mi = PROC.memory_full_info()
    rss = getattr(mi, "rss", None)
    uss = getattr(mi, "uss", None)  # Linux only (usually)
    vms = getattr(mi, "vms", None)

    elapsed = time.time() - START_TIME
    return (
        f"[{tag}] t={elapsed:7.1f}s  "
        f"RSS={_fmt_mb(rss)}  "
        f"USS={_fmt_mb(uss)}  "
        f"VMS={_fmt_mb(vms)}"
    )


def tracemalloc_top_delta(prev_snap, cur_snap, limit: int = 10) -> str:
    """
    Compare snapshots and show the biggest deltas by line.
    """
    stats = cur_snap.compare_to(prev_snap, "lineno")
    lines = []
    for s in stats[:limit]:
        # Example line: "path/to/file.py:123: size=... KiB (+... KiB), count=... (+...), average=..."
        lines.append(str(s))
    return "\n".join(lines) if lines else "(no tracemalloc deltas)"


def gc_line() -> str:
    """
    GC debug stats: (gen0, gen1, gen2) collection counts.
    """
    # get_count() returns number of objects tracked since last collection for each generation
    c0, c1, c2 = gc.get_count()
    return f"GC tracked since last collect: gen0={c0} gen1={c1} gen2={c2}"


# Graceful Ctrl+C so you get a final snapshot
_STOP = False


def _handle_sigint(signum, frame):
    global _STOP
    _STOP = True


signal.signal(signal.SIGINT, _handle_sigint)


def main():
    # Start tracemalloc early so we capture allocations during setup too.
    tracemalloc.start(TRACEMALLOC_FRAMES)
    prev_snap = tracemalloc.take_snapshot()

    print(proc_mem_line("start"))
    print(gc_line())
    print("-" * 100)

    univ = get_symbol_universe()
    total = len(univ) if hasattr(univ, "__len__") else None

    # Create client with log level configuration
    client = ThetaClient(
        num_threads=4,
        storage_config=MinIOConfig(),
        log_level="WARNING",  # "DEBUG" for verbose output, "WARNING" for quiet
    )

    # Optional: if universe is a generator, we still want an index
    for i, symbol in enumerate(univ, 1):
        if _STOP:
            print("\nSIGINT received; stopping after current iteration.")
            break

        req = OptionRequest(
            symbol=symbol,
            start_date=20160101,
            end_date=20251231,
            data_type=DataType.HISTORY,
            endpoint=Endpoint.GREEKS_EOD,
            file_granularity=FileGranularity.MONTHLY,
            force_refresh=False,
        )

        client.request_data(req)

        if SLEEP_BETWEEN_SYMBOLS > 0:
            time.sleep(SLEEP_BETWEEN_SYMBOLS)

        if i % CHECKPOINT_EVERY == 0:
            if DO_GC_AT_CHECKPOINT:
                gc.collect()

            cur_snap = tracemalloc.take_snapshot()

            # Memory + tracemalloc delta since last checkpoint
            progress = f"{i}" + (f"/{total}" if total is not None else "")
            print(proc_mem_line(f"checkpoint {progress}"))
            print(gc_line())
            print("Top Python allocation deltas since last checkpoint (by line):")
            print(tracemalloc_top_delta(prev_snap, cur_snap, limit=TOP_ALLOCS))
            print("-" * 100)

            prev_snap = cur_snap

    # Final report
    with suppress(Exception):
        if DO_GC_AT_CHECKPOINT:
            gc.collect()
    final_snap = tracemalloc.take_snapshot()
    print(proc_mem_line("final"))
    print(gc_line())
    print("Top Python allocation deltas since last checkpoint (by line):")
    print(tracemalloc_top_delta(prev_snap, final_snap, limit=TOP_ALLOCS))
    print("-" * 100)

    # Optional: show tracemalloc peak/current
    cur, peak = tracemalloc.get_traced_memory()
    print(f"tracemalloc traced current={_fmt_mb(cur)} peak={_fmt_mb(peak)}")


if __name__ == "__main__":
    main()
