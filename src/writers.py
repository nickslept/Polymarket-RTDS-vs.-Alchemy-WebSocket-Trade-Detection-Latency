"""
src/writers.py
==============
Async writer coroutines for the trades and orphans Parquet files.

Each writer:
  - Drains its queue into an in-memory buffer
  - Flushes to Parquet when FLUSH_ROWS is reached OR FLUSH_INTERVAL_S seconds have elapsed, whichever comes first
  - Never blocks the event loop (all disk I/O is isolated here)
  - On any exit (KeyboardInterrupt / CancelledError), the finally block drains the queue and flushes remaining rows before closing the file
"""
import asyncio
import time

import pyarrow.parquet as pq

import config
import src.state as state
from src.schemas import (
    TRADES_SCHEMA,
    ORPHANS_SCHEMA,
    write_trades_batch,
    write_orphans_batch,
)


async def trades_writer(path: str) -> None:
    writer     = pq.ParquetWriter(path, TRADES_SCHEMA)
    buffer:    list = []
    last_flush = time.monotonic()
    total_rows = 0

    try:
        while True:
            try:
                timeout = max(config.FLUSH_INTERVAL_S - (time.monotonic() - last_flush), 0.01)
                row     = await asyncio.wait_for(state.trades_queue.get(), timeout=timeout)
                buffer.append(row)
            except asyncio.TimeoutError: # no new rows arrived within the flush interval
                pass

            if buffer and (
                len(buffer) >= config.FLUSH_ROWS
                or time.monotonic() - last_flush >= config.FLUSH_INTERVAL_S
            ):
                write_trades_batch(writer, buffer)
                total_rows += len(buffer)
                print(f"[trades] Flushed {len(buffer)} rows — {total_rows} total matched trades")
                buffer.clear()
                last_flush = time.monotonic()
    finally:
        while not state.trades_queue.empty():
            try:
                buffer.append(state.trades_queue.get_nowait())
            except Exception:
                break
        try:
            if buffer:
                write_trades_batch(writer, buffer)
            writer.close()
        except Exception as e:
            print(f"[shutdown] Error flushing trades: {e}")


async def orphans_writer(path: str) -> None:
    writer     = pq.ParquetWriter(path, ORPHANS_SCHEMA)
    buffer:    list = []
    last_flush = time.monotonic()
    total_rows = 0

    try:
        while True:
            try:
                timeout = max(config.FLUSH_INTERVAL_S - (time.monotonic() - last_flush), 0.01)
                row     = await asyncio.wait_for(state.orphans_queue.get(), timeout=timeout)
                buffer.append(row)
            except asyncio.TimeoutError:
                pass

            if buffer and (
                len(buffer) >= config.FLUSH_ROWS
                or time.monotonic() - last_flush >= config.FLUSH_INTERVAL_S
            ):
                write_orphans_batch(writer, buffer)
                total_rows += len(buffer)
                print(f"[orphans] Flushed {len(buffer)} rows — {total_rows} total orphans")
                buffer.clear()
                last_flush = time.monotonic()
    finally:
        while not state.orphans_queue.empty():
            try:
                buffer.append(state.orphans_queue.get_nowait())
            except Exception:
                break
        try:
            if buffer:
                write_orphans_batch(writer, buffer)
            writer.close()
        except Exception as e:
            print(f"[shutdown] Error flushing orphans: {e}")