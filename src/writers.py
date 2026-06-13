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
                timeout = max(config.FLUSH_INTERVAL_SECONDS - (time.monotonic() - last_flush), 0.01) 
                row     = await asyncio.wait_for(state.trades_queue.get(), timeout=timeout)
                buffer.append(row)
            except asyncio.TimeoutError: # no new rows arrived within the flush interval
                pass

            if buffer and (
                len(buffer) >= config.FLUSH_ROW_THRESHOLD
                or time.monotonic() - last_flush >= config.FLUSH_INTERVAL_SECONDS
            ):
                write_trades_batch(writer, buffer)
                total_rows += len(buffer)
                print(f"[writers] Flushed {len(buffer)} rows. {total_rows} total matched trades. Queue backlog: {state.trades_queue.qsize()} rows")
                buffer.clear()
                last_flush = time.monotonic()
    finally:
        while not state.trades_queue.empty():
            try:
                buffer.append(state.trades_queue.get_nowait())
            except Exception:
                break
        print(f"[writers] Shutting down trades writer... Draining {len(buffer)} rows before shutdown.")
        try:
            if buffer:
                write_trades_batch(writer, buffer)
                total_rows += len(buffer)
            writer.close()
            print(f"[writers] Trades writer closed. Total rows written: {total_rows}.")
        except Exception as e:
            print(f"[writers] Error flushing trades: {e}")


async def orphans_writer(path: str) -> None:
    writer     = pq.ParquetWriter(path, ORPHANS_SCHEMA)
    buffer:    list = []
    last_flush = time.monotonic()
    total_rows = 0

    try:
        while True:
            try:
                timeout = max(config.FLUSH_INTERVAL_SECONDS - (time.monotonic() - last_flush), 0.01)
                row     = await asyncio.wait_for(state.orphans_queue.get(), timeout=timeout)
                buffer.append(row)
            except asyncio.TimeoutError:
                pass

            if buffer and (
                len(buffer) >= config.FLUSH_ROW_THRESHOLD
                or time.monotonic() - last_flush >= config.FLUSH_INTERVAL_SECONDS
            ):
                write_orphans_batch(writer, buffer)
                total_rows += len(buffer)
                print(f"[writers] Flushed {len(buffer)} rows. {total_rows} total orphans. Queue backlog: {state.orphans_queue.qsize()} rows")
                buffer.clear()
                last_flush = time.monotonic()
    finally:
        while not state.orphans_queue.empty():
            try:
                buffer.append(state.orphans_queue.get_nowait())
            except Exception:
                break
        print(f"[writers] Shutting down orphans writer... Draining {len(buffer)} rows before shutdown.")
        try:
            if buffer:
                write_orphans_batch(writer, buffer)
                total_rows += len(buffer)
            writer.close()
            print(f"[writers] Orphans writer closed. Total rows written: {total_rows}.")
        except Exception as e:
            print(f"[writers] Error flushing orphans: {e}")