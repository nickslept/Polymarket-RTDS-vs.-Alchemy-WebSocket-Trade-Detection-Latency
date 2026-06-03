"""
src/schemas.py
==============
Pyarrow schemas for the two output Parquet files, and the synchronous batch
write helpers called by the writer coroutines.
"""
import pyarrow as pa
import pyarrow.parquet as pq


TRADES_SCHEMA = pa.schema([
    pa.field("tx_hash",        pa.string()),
    pa.field("poly_rel_ns",    pa.int64()),    # Polymarket arrival ns since run start
    pa.field("alchemy_rel_ns", pa.int64()),    # Alchemy arrival ns since run start
    pa.field("delta_ns",       pa.int64()),    # poly_rel_ns - alchemy_rel_ns (signed)
])

ORPHANS_SCHEMA = pa.schema([
    pa.field("tx_hash",        pa.string()),
    pa.field("arrived_side",   pa.string()),   # "polymarket" or "alchemy"
    pa.field("rel_ns",         pa.int64()),    # arrival ns of the side that did arrive
    pa.field("reason",         pa.string()),   # "unmatched" or "cleared_on_disconnect"
])


def write_trades_batch(writer: pq.ParquetWriter, rows: list) -> None:
    table = pa.table(
        {
            "tx_hash":        [r["tx_hash"]        for r in rows],
            "poly_rel_ns":    [r["poly_rel_ns"]    for r in rows],
            "alchemy_rel_ns": [r["alchemy_rel_ns"] for r in rows],
            "delta_ns":       [r["delta_ns"]       for r in rows],
        },
        schema=TRADES_SCHEMA,
    )
    writer.write_table(table)


def write_orphans_batch(writer: pq.ParquetWriter, rows: list) -> None:
    table = pa.table(
        {
            "tx_hash":      [r["tx_hash"]      for r in rows],
            "arrived_side": [r["arrived_side"] for r in rows],
            "rel_ns":       [r["rel_ns"]       for r in rows],
            "reason":       [r["reason"]       for r in rows],
        },
        schema=ORPHANS_SCHEMA,
    )
    writer.write_table(table)