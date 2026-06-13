import asyncio

run_start_ns:  int                = 0       # time.perf_counter_ns() origin captured at startup; all rel_ns values are offsets from this (not a wall-clock time)
hashmap:       dict               = {}      # tx_hash → {"poly_rel_ns": int|None, "alchemy_rel_ns": int|None}
data_valid:    bool               = False   # True only when both connections (to Alchemy & Polymarket) are confirmed live
trades_queue:  asyncio.Queue | None = None
orphans_queue: asyncio.Queue | None = None