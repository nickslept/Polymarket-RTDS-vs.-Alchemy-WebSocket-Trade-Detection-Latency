import asyncio

run_start_ns:  int                = 0       # timestamp when the main() function starts running
hashmap:       dict               = {}      # tx_hash → {"poly_rel_ns": int|None, "alchemy_rel_ns": int|None}
data_valid:    bool               = False   # True only when both connections (to Alchemy & Polymarket) are confirmed live
trades_queue:  asyncio.Queue | None = None
orphans_queue: asyncio.Queue | None = None