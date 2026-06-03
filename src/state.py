"""
src/state.py
============
All shared states for the tool. 

Other modules read and write these variables via module attribute assignment. Example:
    import src.state as state
    state.data_valid = True          # write
    if not state.data_valid: ...     # read

trades_queue, orphans_queue, and run_start_ns are set in main() before any
coroutines start. They are never read before that point.
"""
import asyncio

run_start_ns:  int                = 0       # timestamp when the main() function starts running
hashmap:       dict               = {}      # tx_hash → {"poly_rel_ns": int|None, "alchemy_rel_ns": int|None}
data_valid:    bool               = False   # True only when both connections (to Alchemy & Polymarket) are confirmed live
trades_queue:  asyncio.Queue | None = None
orphans_queue: asyncio.Queue | None = None