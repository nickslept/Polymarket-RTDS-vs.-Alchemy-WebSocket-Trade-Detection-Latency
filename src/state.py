"""
src/state.py
============
All shared mutable state for the latency tool.

asyncio is single-threaded — only one coroutine runs at a time, so no locks
are needed anywhere.  Other modules read and write these variables via module
attribute assignment:

    import src.state as state
    state.data_valid = True          # write
    if not state.data_valid: ...     # read

Python caches module imports, so every module that does `import src.state`
receives the same object.  Assignments to its attributes are immediately
visible everywhere.

trades_queue, orphans_queue, and run_start_ns are set in main() before any
coroutines start — they are never read before that point.
"""
import asyncio

run_start_ns:  int                = 0
hashmap:       dict               = {}      # tx_hash → {"poly_rel_ns": int|None, "alchemy_rel_ns": int|None}
data_valid:    bool               = False   # True only when both subscriptions are confirmed live
trades_queue:  asyncio.Queue | None = None
orphans_queue: asyncio.Queue | None = None