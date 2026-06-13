import asyncio
import time

import config
import src.state as state


# --- Internal helper ---

def _enqueue_trade(tx_hash: str, poly_rel_ns: int, alchemy_rel_ns: int) -> None:
    state.trades_queue.put_nowait({
        "tx_hash":        tx_hash,
        "poly_rel_ns":    poly_rel_ns,
        "alchemy_rel_ns": alchemy_rel_ns,
        "delta_ns":       poly_rel_ns - alchemy_rel_ns,
    })


# --- Event handlers ---
"""
Both event handlers are called directly from the listener coroutines
Each handler: 
    - Checks if the tx_hash is already in the hashmap. If not, it creates a new entry.
    - Handles the case where the same pipeline (alchemy or polymarket rtds) fires more than once for the same tx_hash... all arrivals after the first are silently discarded to keep the most accurate timestamp.
    - Updates the value of the tx_hash with the new relative timestamp for the pipeline (alchemy or polymarket rtds).
    - Checks whether the other pipeline has already arrived and if so, enqueues the matched trade + deletes the entry from the hashmap.
"""
def handle_poly_event(tx_hash: str, rel_ns: int) -> None:
    """
    Handles adding Polymarket rtds events (keyed by transaction hash, value is the specific pipeline's relative timestamp) to the hashmap.
    Enqueues matched trades when both pipelines have arrived and removes the entry from the hashmap.
    """
    entry = state.hashmap.get(tx_hash)
    if entry is None:
        state.hashmap[tx_hash] = {"poly_rel_ns": rel_ns, "alchemy_rel_ns": None}
        return
    if entry["poly_rel_ns"] is not None:
        return  # first arrival wins (discard duplicates)
    entry["poly_rel_ns"] = rel_ns
    if entry["alchemy_rel_ns"] is not None:
        _enqueue_trade(tx_hash, entry["poly_rel_ns"], entry["alchemy_rel_ns"])
        del state.hashmap[tx_hash]


def handle_alchemy_event(tx_hash: str, rel_ns: int) -> None:
    """
    Handles adding Alchemy events (keyed by transaction hash, value is the specific pipeline's relative timestamp) to the hashmap.
    Enqueues matched trades when both pipelines have arrived and removes the entry from the hashmap.
    """
    entry = state.hashmap.get(tx_hash)
    if entry is None:
        state.hashmap[tx_hash] = {"poly_rel_ns": None, "alchemy_rel_ns": rel_ns}
        return
    if entry["alchemy_rel_ns"] is not None:
        return  # first arrival wins (discard duplicates)
    entry["alchemy_rel_ns"] = rel_ns
    if entry["poly_rel_ns"] is not None:
        _enqueue_trade(tx_hash, entry["poly_rel_ns"], entry["alchemy_rel_ns"])
        del state.hashmap[tx_hash]


# --- Hashmap cleanup upon disconnect (called by the connection manager) ---

def clear_hashmap_on_disconnect() -> None:
    """
    Called on when the connection to either pipline is lost.
    Iterates through the hashmap and adds each entry (unmatched trade) to the orphans queue with a reason of "cleared_on_disconnect" to be added to the orphans parquet.
    """
    for tx_hash, entry in list(state.hashmap.items()):
        arrived_side = "polymarket" if entry["poly_rel_ns"] is not None else "alchemy"
        rel_ns       = entry["poly_rel_ns"] if entry["poly_rel_ns"] is not None else entry["alchemy_rel_ns"]
        state.orphans_queue.put_nowait({
            "tx_hash":      tx_hash,
            "arrived_side": arrived_side,
            "rel_ns":       rel_ns,
            "reason":       "cleared_on_disconnect",
        })
    state.hashmap.clear()


# --- Unmatched trade eviction ---

async def evict_unmatched_trades() -> None:
    """
    Runs every EVICTION_INTERVAL_SECONDS seconds.
    Any entry whose first-arrived timestamp is older than MATCH_TIMEOUT_SECONDS seconds is evicted and added to the orphans queue with a reason of "unmatched" to be added to the orphans parquet.
    """
    while True:
        await asyncio.sleep(config.EVICTION_INTERVAL_SECONDS)

        now_rel_ns = time.perf_counter_ns() - state.run_start_ns #elapsed time since run started
        cutoff_ns  = config.MATCH_TIMEOUT_SECONDS * 1_000_000_000 #convert MATCH_TIMEOUT_SECONDS to nanoseconds

        to_evict = [
            tx_hash for tx_hash, entry in state.hashmap.items()
            if now_rel_ns - (entry["poly_rel_ns"] or entry["alchemy_rel_ns"] or 0) > cutoff_ns #if the current time - the timestamp of the first trade arrival (for either pipeline) is greater than the cutoff time, the entry is added to the list of entries to be evicted
        ]

        for tx_hash in to_evict:
            entry        = state.hashmap.pop(tx_hash)
            arrived_side = "polymarket" if entry["poly_rel_ns"] is not None else "alchemy"
            rel_ns       = entry["poly_rel_ns"] if entry["poly_rel_ns"] is not None else entry["alchemy_rel_ns"]
            state.orphans_queue.put_nowait({
                "tx_hash":      tx_hash,
                "arrived_side": arrived_side,
                "rel_ns":       rel_ns,
                "reason":       "unmatched",
            })