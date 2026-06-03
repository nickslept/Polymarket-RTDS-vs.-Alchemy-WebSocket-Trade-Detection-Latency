"""
src/hashmap.py
==============
All hashmap read/write operations and the TTL sweep coroutine.

The hashmap is keyed by transactionHash.  Each entry holds the relative
arrival timestamp (ns since run start) for each pipeline, or None if that
side has not yet arrived.  When both slots are filled the entry is removed
and the completed trade is enqueued for the writer.

First-arrival-wins: if the same pipeline fires more than once for the same
tx hash, all arrivals after the first are silently discarded.
"""
import asyncio
import time

import config
import src.state as state


# ---------------------------------------------------------------------------
# Internal helper
# ---------------------------------------------------------------------------

def _enqueue_trade(tx_hash: str, poly_rel_ns: int, alchemy_rel_ns: int) -> None:
    state.trades_queue.put_nowait({
        "tx_hash":        tx_hash,
        "poly_rel_ns":    poly_rel_ns,
        "alchemy_rel_ns": alchemy_rel_ns,
        "delta_ns":       poly_rel_ns - alchemy_rel_ns,
    })


# ---------------------------------------------------------------------------
# Event handlers — called directly from the listener coroutines
# ---------------------------------------------------------------------------

def handle_poly_event(tx_hash: str, rel_ns: int) -> None:
    entry = state.hashmap.get(tx_hash)
    if entry is None:
        state.hashmap[tx_hash] = {"poly_rel_ns": rel_ns, "alchemy_rel_ns": None}
        return
    if entry["poly_rel_ns"] is not None:
        return  # first arrival wins — discard duplicate
    entry["poly_rel_ns"] = rel_ns
    if entry["alchemy_rel_ns"] is not None:
        _enqueue_trade(tx_hash, entry["poly_rel_ns"], entry["alchemy_rel_ns"])
        del state.hashmap[tx_hash]


def handle_alchemy_event(tx_hash: str, rel_ns: int) -> None:
    entry = state.hashmap.get(tx_hash)
    if entry is None:
        state.hashmap[tx_hash] = {"poly_rel_ns": None, "alchemy_rel_ns": rel_ns}
        return
    if entry["alchemy_rel_ns"] is not None:
        return  # first arrival wins — discard duplicate
    entry["alchemy_rel_ns"] = rel_ns
    if entry["poly_rel_ns"] is not None:
        _enqueue_trade(tx_hash, entry["poly_rel_ns"], entry["alchemy_rel_ns"])
        del state.hashmap[tx_hash]


# ---------------------------------------------------------------------------
# Disconnect cleanup — called by the connection manager on any drop
# ---------------------------------------------------------------------------

def clear_hashmap_on_disconnect() -> None:
    """
    Evict every in-flight entry and log each as 'cleared_on_disconnect'.
    Keeps the orphan log clean — only genuine pipeline drops appear as
    'unmatched'; entries lost to a disconnect are labelled separately.
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


# ---------------------------------------------------------------------------
# TTL sweep — long-running background coroutine
# ---------------------------------------------------------------------------

async def ttl_sweep() -> None:
    """
    Runs every TTL_CHECK_INTERVAL_S seconds.  Any entry whose first-arrived
    timestamp is older than TTL_SECONDS is evicted and logged as 'unmatched'.
    """
    while True:
        await asyncio.sleep(config.TTL_CHECK_INTERVAL_S)

        now_rel_ns = time.perf_counter_ns() - state.run_start_ns
        cutoff_ns  = config.TTL_SECONDS * 1_000_000_000

        to_evict = [
            tx_hash for tx_hash, entry in state.hashmap.items()
            if (entry["poly_rel_ns"] or entry["alchemy_rel_ns"] or 0) + cutoff_ns < now_rel_ns
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