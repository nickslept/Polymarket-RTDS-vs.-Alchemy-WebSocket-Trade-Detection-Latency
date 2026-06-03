"""
src/connections.py
==================
Connection manager: opens both WebSocket connections simultaneously, waits
for both subscription acknowledgments, then monitors both connections and
handles reconnection with exponential backoff.

On any disconnect or ack timeout:
  1. data_valid is set to False — in-flight events are discarded
  2. Both tasks are cancelled
  3. The hashmap is cleared; every entry is logged as cleared_on_disconnect
  4. Exponential backoff before the next attempt
  5. Both connections are re-opened simultaneously
  6. data_valid is set to True only once both subscription acks are received

Backoff resets to RECONNECT_BASE_S on every successful dual-connect.
"""
import asyncio

import config
import src.state as state
from src.hashmap import clear_hashmap_on_disconnect
from src.listeners import polymarket_listener, alchemy_listener


async def run_connections(alchemy_url: str, order_filled_topic: str) -> None:
    backoff = config.RECONNECT_BASE_S

    while True:
        state.data_valid = False
        poly_ready    = asyncio.Event()
        alchemy_ready = asyncio.Event()

        poly_task    = asyncio.create_task(polymarket_listener(poly_ready))
        alchemy_task = asyncio.create_task(
            alchemy_listener(alchemy_ready, alchemy_url, order_filled_topic)
        )

        try:
            await asyncio.wait_for(
                asyncio.gather(poly_ready.wait(), alchemy_ready.wait()),
                timeout=config.SUB_ACK_TIMEOUT_S,
            )
            state.data_valid = True
            backoff          = config.RECONNECT_BASE_S
            print("[connection] Both subscriptions live — collecting data.")

            done, _ = await asyncio.wait(
                [poly_task, alchemy_task],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in done:
                try:
                    exc = task.exception()
                    if exc:
                        print(f"[connection] Dropped with error: {exc}")
                    else:
                        print("[connection] Connection closed cleanly.")
                except asyncio.CancelledError:
                    pass

        except asyncio.TimeoutError:
            print(
                f"[connection] Subscription ack timed out after "
                f"{config.SUB_ACK_TIMEOUT_S}s — will retry."
            )
        except Exception as e:
            print(f"[connection] Unexpected error: {e}")

        finally:
            state.data_valid = False
            for task in [poly_task, alchemy_task]:
                if not task.done():
                    task.cancel()
                    try:
                        await task
                    except (asyncio.CancelledError, Exception):
                        pass
            clear_hashmap_on_disconnect()

        print(f"[connection] Reconnecting in {backoff}s...")
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, config.RECONNECT_MAX_S)