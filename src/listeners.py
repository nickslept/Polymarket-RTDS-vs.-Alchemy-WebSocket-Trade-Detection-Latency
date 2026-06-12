import asyncio
import json
import time

import websockets

import config
import src.state as state
from src.hashmap import handle_poly_event, handle_alchemy_event


# --- Polymarket WebSocket connection parameters ---
_POLY_WS_URL = "wss://ws-live-data.polymarket.com"
_POLY_SUB    = json.dumps({
    "action": "subscribe",
    "subscriptions": [
        {"topic": "activity", "type": "trades"}
    ]
})
_POLY_PING_INTERVAL_SECONDS = 5


# --- Ping loop for Polymarket WebSocket connection ---
async def _poly_ping_loop(ws) -> None:
    """Sends a ping every [_POLY_PING_INTERVAL_SECONDS] seconds to keep the Polymarket WebSocket connection alive."""
    while True:
        await asyncio.sleep(_POLY_PING_INTERVAL_SECONDS)
        await ws.send("ping")

# --- Polymarket WebSocket listener ---
async def polymarket_listener(ready_event: asyncio.Event) -> None:
    async with websockets.connect(_POLY_WS_URL, close_timeout=1) as ws:
        await ws.send(_POLY_SUB)
        print(f"[connection] Polymarket subscription sent.")
        ready_event.set()  # polymarket doesn't send an ack upon subscription, so we proceed with the assumption that the subscription was successful

        ping_task = asyncio.create_task(_poly_ping_loop(ws))

        try:
            async for raw in ws:
                arrival_perf_ns = time.perf_counter_ns()  

                if not state.data_valid:
                    continue #skips the newest trade if the alchemy connection dropped so you don't populate the hashmap with trades that will never be matched

                
                if not raw.startswith("{"):
                    continue # server sends back pongs — anything that isn't JSON is skipped

                try:
                    msg     = json.loads(raw)
                    payload = msg.get("payload", {})
                    tx_hash = payload.get("transactionHash")
                    if tx_hash:
                        handle_poly_event(tx_hash, arrival_perf_ns - state.run_start_ns) #if the transaction hash evaluates to None (bc it isn't present in the payload), then the data is skipped/not sent to handle_poly_event()
                except Exception:
                    pass
        finally:
            ping_task.cancel() #stops the ping loop
            try:
                await ping_task
            except asyncio.CancelledError:
                pass #intended behavior


# --- Alchemy WebSocket listener ---
async def alchemy_listener(
    ready_event:        asyncio.Event,
    ws_url:             str,
    order_filled_topic: str,
) -> None:
    async with websockets.connect(ws_url, close_timeout=1) as ws:
        sub_payload = json.dumps({
            "jsonrpc": "2.0",
            "id":      1,
            "method":  "eth_subscribe",
            "params": [
                "logs",
                {
                    "address": [
                        config.CTF_EXCHANGE_V2,
                        config.NEG_RISK_CTF_EXCHANGE_V2,
                    ],
                    "topics": [order_filled_topic],
                },
            ],
        })
        await ws.send(sub_payload)

        # Alchemy responds with {"jsonrpc":"2.0","id":1,"result":"0x..."}
        ack_raw = await asyncio.wait_for(ws.recv(), timeout=config.SUB_ACK_TIMEOUT_S)
        ack     = json.loads(ack_raw)
        if "result" not in ack:
            raise RuntimeError(f"Unexpected Alchemy ack: {ack_raw}")
        print(f"[connection] Alchemy subscription ack received.")
        ready_event.set()

        async for raw in ws:
            arrival_perf_ns = time.perf_counter_ns()  # ← first line; no earlier work

            if not state.data_valid:
                continue

            try:
                msg     = json.loads(raw)
                result  = msg.get("params", {}).get("result", {})
                tx_hash = result.get("transactionHash")
                if tx_hash:
                    handle_alchemy_event(tx_hash, arrival_perf_ns - state.run_start_ns)
            except Exception:
                pass