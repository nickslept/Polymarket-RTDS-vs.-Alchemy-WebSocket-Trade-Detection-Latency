"""
src/listeners.py
================
WebSocket listeners for the two data pipelines.

Both listeners follow the same pattern:
  1. Connect
  2. Send subscription payload
  3. Wait for acknowledgment — set ready_event once confirmed
  4. Stream messages indefinitely, recording arrival timestamps and updating
     the hashmap

The arrival timestamp (time.perf_counter_ns()) is always the absolute first
operation after a message is received, before any parsing or branching.
"""
import asyncio
import json
import time

import websockets

import config
import src.state as state
from src.hashmap import handle_poly_event, handle_alchemy_event


_POLY_WS_URL = "wss://ws-live-data.polymarket.com"
_POLY_SUB    = json.dumps({"topic": "activity", "type": "trades"})


async def polymarket_listener(ready_event: asyncio.Event) -> None:
    async with websockets.connect(_POLY_WS_URL) as ws:
        await ws.send(_POLY_SUB)

        # First message back from the server is treated as the subscription ack.
        ack_raw        = await asyncio.wait_for(ws.recv(), timeout=config.SUB_ACK_TIMEOUT_S)
        ack_arrival_ns = time.perf_counter_ns()
        print(f"[connection] Polymarket subscription ack received.")
        ready_event.set()

        # The ack itself could be a trade on a very busy market — process it.
        if state.data_valid:
            try:
                msg     = json.loads(ack_raw)
                tx_hash = msg.get("transactionHash")
                if tx_hash:
                    handle_poly_event(tx_hash, ack_arrival_ns - state.run_start_ns)
            except Exception:
                pass

        async for raw in ws:
            arrival_perf_ns = time.perf_counter_ns()  # ← first line; no earlier work

            if not state.data_valid:
                continue

            try:
                msg     = json.loads(raw)
                tx_hash = msg.get("transactionHash")
                if tx_hash:
                    handle_poly_event(tx_hash, arrival_perf_ns - state.run_start_ns)
            except Exception:
                pass


async def alchemy_listener(
    ready_event:        asyncio.Event,
    ws_url:             str,
    order_filled_topic: str,
) -> None:
    async with websockets.connect(ws_url) as ws:
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