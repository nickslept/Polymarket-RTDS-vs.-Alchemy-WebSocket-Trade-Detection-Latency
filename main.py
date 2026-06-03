"""
Usage:
    python main.py

You will be prompted for your Alchemy API key.  Output Parquet files are written to the output/ directory.
**note to self - add a more in depth explanation of main.py later
"""
import asyncio
import os
import time
from datetime import datetime

import config
import src.state as state
from src.connections import run_connections
from src.hashmap import ttl_sweep
from src.writers import trades_writer, orphans_writer


async def main() -> None:
    # --- User input -----------------------------------------------------------
    api_key     = input("Enter Alchemy API key: ").strip()
    alchemy_url = f"wss://polygon-mainnet.g.alchemy.com/v2/{api_key}"

    # --- Output paths ---------------------------------------------------------
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    run_ts       = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    trades_path  = os.path.join(config.OUTPUT_DIR, f"trades_{run_ts}.parquet")
    orphans_path = os.path.join(config.OUTPUT_DIR, f"orphans_{run_ts}.parquet")
    print(f"[startup] Writing trades  → {trades_path}")
    print(f"[startup] Writing orphans → {orphans_path}")

    # --- Initialise shared state (must happen before coroutines start) --------
    state.trades_queue  = asyncio.Queue()
    state.orphans_queue = asyncio.Queue()
    state.run_start_ns  = time.perf_counter_ns()

    # --- Launch all coroutines ------------------------------------------------
    await asyncio.gather(
        run_connections(alchemy_url, config.ORDER_FILLED_TOPIC),
        trades_writer(trades_path),
        orphans_writer(orphans_path),
        ttl_sweep(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[shutdown] Interrupted — flushing remaining data...")