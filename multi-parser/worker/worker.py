"""
Multi-Machine Parser Worker for Quantframe.
Fetches order data from Warframe Market API through a proxy
and sends results back to the Host (Quantframe Tauri app) via WebSocket.
"""

import asyncio
import json
import logging
import os
import signal
import sys
import time

import aiohttp
from aiohttp_socks import ProxyConnector
import websockets

# Configuration from environment variables
HOST_URL = os.environ.get("HOST_URL", "ws://localhost:8765")
PROXY_URL = os.environ.get("PROXY_URL", "")
WORKER_ID = os.environ.get("WORKER_ID", "worker-unknown")
RATE_LIMIT_MS = int(os.environ.get("RATE_LIMIT_MS", "334"))
WFM_API_BASE = "https://api.warframe.market/v2"
RECONNECT_DELAY = 5  # seconds

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format=f"[{WORKER_ID}] %(asctime)s %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


async def fetch_orders(session: aiohttp.ClientSession, slug: str, fast_check: bool = False) -> dict:
    """
    Fetch orders for a given item.
    If fast_check is True, uses /top endpoint (lighter, pre-sorted).
    Otherwise uses /orders endpoint (full list).
    Returns { sell_orders: [...], buy_orders: [...] }.
    """
    if fast_check:
        url = f"{WFM_API_BASE}/orders/item/{slug}/top"
    else:
        url = f"{WFM_API_BASE}/orders/item/{slug}"

    headers = {
        "Accept": "application/json",
        "Language": "en",
    }
    async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
        if resp.status == 429:
            # Rate limited — wait and retry
            retry_after = int(resp.headers.get("Retry-After", "2"))
            log.warning(f"Rate limited on {slug}, waiting {retry_after}s")
            await asyncio.sleep(retry_after)
            raise aiohttp.ClientError(f"Rate limited for {slug}")

        resp.raise_for_status()
        body = await resp.json()
        
        # WFM V2 usually puts data in "payload", but sometimes "data" (or we fallback)
        payload = body.get("payload", body.get("data", {}))

        if fast_check:
            # /top returns { sell: [...], buy: [...] }
            sell = payload.get("sell", []) if isinstance(payload, dict) else []
            buy = payload.get("buy", []) if isinstance(payload, dict) else []
            return {"sell_orders": sell, "buy_orders": buy}

        # /orders returns a flat array in payload
        all_orders = payload if isinstance(payload, list) else payload.get("orders", [])
        sell = [o for o in all_orders if o.get("type") == "sell"]
        buy = [o for o in all_orders if o.get("type") == "buy"]
        return {"sell_orders": sell, "buy_orders": buy}


async def create_session() -> aiohttp.ClientSession:
    """Create an aiohttp session with optional SOCKS5/HTTP proxy."""
    if PROXY_URL:
        connector = ProxyConnector.from_url(PROXY_URL)
        log.info(f"Using proxy: {PROXY_URL[:30]}...")
    else:
        connector = aiohttp.TCPConnector()
        log.warning("No proxy configured — using direct connection")

    return aiohttp.ClientSession(connector=connector)


async def worker_loop():
    """Main worker loop: connect to Host, receive tasks, fetch data, send results."""
    session = await create_session()

    try:
        log.info(f"Connecting to Host at {HOST_URL}")
        async with websockets.connect(
            HOST_URL,
            ping_interval=20,
            ping_timeout=10,
            close_timeout=5,
        ) as ws:
            log.info("Connected to Host")

            while True:
                # Signal that we're ready for a task
                await ws.send(json.dumps({
                    "type": "ready",
                    "worker_id": WORKER_ID,
                }))

                # Wait for task from Host
                raw = await ws.recv()
                msg = json.loads(raw)
                msg_type = msg.get("type", "")

                if msg_type == "task":
                    slug = msg["slug"]
                    request_id = msg.get("request_id", "")
                    fast_check = msg.get("fast_check", False)
                    
                    mode_str = "FAST" if fast_check else "FULL"
                    log.info(f"Task: {slug} [{mode_str}] (req: {request_id[:8]})")

                    try:
                        start = time.monotonic()
                        data = await fetch_orders(session, slug, fast_check)
                        elapsed_ms = (time.monotonic() - start) * 1000

                        await ws.send(json.dumps({
                            "type": "result",
                            "request_id": request_id,
                            "slug": slug,
                            "data": data,
                            "elapsed_ms": round(elapsed_ms),
                            "worker_id": WORKER_ID,
                        }))
                        log.info(
                            f"Result: {slug} — "
                            f"{len(data.get('sell_orders', []))} sell, "
                            f"{len(data.get('buy_orders', []))} buy "
                            f"({elapsed_ms:.0f}ms)"
                        )
                    except Exception as e:
                        log.error(f"Error fetching {slug}: {e}")
                        await ws.send(json.dumps({
                            "type": "error",
                            "request_id": request_id,
                            "slug": slug,
                            "error": str(e),
                            "worker_id": WORKER_ID,
                        }))

                    # Rate limit: wait between API requests
                    await asyncio.sleep(RATE_LIMIT_MS / 1000)

                elif msg_type == "wait":
                    seconds = msg.get("seconds", 5)
                    log.info(f"Queue empty, waiting {seconds}s")
                    await asyncio.sleep(seconds)

                elif msg_type == "shutdown":
                    log.info("Received shutdown signal from Host")
                    break

                else:
                    log.warning(f"Unknown message type: {msg_type}")

    except websockets.ConnectionClosed as e:
        log.warning(f"Connection closed: {e}")
    except Exception as e:
        log.error(f"Worker error: {e}")
    finally:
        await session.close()


async def main():
    """Entry point: run worker with automatic reconnection."""
    log.info(f"Worker starting — ID: {WORKER_ID}")
    log.info(f"Host: {HOST_URL}")
    log.info(f"Proxy: {'configured' if PROXY_URL else 'NONE'}")
    log.info(f"Rate limit: {RATE_LIMIT_MS}ms")

    while True:
        try:
            await worker_loop()
        except Exception as e:
            log.error(f"Worker loop crashed: {e}")

        log.info(f"Reconnecting in {RECONNECT_DELAY}s...")
        await asyncio.sleep(RECONNECT_DELAY)


if __name__ == "__main__":
    # Handle graceful shutdown
    loop = asyncio.new_event_loop()

    def shutdown_handler():
        log.info("Shutting down...")
        for task in asyncio.all_tasks(loop):
            task.cancel()

    if sys.platform != "win32":
        loop.add_signal_handler(signal.SIGTERM, shutdown_handler)
        loop.add_signal_handler(signal.SIGINT, shutdown_handler)

    try:
        loop.run_until_complete(main())
    except (KeyboardInterrupt, asyncio.CancelledError):
        log.info("Worker stopped")
    finally:
        loop.close()
