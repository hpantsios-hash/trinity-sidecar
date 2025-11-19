import os
import json
import time
import asyncio
import websockets
from collections import defaultdict
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from synthetic_engine import SyntheticEngine, TimeframeConfig  # NEW


# --- Config ---
API_KEY = os.getenv("FINNHUB_API_KEY")
if not API_KEY:
    raise RuntimeError("FINNHUB_API_KEY is not set")

WS_URL = f"wss://ws.finnhub.io?token={API_KEY}"

# Dynamic subscription set
SUBS = set()  # symbols we want to track, e.g. "BINANCE:BTCUSDT", "AAPL", "QQQ"

# Latest tick per symbol
tick = {}

# --- Synthetic Candle Engine setup ---

TF_CONFIGS = [
    # Duration-based microstructure
    TimeframeConfig("1s", "duration", size_seconds=1),
    TimeframeConfig("2s", "duration", size_seconds=2),
    TimeframeConfig("5s", "duration", size_seconds=5),
    TimeframeConfig("10s", "duration", size_seconds=10),
    TimeframeConfig("12s", "duration", size_seconds=12),
    TimeframeConfig("30s", "duration", size_seconds=30),

    # Standard intraday
    TimeframeConfig("1m", "duration", size_seconds=60),
    TimeframeConfig("5m", "duration", size_seconds=300),
    TimeframeConfig("15m", "duration", size_seconds=900),
    TimeframeConfig("1h", "duration", size_seconds=3600),

    # Calendar-based higher TFs
    TimeframeConfig("day", "calendar", calendar_type="day"),
    TimeframeConfig("week", "calendar", calendar_type="week"),
    TimeframeConfig("month", "calendar", calendar_type="month"),
    TimeframeConfig("quarter", "calendar", calendar_type="quarter"),
    TimeframeConfig("year", "calendar", calendar_type="year"),

    # Multi-year macro
    TimeframeConfig("2y", "calendar", calendar_type="multi-year", years_span=2),
    TimeframeConfig("5y", "calendar", calendar_type="multi-year", years_span=5),
    TimeframeConfig("10y", "calendar", calendar_type="multi-year", years_span=10),
]

engine = SyntheticEngine(TF_CONFIGS)

# Queue for subscribe commands from HTTP handlers to WS loop
pending_subs: asyncio.Queue = asyncio.Queue()


async def ws_loop():
    """Single WebSocket loop that keeps Finnhub connection alive
    and listens for subscribe commands via pending_subs queue.
    """
    while True:
        try:
            async with websockets.connect(WS_URL) as ws:
                # Subscribe to anything already in SUBS at connect time
                for s in list(SUBS):
                    await ws.send(json.dumps({"type": "subscribe", "symbol": s}))

                while True:
                    # Try to drain any pending subscription commands
                    try:
                        cmd = pending_subs.get_nowait()
                    except asyncio.QueueEmpty:
                        cmd = None

                    if cmd:
                        await ws.send(cmd)

                    raw = await ws.recv()
                    msg = json.loads(raw)
                    data = msg.get("data") or []
                    for d in data:
                        sym = d["s"]
                        px = d["p"]
                        vol = d.get("v", 0.0)
                        ts = d["t"] // 1000  # ms -> s

                        # Update last tick
                        tick[sym] = {"symbol": sym, "price": px, "ts": ts}

                        # Feed synthetic engine for ALL timeframes
                        engine.ingest_tick(sym, px, vol, ts)

        except Exception as e:
            print("WS reconnect:", e)
            await asyncio.sleep(3)


async def ensure_subscribed(symbol: str):
    """Ensure symbol is in SUBS and queued for subscription."""
    if symbol not in SUBS:
        SUBS.add(symbol)
        cmd = json.dumps({"type": "subscribe", "symbol": symbol})
        await pending_subs.put(cmd)


app = FastAPI()


@app.on_event("startup")
async def startup():
    asyncio.create_task(ws_loop())


@app.get("/live/price")
async def live_price(symbol: str):
    # Dynamically subscribe if needed
    await ensure_subscribed(symbol)

    data = tick.get(symbol)
    if not data:
        # We are subscribed but haven't received a tick yet
        raise HTTPException(status_code=404, detail="no tick yet for symbol")
    return JSONResponse(data)


@app.get("/live/candle")
async def live_candle(symbol: str, tf: str = "1m"):
    """
    Backwards-compatible live candle endpoint.
    Uses SyntheticEngine under the hood (default tf=1m).
    """
    await ensure_subscribed(symbol)

    bar = engine.get_candle(symbol, tf)
    if not bar:
        raise HTTPException(status_code=404, detail="no candle yet for symbol")
    return JSONResponse(bar)


@app.get("/synthetic/candle")
async def synthetic_candle(symbol: str, tf: str):
    """
    Generic synthetic candle endpoint for arbitrary timeframe.
    Example: tf=1s,2s,5s,10s,12s,30s,1m,5m,15m,1h,day,week,month,quarter,year,2y,5y,10y
    """
    await ensure_subscribed(symbol)

    bar = engine.get_candle(symbol, tf)
    if not bar:
        raise HTTPException(status_code=404, detail="no candle yet for symbol/tf")
    return JSONResponse(bar)


@app.get("/synthetic/grid")
async def synthetic_grid(
    symbol: str,
    tfs: str = "1s,5s,1m,5m,15m,1h,day,week,month,quarter,year,5y",
):
    """
    Multi-timeframe grid endpoint.
    Returns a dict: { tf_name: candle_json } for requested tfs.
    """
    await ensure_subscribed(symbol)

    tf_list = [x.strip() for x in tfs.split(",") if x.strip()]
    result = {}
    for tf in tf_list:
        bar = engine.get_candle(symbol, tf)
        if bar:
            result[tf] = bar

    if not result:
        raise HTTPException(status_code=404, detail="no candles yet for symbol/tfs")
    return JSONResponse(result)
