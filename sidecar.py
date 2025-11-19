import os, json, time, asyncio, websockets
from collections import defaultdict
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

# --- Config ---
API_KEY = os.getenv("FINNHUB_API_KEY")
if not API_KEY:
    raise RuntimeError("FINNHUB_API_KEY is not set")
WS_URL = f"wss://ws.finnhub.io?token={API_KEY}"

# Dynamic subscription set
SUBS = set()  # symbols we want to track, e.g. "BINANCE:BTCUSDT"

# Shared state
tick = {}                     # latest price per symbol
livebars = defaultdict(dict)  # symbol -> {"1m": bar}

# Queue for subscribe commands from HTTP handlers to WS loop
pending_subs: asyncio.Queue = asyncio.Queue()


def new_bar(ts: int):
    start = ts - (ts % 60)
    return {
        "t": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(start)),
        "o": None, "h": None, "l": None, "c": None, "v": 0.0,
        "elapsed": 0,
        "remaining": 60,
        "complete": False
    }


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

                        # Update 1m live bar
                        bar = livebars[sym].get("1m") or new_bar(ts)
                        if bar["o"] is None:
                            bar["o"] = px
                            bar["h"] = px
                            bar["l"] = px
                            bar["c"] = px
                        else:
                            bar["h"] = max(bar["h"], px)
                            bar["l"] = min(bar["l"], px)
                            bar["c"] = px

                        bar["v"] += vol
                        now = time.time()
                        # Approximate elapsed time within bar
                        bar["elapsed"] = int(now - ts + (ts % 60))
                        bar["remaining"] = 60 - bar["elapsed"]
                        if bar["remaining"] <= 0:
                            bar["complete"] = True

                        livebars[sym]["1m"] = bar

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
    # Dynamically subscribe if needed
    await ensure_subscribed(symbol)

    bar = livebars.get(symbol, {}).get(tf)
    if not bar:
        raise HTTPException(status_code=404, detail="no candle yet for symbol")
    return JSONResponse(bar)
