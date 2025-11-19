import os, json, time, asyncio, websockets
from collections import defaultdict
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

API_KEY = os.getenv("FINNHUB_API_KEY")
WS_URL  = f"wss://ws.finnhub.io?token={API_KEY}"
SUBS    = {"BINANCE:BTCUSDT", "AAPL", "QQQ"}   # symbols to track

tick     = {}                 # latest price per symbol
livebars = defaultdict(dict)  # symbol -> {"1m": bar}

def new_bar(ts):
    start = ts - (ts % 60)
    return {
        "t": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(start)),
        "o": None, "h": None, "l": None, "c": None, "v": 0.0,
        "elapsed": 0, "remaining": 60, "complete": False
    }

async def ws_loop():
    while True:
        try:
            async with websockets.connect(WS_URL) as ws:
                for s in SUBS:
                    await ws.send(json.dumps({"type": "subscribe", "symbol": s}))
                async for raw in ws:
                    for d in json.loads(raw).get("data", []):
                        sym, px, vol, ts = d["s"], d["p"], d["v"], d["t"] // 1000
                        tick[sym] = {"symbol": sym, "price": px, "ts": ts}
                        bar = livebars[sym].get("1m") or new_bar(ts)
                        if bar["o"] is None:
                            bar.update(o=px, h=px, l=px, c=px)
                        bar["h"] = max(bar["h"], px)
                        bar["l"] = min(bar["l"], px)
                        bar["c"] = px
                        bar["v"] += vol
                        now = time.time()
                        bar["elapsed"]   = int(now - ts + (ts % 60))
                        bar["remaining"] = 60 - bar["elapsed"]
                        if bar["remaining"] <= 0:
                            bar["complete"] = True
                        livebars[sym]["1m"] = bar
        except Exception as e:
            print("WS reconnect:", e)
            await asyncio.sleep(3)

app = FastAPI()

@app.on_event("startup")
async def startup():
    asyncio.create_task(ws_loop())

@app.get("/live/price")
def live_price(symbol: str):
    if symbol not in tick:
        raise HTTPException(status_code=404, detail="symbol not tracked")
    return JSONResponse(tick[symbol])

@app.get("/live/candle")
def live_candle(symbol: str, tf: str = "1m"):
    bar = livebars.get(symbol, {}).get(tf)
    if not bar:
        raise HTTPException(status_code=404, detail="no candle yet")
    return JSONResponse(bar)
