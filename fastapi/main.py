import os
import json
import asyncio
import asyncpg
import logging
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from typing import Optional
from database import get_db

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

app = FastAPI(title="StreamIQ", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

SYMBOLS = os.getenv("SYMBOLS", "AAPL,GOOGL,MSFT,TSLA,NVDA").split(",")


# ── WebSocket Manager ────────────────────────────────────────────────────────

class ConnectionManager:
    def __init__(self):
        self.active: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)
        log.info(f"WS connected — {len(self.active)} active")

    def disconnect(self, ws: WebSocket):
        self.active.remove(ws)
        log.info(f"WS disconnected — {len(self.active)} active")

    async def broadcast(self, data: dict):
        dead = []
        for ws in self.active:
            try:
                await ws.send_json(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.active.remove(ws)

manager = ConnectionManager()


# ── Background broadcaster ────────────────────────────────────────────────────

async def broadcast_latest():
    """Every 5 seconds push latest processed tick for all symbols to all WS clients."""
    db_url = (
        f"postgresql://"
        f"{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
        f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}"
        f"/{os.getenv('POSTGRES_DB')}"
    )
    while True:
        await asyncio.sleep(5)
        if not manager.active:
            continue
        try:
            conn = await asyncpg.connect(db_url)
            rows = await conn.fetch("""
                SELECT DISTINCT ON (symbol)
                    symbol, ts, close, volume,
                    sma_20, sma_50, rsi_14,
                    volume_spike, signal, created_at
                FROM market_processed
                ORDER BY symbol, ts DESC
            """)
            await conn.close()

            if rows:
                payload = []
                for r in rows:
                    payload.append({
                        "symbol":       r["symbol"],
                        "ts":           r["ts"].isoformat() if r["ts"] else None,
                        "close":        float(r["close"]) if r["close"] else None,
                        "volume":       r["volume"],
                        "sma_20":       float(r["sma_20"]) if r["sma_20"] else None,
                        "sma_50":       float(r["sma_50"]) if r["sma_50"] else None,
                        "rsi_14":       float(r["rsi_14"]) if r["rsi_14"] else None,
                        "volume_spike": r["volume_spike"],
                        "signal":       r["signal"],
                    })
                await manager.broadcast({"type": "tick", "data": payload})
        except Exception as e:
            log.error(f"Broadcast error: {e}")


@app.on_event("startup")
async def startup():
    asyncio.create_task(broadcast_latest())


# ── REST Endpoints ────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/symbols")
async def get_symbols():
    return {"symbols": SYMBOLS}


@app.get("/latest")
async def get_latest(db: AsyncSession = Depends(get_db)):
    """Latest processed tick for all symbols."""
    result = await db.execute(text("""
        SELECT DISTINCT ON (symbol)
            symbol, ts, close, volume,
            sma_20, sma_50, rsi_14, volume_spike, signal
        FROM market_processed
        ORDER BY symbol, ts DESC
    """))
    rows = result.fetchall()
    return [dict(zip(result.keys(), r)) for r in rows]


@app.get("/history/{symbol}")
async def get_history(
    symbol: str,
    limit: int = 100,
    db: AsyncSession = Depends(get_db)
):
    """Historical processed ticks for a symbol — for chart rendering."""
    result = await db.execute(text("""
        SELECT ts, close, volume, sma_20, sma_50, rsi_14, volume_spike, signal
        FROM market_processed
        WHERE symbol = :symbol
        ORDER BY ts DESC
        LIMIT :limit
    """), {"symbol": symbol.upper(), "limit": limit})
    rows = result.fetchall()
    keys = result.keys()
    data = [dict(zip(keys, r)) for r in reversed(rows)]

    # Serialize timestamps
    for d in data:
        if d.get("ts") and hasattr(d["ts"], "isoformat"):
            d["ts"] = d["ts"].isoformat()
        for k in ["close","sma_20","sma_50","rsi_14"]:
            if d.get(k) is not None:
                d[k] = float(d[k])
    return data


@app.get("/stats")
async def get_stats(db: AsyncSession = Depends(get_db)):
    """Dashboard stats — signals breakdown, volume spikes count."""
    signals = await db.execute(text("""
        SELECT signal, COUNT(*) as count
        FROM market_processed
        GROUP BY signal
    """))
    spikes = await db.scalar(text("""
        SELECT COUNT(*) FROM market_processed WHERE volume_spike = TRUE
    """))
    total = await db.scalar(text("SELECT COUNT(*) FROM market_processed"))

    return {
        "total_ticks":    total,
        "volume_spikes":  spikes,
        "signals":        {r[0]: r[1] for r in signals},
    }


# ── WebSocket ────────────────────────────────────────────────────────────────

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True:
            await ws.receive_text()  # keep connection alive
    except WebSocketDisconnect:
        manager.disconnect(ws)


# ── Manual tick injection (for testing / market closed) ───────────────────

from pydantic import BaseModel as PM

class ManualTick(PM):
    symbol:    str
    close:     float
    volume:    int
    open:      float = 0
    high:      float = 0
    low:       float = 0

@app.post("/inject")
async def inject_tick(tick: ManualTick, db: AsyncSession = Depends(get_db)):
    """Manually inject a tick — useful when market is closed or yfinance is blocked."""
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)

    await db.execute(text("""
        INSERT INTO market_raw (symbol, ts, open, high, low, close, volume)
        VALUES (:symbol, :ts, :open, :high, :low, :close, :volume)
    """), {
        "symbol": tick.symbol.upper(), "ts": now,
        "open": tick.open or tick.close,
        "high": tick.high or tick.close,
        "low":  tick.low  or tick.close,
        "close": tick.close, "volume": tick.volume,
    })

    await db.execute(text("""
        INSERT INTO market_processed
            (symbol, ts, close, volume, sma_20, sma_50, rsi_14, volume_spike, signal)
        VALUES (:symbol, :ts, :close, :volume, :close, :close, 50, false, 'NEUTRAL')
    """), {"symbol": tick.symbol.upper(), "ts": now,
           "close": tick.close, "volume": tick.volume})

    await db.commit()
    return {"status": "injected", "symbol": tick.symbol.upper(), "close": tick.close}