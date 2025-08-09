import os
import json
import time
import asyncio
from typing import List, Dict, Optional, Any
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import httpx
import yaml
import redis.asyncio as redis
import websockets

from trailing import TrailingManager
from execution import BinanceExec, round_price_qty
from risk_engine import size_notional_eur
from db import (
    get_engine, get_session,
    insert_order_test, recent_orders,
    insert_fill, upsert_position, list_positions,
    settings_get, settings_set, ensure_settings_table,
    recent_fills, ensure_equity_table, insert_equity_point, get_equity_series,
)

# --- notify (Discord / Telegram) fallback-safe ---
try:
    from notify import notify as _notify
except Exception:
    async def _notify(text: str, extra: dict | None = None) -> None:
        print("[notify]", text, extra)

CONFIG_PATH = os.environ.get("CONFIG_PATH", "/app/config/app.yaml")
MODE = os.environ.get("BINANCE_MODE", "mainnet").lower()
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")

LIVE_TRADING = os.environ.get("LIVE_TRADING", "false").lower() == "true"
EXEC_TEST = os.environ.get("EXECUTION_TEST_ORDERS", "true").lower() == "true"

API_KEY = os.environ.get("BINANCE_API_KEY", "")
API_SECRET = os.environ.get("BINANCE_API_SECRET", "")

BINANCE_BASE = os.environ.get(
    "BINANCE_BASE",
    "https://testnet.binance.vision" if MODE == "testnet" else "https://api.binance.com"
)
USER_WS_BASE = "wss://testnet.binance.vision/ws" if MODE == "testnet" else "wss://stream.binance.com:9443/ws"

DB_URL = os.environ.get("DATABASE_URL", "postgresql+asyncpg://bot:botpass@postgres:5432/botdb")

# ---------------- App & CORS ----------------
app = FastAPI(title="Pro Trading Bot API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "PUT", "OPTIONS"],
    allow_headers=["*"],
    max_age=86400,
)

# ---------------- Config ----------------
class Config(BaseModel):
    env: str
    timezone: str
    symbols_seed: List[str]
    volume24h_min_eur: int
    timeframes: Dict[str, str]
    risk: Dict[str, Any]
    execution: Dict[str, Any]

with open(CONFIG_PATH, "r") as f:
    APP_CONFIG = Config(**yaml.safe_load(f))

# ---------------- Globals ----------------
r = redis.from_url(REDIS_URL, decode_responses=True)
trailing_mgr: TrailingManager | None = None
exec_client: BinanceExec | None = None
listen_key: Optional[str] = None

BOT_ENABLED = True
KILL_SWITCH_ACTIVE = False   # global guard on entries

# ---------------- Helpers ----------------
def deep_merge(a, b):
    if not isinstance(a, dict): a = {}
    if not isinstance(b, dict): b = {}
    out = dict(a)
    for k, v in (b or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = v
    return out

def paris_today_str() -> str:
    return datetime.now(ZoneInfo("Europe/Paris")).date().isoformat()

# --- Exposure helpers ---
async def get_mark(symbol: str) -> float | None:
    raw = await r.hget("last_ticks", symbol)
    if not raw: return None
    try:
        return float(json.loads(raw)["close"])
    except Exception:
        return None

async def portfolio_equity_eur() -> float:
    total = 0.0
    async with get_session() as s:
        pos = await list_positions(s)
    for p in pos:
        mark = await get_mark(p["symbol"]) or p["avg_px"]
        total += float(p["qty"]) * float(mark)
    return max(total, 1.0)

async def portfolio_exposure_eur() -> float:
    exp = 0.0
    async with get_session() as s:
        pos = await list_positions(s)
    for p in pos:
        mark = await get_mark(p["symbol"]) or p["avg_px"]
        exp += max(0.0, float(p["qty"])) * float(mark)
    return exp

async def exposure_caps_ok(symbol: str, add_notional_eur: float, overrides: dict) -> tuple[bool, str]:
    risk = (overrides.get("risk") or {})
    expo = (risk.get("exposure") or {})
    max_port_pct = float(expo.get("max_portfolio_expo_pct", 30.0)) / 100.0
    min_cash_eur = float(expo.get("min_cash_reserve_eur", 0.0))
    max_pos_total = int(expo.get("max_positions_total", 99))
    max_sym_notional = float(expo.get("max_per_symbol_notional_eur", 1e12))
    one_pos_per_sym = bool(expo.get("one_position_per_symbol", True))

    async with get_session() as s:
        positions = await list_positions(s)
    pos_map = {p["symbol"]: p for p in positions}
    cur_expo = await portfolio_exposure_eur()
    eq = await portfolio_equity_eur()

    if len(positions) >= max_pos_total and (symbol not in pos_map):
        return False, f"max positions reached ({len(positions)}/{max_pos_total})"

    if one_pos_per_sym and (symbol in pos_map) and float(pos_map[symbol]["qty"]) > 0:
        return False, f"already long {symbol} and one_position_per_symbol=true"

    if symbol in pos_map:
        mark = await get_mark(symbol) or float(pos_map[symbol]["avg_px"])
        cur_sym_notional = float(pos_map[symbol]["qty"]) * mark
    else:
        cur_sym_notional = 0.0
    if (cur_sym_notional + add_notional_eur) > max_sym_notional:
        return False, f"symbol notional cap ({cur_sym_notional+add_notional_eur:.2f}>{max_sym_notional:.2f}€)"

    next_expo = cur_expo + add_notional_eur
    if next_expo > max_port_pct * eq:
        return False, f"portfolio expo cap ({next_expo:.2f}€ > {max_port_pct*100:.1f}% of {eq:.2f}€)"
    if (eq - next_expo) < min_cash_eur:
        return False, f"cash reserve cap ({eq-next_expo:.2f}€ < {min_cash_eur:.2f}€)"
    return True, "ok"

async def positions_with_pnl():
    async with get_session() as s:
        pos = await list_positions(s)
    out = []
    total_unreal = 0.0
    for p in pos:
        mark = await get_mark(p["symbol"])
        avg = float(p["avg_px"]); qty = float(p["qty"])
        pnl = ((mark - avg) * qty) if (mark is not None) else 0.0
        total_unreal += pnl
        out.append({
            **p,
            "mark": mark,
            "unreal_pnl": pnl,
            "unreal_pnl_pct": (pnl / (avg * qty) * 100.0) if (avg * qty) > 0 else None
        })
    return out, total_unreal

def kill_threshold_eur() -> float:
    eq0 = float(APP_CONFIG.risk.get("initial_equity_eur", 140.0))
    max_dd = float(APP_CONFIG.risk.get("max_daily_loss_pct", 6.0)) / 100.0
    return -eq0 * max_dd

# ---------- Equity & PnL day ----------
EQUITY_CACHE_TTL = 30.0  # sec

async def compute_equity_eur_now() -> Optional[float]:
    if exec_client is None:
        return None
    try:
        acct = exec_client.client.account()
        balances = acct.get("balances", [])
    except Exception as e:
        print("[equity] account() error:", e)
        return None

    symbols = set(APP_CONFIG.symbols_seed or [])

    async def symbol_mark(sym: str) -> Optional[float]:
        last = await r.hget("last_ticks", sym)
        if not last:
            return None
        try:
            return float(json.loads(last)["close"])
        except Exception:
            return None

    total = 0.0
    for b in balances:
        asset = b.get("asset")
        if not asset:
            continue
        free = float(b.get("free", "0") or 0.0)
        locked = float(b.get("locked", "0") or 0.0)
        qty = free + locked
        if qty <= 0:
            continue
        if asset == "EUR":
            total += qty
            continue
        sym = f"{asset}EUR"
        if sym not in symbols:
            continue
        px = await symbol_mark(sym)
        if px:
            total += qty * px
    return total

async def get_equity_eur_cached() -> Optional[float]:
    try:
        ts = await r.get("equity:eur:ts")
        val = await r.get("equity:eur:val")
        now = time.time()
        if ts and val:
            age = now - float(ts)
            if age < EQUITY_CACHE_TTL:
                return float(val)
        eq = await compute_equity_eur_now()
        if eq is not None:
            await r.set("equity:eur:val", str(eq))
            await r.set("equity:eur:ts", str(now))
        return eq
    except Exception as e:
        print("[equity] cache error:", e)
        return None

async def ensure_daily_baseline():
    try:
        today = paris_today_str()
        cur = await r.get("pnl:day")
        if cur == today:
            return
        eq = await compute_equity_eur_now()
        if eq is None:
            return
        await r.set("pnl:baseline_eur", str(eq))
        await r.set("pnl:day", today)
        await r.set("config:last_reload", datetime.now(timezone.utc).isoformat())
        global KILL_SWITCH_ACTIVE
        KILL_SWITCH_ACTIVE = False
        print(f"[pnl] baseline set {today} = {eq:.2f}€ (Europe/Paris)")
    except Exception as e:
        print("[pnl] ensure baseline error:", e)

async def daily_baseline_job():
    while True:
        await ensure_daily_baseline()
        await asyncio.sleep(30)

async def get_daily_pnl() -> Optional[float]:
    try:
        base = await r.get("pnl:baseline_eur")
        if base is None:
            await ensure_daily_baseline()
            base = await r.get("pnl:baseline_eur")
        if base is None:
            return None
        base = float(base)
        eq = await get_equity_eur_cached()
        if eq is None:
            return None
        return eq - base
    except Exception as e:
        print("[pnl] daily pnl error:", e)
        return None

# ---------------- Health & config ----------------
@app.get("/health")
async def health():
    return {"status": "ok", "env": APP_CONFIG.env, "mode": MODE, "live": LIVE_TRADING, "testOrders": EXEC_TEST}

@app.get("/config")
async def get_config():
    return APP_CONFIG.model_dump()

@app.get("/symbols")
async def get_symbols():
    return {"symbols": APP_CONFIG.symbols_seed}

@app.get("/filters/{symbol}")
async def get_filters(symbol: str):
    async with httpx.AsyncClient(base_url=BINANCE_BASE, timeout=10) as client:
        ex = await client.get("/api/v3/exchangeInfo", params={"symbol": symbol})
        ex.raise_for_status()
    data = ex.json()
    if not data.get("symbols"):
        return {"symbol": symbol, "filters": []}
    return data["symbols"][0]

# ---------------- Bot ON/OFF & statut ----------------
@app.get("/bot/status")
async def bot_status():
    _, unreal = await positions_with_pnl()
    thresh = kill_threshold_eur()
    last_reload = await r.get("config:last_reload")
    day = await r.get("pnl:day")
    daily_pnl = await get_daily_pnl()
    daily_limit = -float(APP_CONFIG.risk.get("max_daily_loss_pct", 6.0)) * float(APP_CONFIG.risk.get("initial_equity_eur", 140.0)) / 100.0
    eq_now = await get_equity_eur_cached()
    return {
        "enabled": BOT_ENABLED,
        "kill_active": KILL_SWITCH_ACTIVE,
        "unreal_pnl": unreal,
        "kill_threshold_eur": thresh,
        "mode": "live" if (LIVE_TRADING and not EXEC_TEST) else "paper",
        "config_last_reload": last_reload,
        "pnl_day": day,
        "equity_eur": eq_now,
        "daily_pnl_eur": daily_pnl,
        "daily_limit_eur": daily_limit,
    }

@app.post("/bot/enable")
async def bot_enable():
    global BOT_ENABLED
    BOT_ENABLED = True
    return {"ok": True, "enabled": BOT_ENABLED}

@app.post("/bot/disable")
async def bot_disable():
    global BOT_ENABLED
    BOT_ENABLED = False
    return {"ok": True, "enabled": BOT_ENABLED}

# ---------------- PNL endpoints ----------------
@app.get("/pnl/daily")
async def pnl_daily():
    day = await r.get("pnl:day")
    eq = await get_equity_eur_cached()
    base = await r.get("pnl:baseline_eur")
    pnl = await get_daily_pnl()
    return {
        "day": day,
        "equity_eur": eq,
        "baseline_eur": float(base) if base else None,
        "daily_pnl_eur": pnl
    }

@app.post("/pnl/reset")
async def pnl_reset():
    await ensure_daily_baseline()
    base = await r.get("pnl:baseline_eur")
    return {"ok": True, "baseline_eur": float(base) if base else None, "day": await r.get("pnl:day")}

# ---------------- ORDERS (TEST) ----------------
class TestOrder(BaseModel):
    symbol: str
    side: str  # BUY/SELL
    type: str = "MARKET"
    quote_eur: float

@app.post("/trade/test")
async def trade_test(order: TestOrder):
    if exec_client is None:
        return {"ok": False, "error": "exec_client not initialized (missing keys?)"}
    try:
        res = exec_client.order_test_market_quote(order.symbol, order.side.upper(), order.quote_eur)
        async with get_session() as s:
            oid = await insert_order_test(s, order.symbol, order.side.upper(), order.quote_eur, "TEST_OK")
            await s.commit()
        return {"ok": True, "binance": res, "order_id": oid}
    except Exception as e:
        async with get_session() as s:
            oid = await insert_order_test(s, order.symbol, order.side.upper(), order.quote_eur, f"TEST_ERR:{e}")
            await s.commit()
        return {"ok": False, "error": str(e), "order_id": oid}

# ---------------- ORDERS views ----------------
@app.get("/orders")
async def get_orders():
    async with get_session() as s:
        data = await recent_orders(s, 50)
        return {"orders": data}

@app.get("/fills")
async def get_fills(limit: int = 100):
    async with get_session() as s:
        data = await recent_fills(s, min(max(int(limit), 1), 500))
        return {"fills": data}

@app.get("/positions")
async def get_positions():
    pos, total_unreal = await positions_with_pnl()
    return {"positions": pos, "total_unreal": total_unreal}

# ---------------- SIM Fill (paper) ----------------
class SimFill(BaseModel):
    symbol: str
    price: float
    qty: float   # +achat / -vente
    order_id: int | None = None
    fee: float = 0.0

@app.post("/sim/fill")
async def sim_fill(body: SimFill):
    async with get_session() as s:
        fid = await insert_fill(s, body.order_id, body.price, body.qty, body.fee)
        pid = await upsert_position(s, body.symbol, body.qty, body.price)
        await s.commit()
    return {"ok": True, "fill_id": fid, "position_id": pid}

# ---------------- PARTIAL CLOSE ----------------
class CloseReq(BaseModel):
    percent: Optional[float] = None   # 25/50/100
    qty: Optional[float] = None       # qty base (au besoin)

@app.post("/positions/{symbol}/close")
async def close_position(symbol: str, body: CloseReq):
    if exec_client is None:
        raise HTTPException(400, "exec_client not initialized (keys missing).")
    async with get_session() as s:
        pos_list = await list_positions(s)
    pos = next((p for p in pos_list if p["symbol"] == symbol), None)
    if not pos:
        raise HTTPException(404, f"Aucune position ouverte sur {symbol}")

    qty_pos = float(pos["qty"])
    if qty_pos <= 0:
        raise HTTPException(400, "Position non positive (rien à vendre).")

    if body.qty and body.qty > 0:
        qty_sell = min(qty_pos, float(body.qty))
    else:
        pct = max(0.0, min(1.0, (body.percent or 100.0) / 100.0))
        qty_sell = qty_pos * pct

    info = exec_client.exchange_info(symbol)
    _, qty_sell = round_price_qty(1.0, qty_sell, info)

    if qty_sell <= 0:
        raise HTTPException(400, "Quantité après arrondi est nulle.")

    if EXEC_TEST or not LIVE_TRADING:
        res = exec_client.order_test_market_qty(symbol, "SELL", qty_sell)
        px = await get_mark(symbol)
        if px:
            async with get_session() as s:
                await insert_fill(s, None, px, -qty_sell, 0.0)
                await upsert_position(s, symbol, -qty_sell, px)
                await s.commit()
        # notify close
        try:
            await _notify(f"🔔 Sortie {symbol} @ {px:.6f} (close) qty={qty_sell}")
        except Exception:
            pass
        return {"ok": True, "mode": "paper", "binance": res, "qty": qty_sell}

    res = exec_client.order_market_qty(symbol, "SELL", qty_sell)
    try:
        await _notify(f"🔔 Sortie LIVE {symbol} qty={qty_sell}")
    except Exception:
        pass
    return {"ok": True, "mode": "live", "binance": res, "qty": qty_sell}

# ---------------- OCO (TP/SL) ----------------
class OcoReq(BaseModel):
    percent_of_position: float = 100.0  # 0-100
    tp_pct: float = 2.0                 # +2%
    sl_pct: float = 1.0                 # -1%
    basis: str = "avg"                  # 'avg' ou 'last'

@app.post("/positions/{symbol}/oco")
async def place_oco(symbol: str, body: OcoReq):
    if exec_client is None:
        raise HTTPException(400, "exec_client not initialized (keys missing).")
    async with get_session() as s:
        pos_list = await list_positions(s)
    pos = next((p for p in pos_list if p["symbol"] == symbol), None)
    if not pos or float(pos["qty"]) <= 0:
        raise HTTPException(404, f"Aucune position acheteuse sur {symbol}")

    qty_pos = float(pos["qty"])
    pct = max(0.0, min(100.0, float(body.percent_of_position))) / 100.0
    qty_set = qty_pos * pct

    p0 = float(pos["avg_px"])
    if body.basis == "last":
        mark = await get_mark(symbol)
        if mark:
            p0 = mark

    tp_price = p0 * (1.0 + float(body.tp_pct) / 100.0)
    sl_stop  = p0 * (1.0 - float(body.sl_pct) / 100.0)
    sl_limit = sl_stop * (1.0 - 0.001)

    info = exec_client.exchange_info(symbol)
    tp_price, qty_set = round_price_qty(tp_price, qty_set, info)
    sl_stop, _        = round_price_qty(sl_stop,  qty_set, info)
    sl_limit, qty_set = round_price_qty(sl_limit, qty_set, info)

    if qty_set <= 0:
        raise HTTPException(400, "Quantité après arrondi est nulle.")

    if EXEC_TEST or not LIVE_TRADING:
        async with get_session() as s:
            await insert_order_test(s, symbol, "SELL", 0.0, f"OCO_SIM: tp={tp_price}, sl={sl_stop}/{sl_limit}")
            await s.commit()
        return {"ok": True, "mode": "paper", "info": {"qty": qty_set, "tp": tp_price, "sl": [sl_stop, sl_limit]}}

    res = exec_client.new_oco_sell(symbol, qty_set, tp_price, sl_stop, sl_limit)
    return {"ok": True, "mode": "live", "binance": res}

# ---------------- WS → UI ----------------
class ConnectionManager:
    def __init__(self): self.active: list[WebSocket] = []
    async def connect(self, ws: WebSocket): await ws.accept(); self.active.append(ws)
    def disconnect(self, ws: WebSocket):
        if ws in self.active: self.active.remove(ws)
    async def send(self, ws: WebSocket, message: str):
        try: await ws.send_text(message)
        except Exception: self.disconnect(ws)

manager_ticks = ConnectionManager()
manager_signals = ConnectionManager()

@app.websocket("/ws/ticks")
async def ws_ticks(ws: WebSocket):
    await manager_ticks.connect(ws)
    try:
        pubsub = r.pubsub(); await pubsub.subscribe("ticks")
        async for msg in pubsub.listen():
            if msg and msg["type"] == "message":
                await manager_ticks.send(ws, msg["data"])
    except WebSocketDisconnect:
        manager_ticks.disconnect(ws)
    except Exception:
        manager_ticks.disconnect(ws)

@app.websocket("/ws/signals")
async def ws_signals(ws: WebSocket):
    await manager_signals.connect(ws)
    try:
        pubsub = r.pubsub(); await pubsub.subscribe("signals")
        async for msg in pubsub.listen():
            if msg and msg["type"] == "message":
                await manager_signals.send(ws, msg["data"])
    except WebSocketDisconnect:
        manager_signals.disconnect(ws)
    except Exception:
        manager_signals.disconnect(ws)

@app.get("/config/effective")
async def get_config_effective():
    base = APP_CONFIG.model_dump()
    async with get_session() as s:
        ov = await settings_get(s, "overrides")
    eff = deep_merge(base, ov or {})
    return {"config": eff}

@app.get("/config/overrides")
async def get_config_overrides():
    async with get_session() as s:
        ov = await settings_get(s, "overrides")
    return {"overrides": ov or {}}

@app.put("/config/overrides")
async def put_overrides(body: dict):
    async with get_session() as s:
        cur = await settings_get(s, "overrides") or {}
        new = deep_merge(cur, body or {})
        await settings_set(s, "overrides", new)
    await r.publish("config:updates", json.dumps(body or {}))
    return {"ok": True, "overrides": new}

@app.post("/debug/publish_signal")
async def debug_publish_signal(body: dict):
    await r.publish("signals", json.dumps(body or {}))
    return {"ok": True}

# ---- debug notify (Discord test) ----
@app.post("/debug/notify")
async def debug_notify(body: dict):
    msg = body.get("text") or "Ping Discord ✅"
    extra = body.get("extra") or None
    await _notify(msg, extra)
    return {"ok": True}

# ---- exposure status (pour UI Risk) ----
@app.get("/exposure/status")
async def exposure_status():
    async with get_session() as s:
        ov = await settings_get(s, "overrides") or {}
        positions = await list_positions(s)
    eq = await portfolio_equity_eur()
    expo = await portfolio_exposure_eur()
    cap_cfg = (ov.get("risk", {}).get("exposure") or {})
    return {
        "equity_eur": eq,
        "exposure_eur": expo,
        "free_cash_eur": max(0.0, eq - expo),
        "config": {
            "max_portfolio_expo_pct": cap_cfg.get("max_portfolio_expo_pct", 30.0),
            "min_cash_reserve_eur": cap_cfg.get("min_cash_reserve_eur", 0.0),
            "max_positions_total": cap_cfg.get("max_positions_total", 99),
            "max_per_symbol_notional_eur": cap_cfg.get("max_per_symbol_notional_eur", 1e12),
            "one_position_per_symbol": bool(cap_cfg.get("one_position_per_symbol", True)),
        },
        "positions": positions,
    }

# ---------------- Auto-OCO/Trailing control ----------------
@app.get("/auto_oco")
async def auto_oco_states():
    if trailing_mgr is None:
        return {"states": {}}
    st = await trailing_mgr.list_states()
    parsed = {}
    for k, v in (st or {}).items():
        try: parsed[k] = json.loads(v)
        except: parsed[k] = v
    return {"states": parsed}

class AutoOcoStart(BaseModel):
    percent: float = 100.0
    tp_pct: float | None = None
    sl_pct: float | None = None
    trail_mult: float = 1.2
    basis: str = "avg"  # "avg" ou "last"

@app.post("/positions/{symbol}/auto_oco/start")
async def auto_oco_start(symbol: str, body: AutoOcoStart):
    if trailing_mgr is None:
        raise HTTPException(400, "trailing manager indisponible")

    async with get_session() as s:
        pos_list = await list_positions(s)
    pos = next((p for p in pos_list if p["symbol"] == symbol), None)
    if not pos or float(pos["qty"]) <= 0:
        raise HTTPException(404, f"Aucune position acheteuse sur {symbol}")

    qty_pos = float(pos["qty"])
    pct = max(0.0, min(1.0, (body.percent or 100.0) / 100.0))
    qty_set = qty_pos * pct

    price = float(pos["avg_px"])
    if body.basis == "last":
        m = await get_mark(symbol)
        if m: price = m

    tp_pct = float(body.tp_pct if body.tp_pct is not None else 0.016)
    sl_pct = float(body.sl_pct if body.sl_pct is not None else 0.008)

    await trailing_mgr.start(symbol, price, qty_set, tp_pct, sl_pct, pct, float(body.trail_mult))
    return {"ok": True, "symbol": symbol, "qty": qty_set, "entry_px": price, "tp_pct": tp_pct, "sl_pct": sl_pct}

@app.post("/positions/{symbol}/auto_oco/stop")
async def auto_oco_stop(symbol: str):
    if trailing_mgr is None:
        raise HTTPException(400, "trailing manager indisponible")
    await trailing_mgr.stop(symbol)
    return {"ok": True}

# ---------------- Consumer: signals → sizing ----------------
async def consume_signals_loop():
    global KILL_SWITCH_ACTIVE
    if exec_client is None:
        print("[exec] no exec_client (no keys) → signals ignored"); return
    pubsub = r.pubsub(); await pubsub.subscribe("signals")
    print("[signals] consumer started")
    async for msg in pubsub.listen():
        if not msg or msg["type"] != "message": continue
        try:
            s = json.loads(msg["data"])
            symbol = s["symbol"]; side = s.get("side", "BUY").upper()
            score = float(s.get("score", 0.0)); stop_pct = float(s.get("stop_pct", 0.01))

            # 0) bot enable ?
            if not BOT_ENABLED:
                print(f"[skip] bot disabled → ignore {symbol}")
                continue

            # 1) Kill-switch JOURNALIER
            daily_pnl = await get_daily_pnl()
            daily_limit = -float(APP_CONFIG.risk.get("max_daily_loss_pct", 6.0)) * float(APP_CONFIG.risk.get("initial_equity_eur", 140.0)) / 100.0
            if daily_pnl is not None and daily_pnl <= daily_limit:
                KILL_SWITCH_ACTIVE = True
                print(f"[killswitch-day] pnl={daily_pnl:.2f}€ ≤ {daily_limit:.2f}€ → block new entries")
                continue
            else:
                KILL_SWITCH_ACTIVE = False

            # 2) Kill-switch latent (sécurité)
            _, unreal = await positions_with_pnl()
            if unreal <= kill_threshold_eur():
                KILL_SWITCH_ACTIVE = True
                print(f"[killswitch-latent] unreal={unreal:.2f}€ ≤ {kill_threshold_eur():.2f}€ → block")
                continue

            # Sizing
            equity_eur = float(APP_CONFIG.risk.get("initial_equity_eur", 140.0))
            base_risk_pct = float(APP_CONFIG.risk.get("base_risk_pct", 0.8))
            hard_cap_pct = float(APP_CONFIG.risk.get("hard_cap_pct", 5.0))
            info = exec_client.exchange_info(symbol)
            sizing = size_notional_eur(equity_eur, base_risk_pct, hard_cap_pct, score, stop_pct, info)
            quote_eur = sizing["notional_eur"]

            # ---- CAPS d'exposition ----
            async with get_session() as sess:
                ov_all = await settings_get(sess, "overrides") or {}
            ok_caps, reason = await exposure_caps_ok(symbol, quote_eur, ov_all or {})
            if not ok_caps:
                print(f"[exposure] BLOCK {symbol} notional≈{quote_eur:.2f}€ -> {reason}")
                try:
                    await _notify(f"❌ Entrée bloquée {symbol} ({quote_eur:.2f}€) — {reason}")
                except Exception:
                    pass
                async with get_session() as s2:
                    await insert_order_test(s2, symbol, side, quote_eur, f"BLOCK:{reason}")
                    await s2.commit()
                continue

            if not EXEC_TEST and not LIVE_TRADING:
                print(f"[dry] {symbol} {side} notional≈{quote_eur:.2f}€ score={score:.2f}")
                continue

            try:
                res = exec_client.order_test_market_quote(symbol, side, quote_eur)
                print(f"[order.test] {symbol} {side} notional≈{quote_eur:.2f}€ OK")

                # SIM FILL (paper) au dernier prix
                px = await get_mark(symbol)
                if px:
                    raw_qty = max(0.0, quote_eur / px)
                    info = exec_client.exchange_info(symbol)
                    _, qty = round_price_qty(px, raw_qty, info)
                    if qty > 0:
                        async with get_session() as sess:
                            await insert_fill(sess, None, px, +qty, 0.0)  # BUY
                            await upsert_position(sess, symbol, +qty, px)
                            await sess.commit()

                        # notify entrée
                        try:
                            await _notify(f"✅ Entrée {symbol} ~{quote_eur:.2f}€ @ {px:.6f}", {"qty": qty})
                        except Exception:
                            pass

                        # Charger overrides stratégie pour auto-OCO / trailing
                        async with get_session() as sess:
                            ov = await settings_get(sess, "overrides") or {}
                        strat_ov = (ov.get("strategy") or {})
                        auto_oco_enabled = bool(strat_ov.get("auto_oco_enabled", True))
                        trailing_enabled = bool(strat_ov.get("trailing_enabled", True))
                        trail_mult = float(strat_ov.get("atr_trail_mult", 1.2))
                        oco_percent = float(strat_ov.get("oco_percent", 100.0)) / 100.0

                        tp_pct = float(s.get("tp_pct", 0.0) or 0.0)
                        sl_pct = float(s.get("stop_pct", 0.0) or 0.0)

                        if (auto_oco_enabled or trailing_enabled) and (tp_pct > 0 or sl_pct > 0):
                            if trailing_mgr:
                                await trailing_mgr.start(
                                    symbol, px, qty * oco_percent,
                                    tp_pct=max(0.0, tp_pct),
                                    sl_pct=max(0.0, sl_pct),
                                    percent=oco_percent,
                                    trail_mult=trail_mult
                                )
                                print(f"[auto-oco] started {symbol} tp_pct={tp_pct:.4f} sl_pct={sl_pct:.4f} trail_mult={trail_mult} pct={oco_percent*100:.1f}%")

                async with get_session() as sess:
                    await insert_order_test(sess, symbol, side, quote_eur, "TEST_OK"); await sess.commit()
            except Exception as e:
                print(f"[order.test] ERROR {symbol}: {e}")
                async with get_session() as sess:
                    await insert_order_test(sess, symbol, side, quote_eur, f"TEST_ERR:{e}"); await sess.commit()
        except Exception as e:
            print("[signals] parse/error:", e)

# ---------------- Equity snapshot (1/min) & series ----------------
async def equity_snapshot_job():
    """Prend un point d'equity chaque minute (UTC)."""
    await asyncio.sleep(3)  # petit délai au boot
    while True:
        try:
            eq = await compute_equity_eur_now()
            if eq is not None:
                async with get_session() as s:
                    await insert_equity_point(s, datetime.now(timezone.utc), float(eq))
                    await s.commit()
        except Exception as e:
            print("[equity] snapshot error:", e)
        # attend jusqu'à la prochaine minute "entière"
        now = datetime.now(timezone.utc)
        sleep_s = 60 - (now.second + now.microsecond/1e6)
        await asyncio.sleep(max(1.0, sleep_s))

@app.get("/equity/series")
async def equity_series(range: str = "1d"):
    """Renvoie la série d'equity pour 1d / 7d / 30d avec quelques stats."""
    now = datetime.now(timezone.utc)
    rng = (range or "1d").lower()
    days = 1
    if rng in ("1d","1day","day"): days = 1
    elif rng in ("7d","7days","week"): days = 7
    elif rng in ("30d","30days","month"): days = 30
    else:
        days = 1
    since = now - timedelta(days=days)
    async with get_session() as s:
        pts = await get_equity_series(s, since)
    if not pts:
        return {"range": rng, "points": [], "stats": None}
    start = pts[0]["equity_eur"]; end = pts[-1]["equity_eur"]
    change = end - start
    change_pct = (change / start * 100.0) if start else None
    stats = {
        "start": start, "end": end,
        "min": min(p["equity_eur"] for p in pts),
        "max": max(p["equity_eur"] for p in pts),
        "change_eur": change, "change_pct": change_pct
    }
    # format ISO pour ts (Next aime bien)
    out = [{"ts": p["ts"].isoformat(), "equity_eur": p["equity_eur"]} for p in pts]
    return {"range": rng, "points": out, "stats": stats}


# ---------------- User Data Stream (fills live) ----------------
async def user_stream_task():
    global listen_key
    if exec_client is None or not API_KEY:
        print("[uds] disabled (no keys)"); return

    def new_key():
        try:
            return exec_client.client.new_listen_key()["listenKey"]
        except Exception as e:
            print("[uds] new_listen_key error:", e); return None

    listen_key = new_key()
    if not listen_key:
        print("[uds] no listenKey -> abort"); return

    async def keepalive():
        while True:
            await asyncio.sleep(30 * 60)
            try:
                exec_client.client.renew_listen_key(listen_key)
            except Exception as e:
                print("[uds] keepalive failed, recreating:", e)
                nk = new_key()
                if nk:
                    listen_key = nk

    async def consume():
        url = f"{USER_WS_BASE}/{listen_key}"
        while True:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    print("[uds] connected", url)
                    async for msg in ws:
                        try:
                            data = json.loads(msg)
                            if data.get("e") == "executionReport":
                                await handle_exec_report(data)
                        except Exception:
                            pass
            except Exception as e:
                print("[uds] ws error:", e)
                await asyncio.sleep(3)

    async def handle_exec_report(ev: Dict):
        try:
            if ev.get("x") != "TRADE":
                return
            symbol = ev.get("s")
            side   = ev.get("S", "BUY")
            qty    = float(ev.get("l", "0"))
            price  = float(ev.get("L", "0"))
            fee    = float(ev.get("n", "0")) if ev.get("n") else 0.0
            if qty <= 0 or price <= 0:
                return
            qty_signed = qty if side == "BUY" else -qty
            async with get_session() as s:
                await insert_fill(s, None, price, qty_signed, fee)
                await upsert_position(s, symbol, qty_signed, price)
                await s.commit()
            print(f"[uds] fill {symbol} {side} qty={qty} px={price}")
        except Exception as e:
            print("[uds] handle error:", e)

    asyncio.create_task(keepalive())
    asyncio.create_task(consume())

# ---------------- Startup ----------------
@app.on_event("startup")
async def on_startup():
    global exec_client
    if API_KEY and API_SECRET:
        exec_client = BinanceExec(MODE, API_KEY, API_SECRET)
        print(f"[exec] client ready (mode={MODE})")
    else:
        exec_client = None
        print("[exec] no keys provided → execution disabled")

    get_engine(DB_URL)
    async with get_session() as s:
        await ensure_settings_table(s)

    async with get_session() as s:
        await ensure_equity_table(s)

    asyncio.create_task(equity_snapshot_job())


    asyncio.create_task(consume_signals_loop())
    asyncio.create_task(user_stream_task())
    asyncio.create_task(daily_baseline_job())

    global trailing_mgr
    trailing_mgr = TrailingManager(r)
    asyncio.create_task(trailing_mgr.loop())
