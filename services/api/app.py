import os
import json
import time
import asyncio
from typing import List, Tuple, Dict, Any, Optional
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import httpx
import yaml
import redis.asyncio as redis
import websockets
from sqlalchemy import text

from libs.common.signals import SignalsConfig, compute_signal
from notify import notify as _notify
from trailing import TrailingManager
from execution import BinanceExec, round_price_qty, generate_ladder_prices, equal_split
from risk_engine import size_notional_eur
from db import (
    get_engine, get_session, insert_event,
    insert_order_test, recent_orders,
    insert_fill, upsert_position, list_positions,
    settings_get, settings_set, ensure_settings_table,
    recent_fills, ensure_equity_table, insert_equity_point, get_equity_series, get_metrics_range,
)

async def emit_event(type_: str, level: str = "INFO", source: str = "api", payload: dict | None = None):
    payload = payload or {}
    try:
        async with get_session() as s:
            try:
                await insert_event(s, level, source, type_, payload)
                await s.commit()
            except Exception:
                pass
        try:
            await _notify(f"{type_}", payload)
        except Exception:
            pass
    except Exception as e:
        print("[events] emit error:", e)

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

# Simple ops metrics
METRICS = {
    'uds_reconnects': 0,
    'uds_connected': False,
    'redis_ping_ms': None,
    'db_ping_ms': None,
    'last_keepalive_ok': None,
}

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
    """
    Prix spot prioritaire depuis Redis (last_ticks), sinon fallback REST Binance,
    avec mini-cache Redis (5s) pour éviter le spam réseau au redémarrage.
    """
    # 1) Redis ticks (source temps réel)
    raw = await r.hget("last_ticks", symbol)
    if raw:
        try:
            return float(json.loads(raw)["close"])
        except Exception:
            pass  # on essaie le fallback

    # 2) Mini-cache local pour le fallback REST
    cache_key = f"px:{symbol}:cache"
    c = await r.get(cache_key)
    if c:
        try:
            return float(c)
        except Exception:
            pass

    # 3) REST public Binance
    try:
        async with httpx.AsyncClient(timeout=5) as cli:
            resp = await cli.get(f"{BINANCE_BASE}/api/v3/ticker/price", params={"symbol": symbol})
            resp.raise_for_status()
            price = float(resp.json()["price"])
        # cache 5s
        try:
            await r.setex(cache_key, 5, str(price))
        except Exception:
            pass
        return price
    except Exception:
        return None

async def get_book(symbol: str) -> dict | None:
    """
    Lit bid/ask/mid récents depuis Redis (hash 'last_book').
    Fallback: si absent, tente un prix 'mid' depuis get_mark().
    """
    raw = await r.hget("last_book", symbol)
    if raw:
        try:
            data = json.loads(raw)
            # attendu: {"bid":..., "ask":..., "mid":..., "ts": ms}
            if all(k in data for k in ("bid", "ask", "mid")):
                return data
        except Exception:
            pass
    px = await get_mark(symbol)
    if px is None:
        return None
    # synthèse basique si pas de book
    return {"bid": px * (1 - 0.0005), "ask": px * (1 + 0.0005), "mid": px, "ts": int(time.time()*1000)}

def bps(x: float) -> float:
    return float(x) / 10_000.0

def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))

async def compute_limit_within_band(symbol: str, side: str, band_bps: int) -> dict:
    """
    Calcule un prix LIMIT 'propre' autour du MID en plafonnant le slippage via une bande en bps.
    Règle:
      - BUY  : prix = min(ask, mid * (1 + band))
      - SELL : prix = max(bid, mid * (1 - band))
    Si ask > mid*(1+band) (ou bid < mid*(1-band)), on renvoie flagged=fallback_possible.
    """
    side = side.upper()
    book = await get_book(symbol)
    if not book:
        raise HTTPException(503, f"Order book indisponible pour {symbol}")

    bid, ask, mid = float(book["bid"]), float(book["ask"]), float(book["mid"])
    band = bps(band_bps)

    if side == "BUY":
        cap = mid * (1.0 + band)
        px = min(ask, cap)
        needs_fallback = ask > cap
    elif side == "SELL":
        floor = mid * (1.0 - band)
        px = max(bid, floor)
        needs_fallback = bid < floor
    else:
        raise HTTPException(400, "side doit être BUY ou SELL")

    return {
        "bid": bid, "ask": ask, "mid": mid,
        "band_bps": band_bps,
        "limit_px": px,
        "fallback_suggested": bool(needs_fallback),
    }

async def attach_after_fill(symbol: str, entry_px: float, qty: float,
                            tp_pct: float | None = None, sl_pct: float | None = None, trail_mult: float | None = None):
    """
    Attache un OCO/trailing après une entrée exécutée.
    - PAPER : on log un ORDER_TEST 'OCO_SIM'
    - LIVE  : on démarre trailing_mgr (best-effort)
    """
    tp_pct = float(tp_pct if tp_pct is not None else 0.016)
    sl_pct = float(sl_pct if sl_pct is not None else 0.008)
    trail_mult = float(trail_mult if trail_mult is not None else 1.2)

    is_paper = (EXEC_TEST or not LIVE_TRADING)

    if is_paper:
        try:
            async with get_session() as s:
                await insert_order_test(s, symbol, "SELL", 0.0, f"OCO_SIM: tp={tp_pct}, sl={sl_pct}, trail={trail_mult}")
                await s.commit()
            try:
                await _notify("📎 OCO (paper) attaché", {"symbol": symbol, "tp_pct": tp_pct, "sl_pct": sl_pct})
            except Exception:
                pass
        except Exception:
            pass
        return

    # LIVE
    try:
        if trailing_mgr:
            await trailing_mgr.start(symbol, float(entry_px), float(qty), tp_pct, sl_pct, 1.0, trail_mult)
            try:
                await _notify("📎 OCO (live) attaché", {"symbol": symbol, "tp_pct": tp_pct, "sl_pct": sl_pct})
            except Exception:
                pass
    except Exception as e:
        try:
            await _notify("⚠️ OCO attach error", {"symbol": symbol, "error": str(e)})
        except Exception:
            pass

async def portfolio_equity_eur() -> float:
    """Valeur des positions uniquement (utilitaire)."""
    total = 0.0
    async with get_session() as s:
        pos = await list_positions(s)
    for p in pos:
        mark = await get_mark(p["symbol"]) or p["avg_px"]
        total += float(p["qty"]) * float(mark)
    return max(total, 1.0)

async def portfolio_exposure_eur() -> float:
    """Exposition (longs) en € via positions."""
    exp = 0.0
    async with get_session() as s:
        pos = await list_positions(s)
    for p in pos:
        mark = await get_mark(p["symbol"]) or p["avg_px"]
        exp += max(0.0, float(p["qty"])) * float(mark)
    return exp

async def exposure_caps_ok(symbol: str, add_notional_eur: float, overrides: dict) -> tuple[bool, str]:
    """
    Validation des caps d'exposition :
    - Expo courante = somme des positions longues (en €)
    - Equity de référence = equity live (cash EUR + positions) si dispo, sinon initial_equity_eur
    """
    risk = (overrides.get("risk") or {})
    expo = (risk.get("exposure") or {})

    max_port_pct  = float(expo.get("max_portfolio_expo_pct", 30.0)) / 100.0
    min_cash_eur  = float(expo.get("min_cash_reserve_eur", 0.0))
    max_pos_total = int(expo.get("max_positions_total", 99))
    max_sym_notional = float(expo.get("max_per_symbol_notional_eur", 1e12))
    one_pos_per_sym  = bool(expo.get("one_position_per_symbol", True))

    # Positions actuelles
    async with get_session() as s:
        positions = await list_positions(s)
    pos_map = {p["symbol"]: p for p in positions}

    # Expo actuelle = somme des longues
    cur_expo = 0.0
    for p in positions:
        mark = await get_mark(p["symbol"]) or float(p["avg_px"])
        cur_expo += max(0.0, float(p["qty"])) * mark

    # Equity de référence = equity live (cash+positions) si possible, sinon initial_equity_eur
    eq_live = await get_equity_eur_cached()
    eq_base = float(APP_CONFIG.risk.get("initial_equity_eur", 140.0))
    eq_ref  = float(eq_live) if eq_live is not None else eq_base
    eq_ref  = max(eq_ref, 1.0)

    # Limites de comptage de positions
    if len(positions) >= max_pos_total and (symbol not in pos_map):
        return False, f"max positions reached ({len(positions)}/{max_pos_total})"

    if one_pos_per_sym and (symbol in pos_map) and float(pos_map[symbol]["qty"]) > 0:
        return False, f"already long {symbol} and one_position_per_symbol=true"

    # Cap notional par symbole
    if symbol in pos_map:
        mark = await get_mark(symbol) or float(pos_map[symbol]["avg_px"])
        cur_sym_notional = float(pos_map[symbol]["qty"]) * mark
    else:
        cur_sym_notional = 0.0
    if (cur_sym_notional + add_notional_eur) > max_sym_notional:
        return False, f"symbol notional cap ({cur_sym_notional+add_notional_eur:.2f}>{max_sym_notional:.2f}€)"

    # Caps globaux vs equity de référence
    next_expo = cur_expo + add_notional_eur
    if next_expo > max_port_pct * eq_ref:
        return False, f"portfolio expo cap ({next_expo:.2f}€ > {max_port_pct*100:.1f}% of {eq_ref:.2f}€)"
    if (eq_ref - next_expo) < min_cash_eur:
        return False, f"cash reserve cap ({eq_ref-next_expo:.2f}€ < {min_cash_eur:.2f}€)"

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
    """
    Equity live = cash EUR + valorisation des autres soldes convertis en EUR via last_ticks.
    Requiert des clés (exec_client.account()) ; sinon retourne None.
    """
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
        return await get_mark(sym)

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

# ---------- HISTORIQUE: helpers ----------
INTERVAL_MS = {"1m": 60_000, "1h": 3_600_000, "4h": 14_400_000}

def _ms(dt: datetime) -> int: return int(dt.timestamp() * 1000)
def _dt(ms: int) -> datetime: return datetime.fromtimestamp(ms/1000, tz=timezone.utc)

async def _ensure_candles_columns():
    # Assure la présence de la colonne "close" (ton schéma peut l'avoir manquée)
    async with get_session() as s:
        try:
            await s.execute(text("ALTER TABLE candles ADD COLUMN IF NOT EXISTS close NUMERIC"))
            await s.commit()
        except Exception:
            pass

async def _fetch_klines(symbol: str, interval: str, start_ms: int, end_ms: int):
    base = BINANCE_BASE
    async with httpx.AsyncClient(base_url=base, timeout=15) as cli:
        cur = start_ms
        while cur < end_ms:
            r = await cli.get("/api/v3/klines", params={
                "symbol": symbol, "interval": interval,
                "startTime": cur, "endTime": end_ms, "limit": 1000
            })
            r.raise_for_status()
            batch = r.json() or []
            if not batch:
                break
            for k in batch:
                yield k
            last_close = batch[-1][6]
            cur = max(cur + 1, last_close + 1)  # évite boucles infinies

async def _upsert_candles(rows: List[Tuple[str,str,datetime,float,float,float,float,float]]):
    if not rows: return
    async with get_session() as s:
        # upsert sur (symbol, tf, ts)
        values_sql, params = [], {}
        for i, (sym, tf, ts, o, h, l, c, v) in enumerate(rows):
            values_sql.append(f"(:s{i}, :tf{i}, :ts{i}, :o{i}, :h{i}, :l{i}, :c{i}, :v{i})")
            params.update({
                f"s{i}": sym, f"tf{i}": tf, f"ts{i}": ts,
                f"o{i}": o, f"h{i}": h, f"l{i}": l, f"c{i}": c, f"v{i}": v
            })
        sql = f"""
        INSERT INTO candles(symbol, tf, ts, open, high, low, close, volume)
        VALUES {", ".join(values_sql)}
        ON CONFLICT (symbol, tf, ts) DO UPDATE SET
          open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
          close=EXCLUDED.close, volume=EXCLUDED.volume
        """
        await s.execute(text(sql), params)
        await s.commit()

async def _hydrate_job(symbols: List[str], intervals: List[str], lookback_days: int, status: Dict[str, Any]):
    await _ensure_candles_columns()
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=lookback_days)
    start_ms, end_ms = _ms(start), _ms(now)

    total = 0
    for sym in symbols:
        status["current_symbol"] = sym
        for itv in intervals:
            if itv not in INTERVAL_MS:
                continue
            status["current_tf"] = itv
            rows, prev_open = [], None
            async for k in _fetch_klines(sym, itv, start_ms, end_ms):
                ot = int(k[0]); op, hi, lo, cl, vol = map(float, (k[1], k[2], k[3], k[4], k[5]))
                # on stocke ts = closeTime (k[6]) pour coller aux clôtures
                ts_close = _dt(int(k[6]))
                # log simple de gap si besoin
                if prev_open is not None and ot != prev_open + INTERVAL_MS[itv]:
                    status["last_gap"] = f"{sym} {itv} gap {_dt(prev_open)} -> {_dt(ot)}"
                prev_open = ot
                rows.append((sym, itv, ts_close, op, hi, lo, cl, vol))
                if len(rows) >= 600:
                    await _upsert_candles(rows); total += len(rows); rows.clear(); status["inserted"] = total
            if rows:
                await _upsert_candles(rows); total += len(rows); rows.clear(); status["inserted"] = total
    status["done"] = True
    status["inserted"] = total

# ---------------- Health & config ----------------
@app.get("/health")
async def health():
    return {"status": "ok", "env": APP_CONFIG.env, "mode": MODE, "live": LIVE_TRADING, "testOrders": EXEC_TEST}

@app.get("/health/extended")
async def health_extended():
    return {"status": "ok","env": APP_CONFIG.env,"mode": MODE,"live": LIVE_TRADING,"metrics": METRICS}

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

@app.get("/pnl/history")
async def pnl_history(days: int = 30):
    days = max(1, min(int(days), 365))
    since = datetime.now(timezone.utc) - timedelta(days=days)
    async with get_session() as s:
        data = await get_metrics_range(s, since)
    return {"from": since.date().isoformat(), "days": days, "history": data}

@app.post("/pnl/reset")
async def pnl_reset():
    await ensure_daily_baseline()
    base = await r.get("pnl:baseline_eur")
    return {"ok": True, "baseline_eur": float(base) if base else None, "day": await r.get("pnl:day")}

# ---------------- HISTORY ----------------
@app.get("/candles")
async def get_candles(
    symbol: str,
    tf: str,
    start: str = Query(..., description="ISO ex: 2025-07-01T00:00:00Z"),
    end: str   = Query(..., description="ISO ex: 2025-07-02T00:00:00Z"),
    limit: int = Query(5000, ge=1, le=100000),
):
    def _parse_iso(s: str):
        s = s.replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    t1, t2 = _parse_iso(start), _parse_iso(end)
    async with get_session() as s:
        q = text("""
            SELECT ts, open, high, low, close, volume
            FROM candles
            WHERE symbol=:sym AND tf=:tf AND ts >= :t1 AND ts <= :t2
            ORDER BY ts ASC
            LIMIT :lim
        """)
        res = await s.execute(q, {"sym": symbol, "tf": tf, "t1": t1, "t2": t2, "lim": limit})
        out = []
        for r in res.fetchall():
            m = dict(r._mapping)
            out.append({
                "ts": m["ts"].isoformat(),
                "open": float(m["open"]), "high": float(m["high"]),
                "low": float(m["low"]), "close": float(m["close"]),
                "volume": float(m["volume"]),
            })
    return {"symbol": symbol, "tf": tf, "rows": out}

@app.get("/symbols/eligible")
async def symbols_eligible():
    try:
        arr = await r.smembers("symbols:eligible:EUR")
        if arr:
            return {"eligible": sorted(arr), "source": "redis"}
    except Exception:
        pass
    return {"eligible": APP_CONFIG.symbols_seed, "source": "seed"}

class RefreshBody(BaseModel):
    min_eur_24h: float | None = None

@app.post("/symbols/eligible/refresh")
async def symbols_eligible_refresh(body: RefreshBody | None = None):
    thr = float(APP_CONFIG.volume24h_min_eur)
    if body and body.min_eur_24h is not None:
        thr = float(body.min_eur_24h)
    async with httpx.AsyncClient(base_url=BINANCE_BASE, timeout=20) as cli:
        tick = await cli.get("/api/v3/ticker/24hr")
        tick.raise_for_status()
        items = tick.json() or []
    allowed = []
    for t in items:
        sym = (t.get("symbol") or "")
        if not sym.endswith("EUR"):
            continue
        try:
            qvol = float(t.get("quoteVolume") or 0.0)
        except:
            qvol = 0.0
        if qvol >= thr:
            allowed.append(sym)
    if allowed:
        try:
            await r.delete("symbols:eligible:EUR")
            await r.sadd("symbols:eligible:EUR", *allowed)
            await r.set("symbols:eligible:EUR:last_update_ts", str(int(time.time())))
        except Exception:
            pass
    return {"eligible": sorted(allowed), "threshold_eur": thr}

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
    
class LimitBandPreview(BaseModel):
    symbol: str
    side: str           # BUY/SELL
    quote_eur: float    # notionnel cible
    band_bps: int | None = None   # optionnel, si None → config

@app.post("/trade/limit-band/preview")
async def trade_limit_band_preview(req: LimitBandPreview):
    symbol = req.symbol.upper()
    side   = req.side.upper()
    if side not in ("BUY","SELL"):
        raise HTTPException(400, "side doit être BUY ou SELL")

    band = int(req.band_bps) if req.band_bps is not None else int(APP_CONFIG.execution.get("price_band_bps", 30))
    info = exec_client.exchange_info(symbol) if exec_client else None

    calc = await compute_limit_within_band(symbol, side, band)
    px = float(calc["limit_px"])

    # qty brute depuis notionnel
    qty_raw = max(0.0, float(req.quote_eur)) / px
    # respect LOT_SIZE/PRICE_FILTER via helper existant
    if info:
        px, qty = round_price_qty(px, qty_raw, info)
    else:
        qty = qty_raw

    return {
        "symbol": symbol, "side": side,
        "book": {"bid": calc["bid"], "ask": calc["ask"], "mid": calc["mid"]},
        "band_bps": band,
        "limit_px": px,
        "qty": qty,
        "fallback_suggested": calc["fallback_suggested"],
    }

@app.get("/signals/preview")
async def signals_preview(symbol: str, lookback_1m: int = 200, lookback_4h: int = 300):
    """
    Calcule le score de signal pour `symbol`:
      - trend filtre 4h (EMA200)
      - breakout + spike volume + momentum flash sur 1m
      - stop_pct suggéré = k*ATR/price
    Lecture depuis table 'candles' (DB). Réutilise la config effective (app.yaml + overrides).
    """
    sym = symbol.upper()

    # 1) charge la config (avec défauts si non présents)
    cfg_dict = {}
    try:
        # on récupère overrides/effective si tu les exposes déjà dans APP_CONFIG
        cfg_dict = (APP_CONFIG.strategy or {}).get("signals", {}) if APP_CONFIG and hasattr(APP_CONFIG, "strategy") else {}
        # possibilité d'overrides côté runtime (ex: via /config/overrides)
        async with get_session() as s:
            ov = await settings_get(s, "overrides") or {}
            if "signals" in (ov.get("strategy") or {}):
                cfg_dict.update(ov["strategy"]["signals"])
    except Exception:
        pass
    cfg = SignalsConfig(**cfg_dict)

    # 2) lire les bougies depuis DB
    async def _fetch(tf: str, limit: int):
        q = text("""
            SELECT ts, open, high, low, close, volume
            FROM candles
            WHERE symbol = :sym AND tf = :tf
            ORDER BY ts DESC
            LIMIT :lim
        """)
        async with get_session() as s:
            res = await s.execute(q, {"sym": sym, "tf": tf, "lim": int(limit)})
            rows = [dict(r._mapping) for r in res.fetchall()]
        # on remet en ordre chronologique
        rows.reverse()
        # cast num -> float (au cas où)
        for r in rows:
            r["open"] = float(r["open"]); r["high"] = float(r["high"]); r["low"] = float(r["low"])
            r["close"] = float(r["close"]); r["volume"] = float(r["volume"])
        return rows

    c1m  = await _fetch("1m", min(max(lookback_1m, 60), 2000))
    c4h  = await _fetch("4h", min(max(lookback_4h, 210), 2000))

    if len(c1m) == 0 or len(c4h) == 0:
        raise HTTPException(404, "Pas assez de données candles en DB pour ce symbole.")

    # 3) calcul du signal
    sig = compute_signal(c1m, c4h, cfg)

    # 4) cooldown soft (via Redis)
    cooldown_ok = True
    cooldown_key = f"signals:cooldown:{sym}"
    try:
        cd = int(cfg["cooldown_sec"])
        if cd > 0 and await redis.get(cooldown_key):
            cooldown_ok = False
    except Exception:
        pass

    # 5) retourne un résumé compact
    last = c1m[-1]
    return {
        "symbol": sym,
        "last": {"ts": str(last["ts"]), "close": last["close"], "volume": last["volume"]},
        "trend_ok": sig["trend_ok"],
        "ema200_4h": sig["ema200_4h"],
        "atr_1m": sig["atr_1m"],
        "stop_pct": sig["stop_pct"],
        "components": sig["components"],
        "score": sig["score"],
        "cooldown_ok": cooldown_ok,
        "config": sig["config_used"],
    }

class LadderPreviewReq(BaseModel):
    symbol: str
    side: str               # BUY/SELL
    total_quote_eur: float  # budget total en €
    n_levels: int = 3
    step_bps: int = 20
    tif: Optional[str] = "GTC"
    limit_maker: bool = False

class LadderPlaceReq(LadderPreviewReq):
    attach_oco: bool = False
    tp_pct: Optional[float] = None
    sl_pct: Optional[float] = None
    trail_mult: Optional[float] = 1.2

class PlaceLimitBandReq(BaseModel):
    symbol: str
    side: str            # BUY/SELL
    quote_eur: float
    band_bps: int | None = None
    allow_market_fallback: bool = True
    score: float | None = None       # ← nouveau
    tif: str | None = "GTC"
    limit_maker: bool = False
    # OCO auto à l’entrée (facultatif)
    attach_oco: bool = False
    tp_pct: float | None = None      # ex: 0.016 (=+1.6%)
    sl_pct: float | None = None      # ex: 0.008 (=-0.8%)
    trail_mult: float | None = 1.2

@app.post("/trade/limit-band/place")
async def trade_limit_band_place(req: PlaceLimitBandReq):
    if exec_client is None:
        raise HTTPException(400, "exec_client not initialized (keys missing).")

    symbol = req.symbol.upper()
    side   = req.side.upper()
    if side not in ("BUY", "SELL"):
        raise HTTPException(400, "side doit être BUY ou SELL")

    # 1) Prix LIMIT within band + spread
    band = int(req.band_bps) if req.band_bps is not None else int(APP_CONFIG.execution.get("price_band_bps", 30))
    calc = await compute_limit_within_band(symbol, side, band)
    px   = float(calc["limit_px"])
    needs_fallback = bool(calc["fallback_suggested"])

    bid = float(calc["bid"]); ask = float(calc["ask"]); mid = float(calc["mid"])
    spread_bps = ((ask - bid) / max(mid, 1e-9)) * 10_000.0

    # 2) Garde-fous fallback MARKET: score fort ET spread contenu
    cfg_exec = APP_CONFIG.execution or {}
    thr_score = float(cfg_exec.get("min_score_for_market_fallback", 0.8))
    max_spread_bps = int(cfg_exec.get("max_spread_bps_for_market", 20))
    strong_enough = (req.score is not None and float(req.score) >= thr_score)
    spread_ok = (spread_bps <= max_spread_bps)
    allow_market_now = bool(req.allow_market_fallback and needs_fallback and strong_enough and spread_ok)

    # 3) Arrondis vs filtres
    info = exec_client.exchange_info(symbol)
    qty_raw = max(0.0, float(req.quote_eur)) / px
    px, qty = round_price_qty(px, qty_raw, info)
    if qty <= 0:
        raise HTTPException(400, "Quantité nulle après arrondi (minQty/stepSize trop contraignants).")

    # 4) Caps d’exposition (après arrondi, notional réel)
    notional_eur = qty * px
    async with get_session() as s:
        ov = await settings_get(s, "overrides") or {}
    ok_caps, reason = await exposure_caps_ok(symbol, notional_eur, ov)
    if not ok_caps:
        async with get_session() as s:
            await insert_order_test(s, symbol, side, notional_eur, f"BLOCK:{reason}")
            await s.commit()
        raise HTTPException(400, f"Exposition bloquée: {reason}")

    # 5) PAPER vs LIVE
    mode_paper = (EXEC_TEST or not LIVE_TRADING)

    # ---------- Fallback MARKET (conditionné) ----------
    if allow_market_now:
        if mode_paper:
            # test côté Binance (quote) + fill simulé
            try:
                exec_client.order_test_market_quote(symbol, side, float(req.quote_eur))
            except Exception as e:
                raise HTTPException(400, f"order_test MARKET a échoué: {e}")

            book = await get_book(symbol)
            if not book:
                raise HTTPException(503, "Order book indisponible pour simuler le fill MARKET.")
            fill_px = float(book["ask"] if side == "BUY" else book["bid"])
            raw_qty = max(0.0, float(req.quote_eur)) / max(1e-12, fill_px)
            _, mkt_qty = round_price_qty(fill_px, raw_qty, info)
            if mkt_qty <= 0:
                raise HTTPException(400, "Quantité MARKET nulle après arrondi.")

            async with get_session() as s:
                await insert_fill(s, None, fill_px, (+mkt_qty if side == "BUY" else -mkt_qty), 0.0)
                await upsert_position(s, symbol, (+mkt_qty if side == "BUY" else -mkt_qty), fill_px)
                await s.commit()
            try:
                await _notify("✅ Entrée PAPER (fallback MARKET)", {"symbol": symbol, "side": side, "qty": mkt_qty, "price": fill_px})
            except Exception:
                pass

            # OCO attaché (optionnel) — via helper unique
            if req.attach_oco and side == "BUY":
                await attach_after_fill(symbol, float(fill_px), float(mkt_qty),
                                        tp_pct=req.tp_pct, sl_pct=req.sl_pct, trail_mult=req.trail_mult)

            return {"ok": True, "mode": "paper", "type": "MARKET", "qty": mkt_qty, "price": fill_px, "fallback_applied": True}

        # LIVE: MARKET qty
        try:
            book = await get_book(symbol)
            if not book:
                raise HTTPException(503, "Order book indisponible pour placer MARKET live.")
            live_px_ref = float(book["ask"] if side == "BUY" else book["bid"])
            raw_qty = max(0.0, float(req.quote_eur)) / max(1e-12, live_px_ref)
            _, live_qty = round_price_qty(live_px_ref, raw_qty, info)
            if live_qty <= 0:
                raise HTTPException(400, "Quantité MARKET (live) nulle après arrondi.")
            res = exec_client.order_market_qty(symbol, side, live_qty)

            # OCO attaché live (best-effort) — via helper
            if req.attach_oco and side == "BUY":
                await attach_after_fill(symbol, float(live_px_ref), float(live_qty),
                                        tp_pct=req.tp_pct, sl_pct=req.sl_pct, trail_mult=req.trail_mult)

            return {"ok": True, "mode": "live", "type": "MARKET", "binance": res, "fallback_applied": True}
        except Exception as e:
            raise HTTPException(400, f"order MARKET (live) a échoué: {e}")

    # ---------- LIMIT within band ----------
    if mode_paper:
        book = await get_book(symbol)
        if not book:
            raise HTTPException(503, "Order book indisponible.")
        bid, ask = float(book["bid"]), float(book["ask"])
        marketable = (px >= ask) if side == "BUY" else (px <= bid)
        if marketable:
            fill_px = ask if side == "BUY" else bid
            async with get_session() as s:
                await insert_fill(s, None, fill_px, (+qty if side == "BUY" else -qty), 0.0)
                await upsert_position(s, symbol, (+qty if side == "BUY" else -qty), fill_px)
                await s.commit()
            try:
                await _notify("✅ Entrée PAPER (LIMIT exécuté)", {"symbol": symbol, "side": side, "qty": qty, "price": fill_px})
            except Exception:
                pass

            # OCO attaché (optionnel) — via helper
            if req.attach_oco and side == "BUY":
                await attach_after_fill(symbol, float(fill_px), float(qty),
                                        tp_pct=req.tp_pct, sl_pct=req.sl_pct, trail_mult=req.trail_mult)

            return {"ok": True, "mode": "paper", "type": "LIMIT", "qty": qty, "price": fill_px, "resting": False}
        else:
            return {"ok": True, "mode": "paper", "type": "LIMIT", "qty": qty, "price": px, "resting": True}

    # LIVE: LIMIT (post-only ou GTC/IOC/FOK)
    try:
        if req.limit_maker:
            res = exec_client.order_limit_px_qty(symbol, side, px, qty, "GTC", limit_maker=True)
        else:
            tif = (req.tif or "GTC").upper()
            res = exec_client.order_limit_px_qty(symbol, side, px, qty, tif)
        # L'attache OCO après un LIMIT live sera gérée via UDS (executionReport).
        return {"ok": True, "mode": "live", "type": "LIMIT", "price": px, "qty": qty, "binance": res}
    except Exception as e:
        raise HTTPException(400, f"order LIMIT a échoué: {e}")
    
@app.post("/orders/ladder/preview")
async def orders_ladder_preview(req: LadderPreviewReq):
    symbol = req.symbol.upper()
    side = req.side.upper()
    if side not in ("BUY", "SELL"):
        raise HTTPException(400, "side doit être BUY ou SELL")
    if req.n_levels < 1 or req.n_levels > 10:
        raise HTTPException(400, "n_levels doit être entre 1 et 10")

    # book / mid
    book = await get_book(symbol)
    if not book:
        raise HTTPException(503, "Order book indisponible.")
    bid, ask, mid = float(book["bid"]), float(book["ask"]), float(book["mid"])

    # grille de prix (helpers venus de execution.py)
    prices = generate_ladder_prices(mid, side, int(req.n_levels), int(req.step_bps))
    # split égal du notionnel total
    quotes = equal_split(float(req.total_quote_eur), len(prices))

    info = exec_client.exchange_info(symbol) if exec_client else None
    levels, total_notional = [], 0.0
    for p, q in zip(prices, quotes):
        raw_qty = q / max(1e-12, p)
        px, qty = (p, raw_qty)
        if info:
            px, qty = round_price_qty(p, raw_qty, info)
        notional = qty * px
        total_notional += notional
        marketable = (px >= ask) if side == "BUY" else (px <= bid)
        levels.append({
            "target_px": p,
            "limit_px": px,
            "qty": qty,
            "notional_eur": notional,
            "marketable_now": marketable,
        })

    async with get_session() as s:
        ov = await settings_get(s, "overrides") or {}
    ok_caps, reason = await exposure_caps_ok(symbol, total_notional, ov)

    return {
        "symbol": symbol, "side": side,
        "mid": mid, "bid": bid, "ask": ask,
        "step_bps": req.step_bps, "n_levels": req.n_levels,
        "total_quote_eur": req.total_quote_eur,
        "levels": levels,
        "caps": {"ok": ok_caps, "reason": (None if ok_caps else reason)},
        "tif": (req.tif or "GTC").upper(),
        "limit_maker": bool(req.limit_maker),
    }

@app.post("/orders/ladder/place")
async def orders_ladder_place(req: LadderPlaceReq):
    if exec_client is None:
        raise HTTPException(400, "exec_client not initialized (keys missing).")

    preview = await orders_ladder_preview(req)
    if not preview["caps"]["ok"]:
        raise HTTPException(400, f"Exposition bloquée: {preview['caps']['reason']}")

    symbol = preview["symbol"]; side = preview["side"]
    tif = (req.tif or "GTC").upper()
    limit_maker = bool(req.limit_maker)
    mode_paper = (EXEC_TEST or not LIVE_TRADING)

    placed = []
    book = await get_book(symbol)
    if not book:
        raise HTTPException(503, "Order book indisponible.")
    bid, ask = float(book["bid"]), float(book["ask"])

    for lvl in preview["levels"]:
        px = float(lvl["limit_px"]); qty = float(lvl["qty"])
        if qty <= 0:
            continue

        if mode_paper:
            marketable = (px >= ask) if side == "BUY" else (px <= bid)
            if marketable:
                fill_px = ask if side == "BUY" else bid
                async with get_session() as s:
                    await insert_fill(s, None, fill_px, (+qty if side == "BUY" else -qty), 0.0)
                    await upsert_position(s, symbol, (+qty if side == "BUY" else -qty), fill_px)
                    await s.commit()
                placed.append({"type": "LIMIT", "qty": qty, "price": fill_px, "resting": False})
            else:
                placed.append({"type": "LIMIT", "qty": qty, "price": px, "resting": True})
            continue

        # LIVE
        try:
            res = exec_client.order_limit_px_qty(symbol, side, px, qty, tif, limit_maker=limit_maker)
            placed.append({"type": "LIMIT", "qty": qty, "price": px, "binance": res})
        except Exception as e:
            placed.append({"type": "LIMIT", "qty": qty, "price": px, "error": str(e)})

    # OCO attaché (approx) sur la partie exécutée immédiatement
    if req.attach_oco and side == "BUY":
        qty_sum = sum(float(i["qty"]) for i in placed if i.get("resting") is False and "error" not in i)
        if qty_sum > 0:
            entry_px_ref = float(ask)  # approx live
            await attach_after_fill(
                symbol, entry_px_ref, qty_sum,
                tp_pct=req.tp_pct, sl_pct=req.sl_pct, trail_mult=req.trail_mult
            )

    return {"ok": True, "mode": ("paper" if mode_paper else "live"),
            "symbol": symbol, "side": side, "placed": placed, "tif": tif, "limit_maker": limit_maker}

@app.get("/debug/book/{symbol}")
async def debug_book(symbol: str):
    b = await get_book(symbol.upper())
    if not b:
        raise HTTPException(404, "book indisponible")
    return b

@app.get("/debug/oco_sim")
async def debug_oco_sim(limit: int = 20):
    """Liste les OCO simulés (PAPER) enregistrés via insert_order_test(..., status='OCO_SIM: ...')."""
    async with get_session() as s:
        await ensure_orders_table(s) if 'ensure_orders_table' in globals() else None  # no-op si tu n'as pas cette helper
        q = text("""
            SELECT id, client_id, symbol, side, type, qty, price, status, created_at
            FROM orders
            WHERE status LIKE 'OCO_SIM%%'
            ORDER BY id DESC
            LIMIT :lim
        """)
        res = await s.execute(q, {"lim": limit})
        rows = [dict(r._mapping) for r in res.fetchall()]
    return {"rows": rows}

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

    symbol = (symbol or "").upper()

    # --- charge position en base
    async with get_session() as s:
        pos_list = await list_positions(s)
    pos = next((p for p in pos_list if p["symbol"] == symbol), None)
    if not pos:
        raise HTTPException(404, f"Aucune position ouverte sur {symbol}")

    qty_pos = float(pos["qty"])
    if qty_pos <= 0:
        raise HTTPException(400, "Position non positive (rien à vendre).")

    # --- quantité à vendre (valeur brute)
    if body.qty and body.qty > 0:
        qty_sell = min(qty_pos, float(body.qty))
    else:
        pct = max(0.0, min(1.0, (body.percent or 100.0) / 100.0))
        qty_sell = qty_pos * pct

    if qty_sell <= 0:
        raise HTTPException(400, "Quantité demandée nulle.")

    # --- garde-fou: respecte stepSize/minQty (et string décimale, pas de 6E-05)
    try:
        qty_str_check = exec_client._format_qty(symbol, qty_sell)
    except Exception as e:
        raise HTTPException(400, f"Erreur format qty: {e}")

    if qty_str_check == "0":
        raise HTTPException(400, f"Quantité trop petite (en dessous de minQty/stepSize) pour {symbol}")

    # --- PAPER (test orders) ---
    if EXEC_TEST or not LIVE_TRADING:
        try:
            # test order côté Binance (vérifie les filtres)
            _ = exec_client.order_test_market_qty(symbol, "SELL", qty_sell)
        except Exception as e:
            # notif + erreur claire
            try:
                await _notify("❌ close PAPER error", {"symbol": symbol, "reason": str(e)})
            except Exception:
                pass
            raise HTTPException(400, f"order_test_market_qty a échoué: {e}")

        # besoin d'un prix pour simuler le fill
        px = await get_mark(symbol)
        if px is None:
            raise HTTPException(503, f"Prix indisponible pour {symbol} (ticks & fallback). Réessaye.")

        # persistance du fill simulé & MAJ position
        async with get_session() as s:
            await insert_fill(s, None, px, -float(qty_str_check), 0.0)
            await upsert_position(s, symbol, -float(qty_str_check), px)
            await s.commit()

        # notify
        try:
            await _notify("🔔 Sortie PAPER", {"symbol": symbol, "qty": qty_str_check, "price": px})
        except Exception:
            pass

        return {"ok": True, "mode": "paper", "qty": qty_str_check, "price": px}

    # --- LIVE ---
    try:
        res = exec_client.order_market_qty(symbol, "SELL", qty_sell)
    except Exception as e:
        try:
            await _notify("❌ close LIVE error", {"symbol": symbol, "reason": str(e)})
        except Exception:
            pass
        raise HTTPException(400, f"order_market_qty a échoué: {e}")

    try:
        await _notify("🔔 Sortie LIVE", {"symbol": symbol, "qty": qty_str_check})
    except Exception:
        pass

    return {"ok": True, "mode": "live", "binance": res, "qty": qty_str_check}

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

# --------------- HYDRATATION (endpoints) -------------
HYDRATE_TASK: asyncio.Task | None = None
HYDRATE_STATUS: Dict[str, Any] = {"running": False, "done": False, "inserted": 0, "current_symbol": None, "current_tf": None, "last_gap": None}

class HydrateReq(BaseModel):
    lookback_days: int = 30
    intervals: List[str] = ["1m", "1h", "4h"]
    symbols: Optional[List[str]] = None  # si None -> seed de la config

@app.post("/debug/hydrate/start")
async def debug_hydrate_start(req: HydrateReq):
    global HYDRATE_TASK, HYDRATE_STATUS
    if HYDRATE_TASK and not HYDRATE_TASK.done():
        return {"status": "already_running", **HYDRATE_STATUS}

    # charge symboles (seed) si non fournis
    symbols = (req.symbols if (req.symbols and len(req.symbols) > 0) else (APP_CONFIG.symbols_seed or []))
    intervals = [tf for tf in req.intervals if tf in ("1m","1h","4h")]

    HYDRATE_STATUS = {"running": True, "done": False, "inserted": 0, "current_symbol": None, "current_tf": None, "last_gap": None}
    # lance en tâche de fond (non bloquant)
    async def runner():
        try:
            # init engine si besoin (évite un get_session() non initialisé)
            get_engine(DB_URL)
            await _hydrate_job(symbols, intervals, int(req.lookback_days), HYDRATE_STATUS)
        except Exception as e:
            HYDRATE_STATUS["error"] = str(e)
        finally:
            HYDRATE_STATUS["running"] = False

    HYDRATE_TASK = asyncio.create_task(runner())
    return {"status": "started", "symbols": symbols, "intervals": intervals, "lookback_days": req.lookback_days}

@app.get("/debug/hydrate/status")
async def debug_hydrate_status():
    # retourne l'état courant de la tâche
    return HYDRATE_STATUS

# --------------- WHITELIST (EUR 24h) -------------
@app.post("/debug/whitelist/refresh")
async def debug_whitelist_refresh():
    thr = float(APP_CONFIG.volume24h_min_eur)
    try:
        async with httpx.AsyncClient(base_url=BINANCE_BASE, timeout=20) as cli:
            tick = await cli.get("/api/v3/ticker/24hr")
            tick.raise_for_status()
            items = tick.json() or []
        allowed = []
        for t in items:
            sym = (t.get("symbol") or "")
            if sym.endswith("EUR"):
                try:
                    qvol = float(t.get("quoteVolume") or 0.0)
                except:
                    qvol = 0.0
                if qvol >= thr:
                    allowed.append(sym)
        if allowed:
            try:
                await r.delete("symbols:eligible:EUR")
                await r.sadd("symbols:eligible:EUR", *allowed)
                await r.set("symbols:eligible:EUR:last_update_ts", str(int(time.time())))
            except Exception:
                pass
        return {"ok": True, "eligible": sorted(allowed), "threshold_eur": thr}
    except Exception as e:
        return {"ok": False, "error": str(e)}

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

@app.get("/debug/price/{symbol}")
async def debug_price(symbol: str):
    sym = (symbol or "").upper()
    px = await get_mark(sym)
    return {"symbol": sym, "price": px}

# ---- exposure status (pour UI Risk) ----
@app.get("/exposure/status")
async def exposure_status():
    async with get_session() as s:
        ov = await settings_get(s, "overrides") or {}
        positions = await list_positions(s)
    # equity de ref pour UI = live si dispo sinon initial_equity
    eq_live = await get_equity_eur_cached()
    eq_ref  = float(eq_live) if eq_live is not None else float(APP_CONFIG.risk.get("initial_equity_eur", 140.0))
    expo = await portfolio_exposure_eur()
    cap_cfg = (ov.get("risk", {}).get("exposure") or {})
    return {
        "equity_eur": eq_ref,
        "exposure_eur": expo,
        "free_cash_eur": max(0.0, eq_ref - expo),
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

            # ---- CAPS d'exposition (avec equity live si dispo) ----
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
                    try:
                        await _notify("❌ order.test ERROR", {
                            "symbol": symbol,
                            "reason": str(e)
                        })
                    except Exception:
                        pass
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


# ---------------- Ops watchdog ----------------
async def ops_watchdog_job():
    failures = {"redis": 0, "db": 0}
    while True:
        # Redis ping
        t0 = time.perf_counter()
        try:
            pong = await r.ping()
            METRICS["redis_ping_ms"] = round((time.perf_counter() - t0) * 1000.0, 1)
            failures["redis"] = 0
        except Exception as e:
            failures["redis"] += 1
            try:
                await emit_event("REDIS_UNAVAILABLE", "ERROR", "api", {"msg": str(e), "count": failures["redis"]})
            except Exception:
                pass
        # DB ping
        t1 = time.perf_counter()
        try:
            async with get_session() as s:
                await s.execute(text("SELECT 1"))
            METRICS["db_ping_ms"] = round((time.perf_counter() - t1) * 1000.0, 1)
            failures["db"] = 0
        except Exception as e:
            failures["db"] += 1
            try:
                await emit_event("DB_UNAVAILABLE", "ERROR", "api", {"msg": str(e), "count": failures["db"]})
            except Exception:
                pass
        await asyncio.sleep(30)

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

    lk = {'value': new_key()}
    if not lk['value']:
        print("[uds] no listenKey -> abort"); return

    async def keepalive():
        while True:
            await asyncio.sleep(30 * 60)
            try:
                exec_client.client.renew_listen_key(lk['value'])
                METRICS['last_keepalive_ok'] = datetime.now(timezone.utc).isoformat()
            except Exception as e:
                print("[uds] keepalive failed, recreating:", e)
                nk = new_key()
                if nk:
                    lk['value'] = nk

    async def handle_exec_report(ev: Dict[str, Any]):
        """
        Binance USER DATA STREAM: executionReport
        On attache un OCO/trailing dès qu'un BUY est exécuté (FILLED ou PARTIALLY_FILLED).
        """
        try:
            if ev.get("e") != "executionReport":
                return
            if ev.get("x") != "TRADE":  # exécution partielle/complète
                return

            symbol = ev.get("s")
            side   = ev.get("S", "BUY")
            status = ev.get("X", "")  # NEW / PARTIALLY_FILLED / FILLED / ...
            fill_qty = float(ev.get("l") or 0.0)   # last executed qty
            fill_px  = float(ev.get("L") or 0.0)   # last executed price

            if not symbol or fill_qty <= 0 or fill_px <= 0:
                return

            # BUY spot → attacher OCO/trailing (live ou paper selon flags globaux)
            if side == "BUY" and status in ("FILLED", "PARTIALLY_FILLED"):
                await attach_after_fill(
                    symbol, float(fill_px), float(fill_qty),
                    tp_pct=None, sl_pct=None, trail_mult=None  # défauts globaux
                )
        except Exception as e:
            try:
                await _notify("⚠️ OCO auto (UDS)", {"error": str(e)})
            except Exception:
                pass

    async def consume():
        url = f"{USER_WS_BASE}/{lk['value']}"
        while True:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    print("[uds] connected", url)
                    METRICS["uds_connected"] = True
                    async for msg in ws:
                        try:
                            data = json.loads(msg)
                            await handle_exec_report(data)
                        except Exception:
                            pass
            except Exception as e:
                METRICS["uds_connected"] = False
                METRICS["uds_reconnects"] = METRICS.get("uds_reconnects", 0) + 1
                print("[uds] ws error:", e)
                await asyncio.sleep(3)

    # démarrage keepalive + consumer
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
    asyncio.create_task(ops_watchdog_job())

    asyncio.create_task(consume_signals_loop())
    asyncio.create_task(user_stream_task())
    asyncio.create_task(daily_baseline_job())

    global trailing_mgr
    trailing_mgr = TrailingManager(r)
    asyncio.create_task(trailing_mgr.loop())
