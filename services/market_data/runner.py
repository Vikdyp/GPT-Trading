import os
import json
import asyncio
import time
from datetime import datetime, timezone
from collections import deque
from typing import Dict, Any, List, Optional

import websockets
import httpx
import yaml
import redis.asyncio as redis

# ---------- ENV ----------
MODE = os.getenv("BINANCE_MODE", "mainnet").lower()
STREAM_BASE = "wss://stream.binance.com:9443/stream" if MODE == "mainnet" else "wss://testnet.binance.vision/stream"
API_BASE   = os.getenv("API_BASE", "http://api:8000")
REDIS_URL  = os.getenv("REDIS_URL", "redis://redis:6379/0")
CONFIG_PATH = os.getenv("CONFIG_PATH", "/app/config/app.yaml")

# ---------- STATE ----------
r: redis.Redis = None
BASE_CFG: Dict[str, Any] = {}
EFFECTIVE_CFG: Dict[str, Any] = {}
LAST_RELOAD_ISO: Optional[str] = None

# ---------- HELPERS ----------
def now_ms() -> int: return int(time.time() * 1000)
def iso_utc() -> str: return datetime.now(timezone.utc).isoformat()

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

def ema_step(prev: Optional[float], price: float, period: int) -> float:
    if period <= 1: return float(price)
    k = 2.0 / (period + 1.0)
    return price if prev is None else prev + k * (price - prev)

def rsi_calc(values: List[float], length: int) -> Optional[float]:
    if len(values) < length + 1: return None
    gains = losses = 0.0
    for i in range(-length, 0):
        diff = values[i] - values[i - 1]
        gains += max(diff, 0.0)
        losses += max(-diff, 0.0)
    if losses == 0: return 100.0
    rs = (gains / length) / (losses / length)
    return 100.0 - 100.0 / (1.0 + rs)

# ---------- per-symbol state ----------
class SymState:
    def __init__(self, tick_maxlen: int, main_len_needed: int, trend_len_needed: int):
        # ticks (miniticker closes) pour réactivité EMA/RSI intrabar
        self.ticks = deque(maxlen=tick_maxlen)
        self.ema_fast = None
        self.ema_slow = None
        self.rsi = None
        self.last_cross = None
        self.last_signal_ts = 0.0

        # KLINE TF principal (ATR)
        self.main_closes = deque(maxlen=main_len_needed)
        self.main_highs  = deque(maxlen=main_len_needed)
        self.main_lows   = deque(maxlen=main_len_needed)
        self.atr: Optional[float] = None  # Wilder

        # KLINE HTF trend (EMA)
        self.trend_closes = deque(maxlen=trend_len_needed)
        self.trend_ema: Optional[float] = None

STATES: Dict[str, SymState] = {}

# ---------- CONFIG ----------
async def load_base_config():
    global BASE_CFG
    with open(CONFIG_PATH, "r") as f:
        BASE_CFG = yaml.safe_load(f) or {}

async def fetch_effective_config() -> Dict[str, Any]:
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(f"{API_BASE}/config/effective")
            r.raise_for_status()
            return r.json().get("config", {})
    except Exception:
        return BASE_CFG

def apply_config(cfg: Dict[str, Any]):
    global EFFECTIVE_CFG, LAST_RELOAD_ISO, STATES
    EFFECTIVE_CFG = cfg or {}

    symbols = EFFECTIVE_CFG.get("symbols_seed") or []
    strat   = EFFECTIVE_CFG.get("strategy") or {}
    ema_f   = int(strat.get("ema_fast", 9))
    ema_s   = int(strat.get("ema_slow", 21))
    rsi_len = int(strat.get("rsi_len", 14))

    atr_len = int(strat.get("atr_len", 14))
    trend_tf = str(strat.get("trend_tf", "4h"))
    ema_trend_len = int(strat.get("ema_trend_len", 50))

    # tailles des buffers
    tick_maxlen = max(ema_s * 6, rsi_len * 6, 600)
    main_need = max(atr_len + 2, 120)     # un peu de marge
    trend_need = max(ema_trend_len + 2, 120)

    # init/resize states
    for s in symbols:
        st = STATES.get(s)
        if not st:
            STATES[s] = SymState(tick_maxlen, main_need, trend_need)
        else:
            # ticks
            if st.ticks.maxlen != tick_maxlen:
                st.ticks = deque(list(st.ticks), maxlen=tick_maxlen)
            # main
            if st.main_closes.maxlen != main_need:
                st.main_closes = deque(list(st.main_closes), maxlen=main_need)
                st.main_highs  = deque(list(st.main_highs),  maxlen=main_need)
                st.main_lows   = deque(list(st.main_lows),   maxlen=main_need)
                st.atr = None
            # trend
            if st.trend_closes.maxlen != trend_need:
                st.trend_closes = deque(list(st.trend_closes), maxlen=trend_need)
                st.trend_ema = None

    for s in list(STATES.keys()):
        if s not in symbols: del STATES[s]

    LAST_RELOAD_ISO = iso_utc()

# ---------- ATR ----------
def update_atr_wilder(st: SymState, atr_len: int):
    n = len(st.main_closes)
    if n < atr_len + 1:
        return
    # calc TR de la dernière bougie clôturée
    c1 = st.main_closes[-1]
    h1 = st.main_highs[-1]
    l1 = st.main_lows[-1]
    c0 = st.main_closes[-2]
    tr = max(h1 - l1, abs(h1 - c0), abs(l1 - c0))

    if st.atr is None:
        # seed = moyenne simple des TR initiaux
        if n >= atr_len + 1:
            trs = []
            for i in range(-atr_len, 0):
                hi = st.main_highs[i]
                lo = st.main_lows[i]
                cp = st.main_closes[i-1]
                trs.append(max(hi - lo, abs(hi - cp), abs(lo - cp)))
            st.atr = sum(trs) / atr_len
    else:
        st.atr = (st.atr * (atr_len - 1) + tr) / atr_len

# ---------- SIGNAL LOGIC ----------
async def maybe_emit_signal(symbol: str, last_price: float):
    st = STATES.get(symbol)
    if not st: return

    strat = EFFECTIVE_CFG.get("strategy") or {}
    ema_f = int(strat.get("ema_fast", 9))
    ema_s = int(strat.get("ema_slow", 21))
    rsi_len = int(strat.get("rsi_len", 14))
    rsi_buy_max = float(strat.get("rsi_buy_max", 70))
    cooldown_s = int(strat.get("cooldown_s", 45))

    atr_len = int(strat.get("atr_len", 14))
    atr_mult_stop = float(strat.get("atr_mult_stop", 1.8))
    atr_min_stop_pct = float(strat.get("atr_min_stop_pct", 0.003))  # 0.3% plancher
    rr = float(strat.get("reward_risk", 2.0))  # TP = rr * stop

    # Trend filter (HTF)
    ema_trend_len = int(strat.get("ema_trend_len", 50))
    long_ok = (st.trend_ema is not None and
               len(st.trend_closes) >= ema_trend_len and
               st.trend_ema is not None and
               st.trend_ema <= st.trend_closes[-1])  # prix > ema HTF

    # EMA tick streaming
    st.ema_fast = ema_step(st.ema_fast, last_price, ema_f)
    st.ema_slow = ema_step(st.ema_slow, last_price, ema_s)
    st.rsi = rsi_calc(list(st.ticks), rsi_len)
    if st.ema_fast is None or st.ema_slow is None or st.rsi is None:
        return

    cross = "bull" if st.ema_fast > st.ema_slow else "bear"
    ts_now = time.time()

    if (
        cross == "bull" and st.last_cross == "bear"  # croisement haussier
        and st.rsi <= rsi_buy_max
        and long_ok
        and (ts_now - st.last_signal_ts) >= cooldown_s
        and st.atr is not None
    ):
        # stop en % via ATR
        stop_pct = max(atr_mult_stop * (st.atr / max(1e-9, last_price)), atr_min_stop_pct)
        # score ∈ [1,3] selon l’écart relatif fast/slow (comme avant)
        gap = (st.ema_fast - st.ema_slow) / max(1e-9, st.ema_slow)
        score = max(1.0, min(3.0, 1.0 + gap * 100.0))
        payload = {
            "symbol": symbol,
            "side": "BUY",
            "score": round(float(score), 3),
            "stop_pct": float(stop_pct),
            "tp_pct": float(stop_pct * rr),
            "reason": "ema_cross_bull+rsi_ok+trend_htf",
            "ts": now_ms(),
            "price": last_price,
        }
        await r.publish("signals", json.dumps(payload))
        st.last_signal_ts = ts_now

    st.last_cross = cross

# ---------- LOOPS ----------
async def ticks_loop():
    """miniTicker: réactivité intra-bar pour EMA/RSI et publier ticks."""
    while True:
        symbols = EFFECTIVE_CFG.get("symbols_seed") or []
        if not symbols:
            await asyncio.sleep(1.0); continue
        streams = "/".join([f"{s.lower()}@miniticker" for s in symbols])
        url = f"{STREAM_BASE}?streams={streams}"
        print(f"[md] ticks ws → {url} ({len(symbols)} syms)")
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                while True:
                    data = json.loads(await ws.recv())
                    d = data.get("data") or {}
                    sym = d.get("s"); c = d.get("c")
                    if not sym or not c: continue
                    try: px = float(c)
                    except: continue

                    tick = {"symbol": sym, "close": px, "ts": now_ms()}
                    await r.hset("last_ticks", sym, json.dumps(tick))
                    await r.publish("ticks", json.dumps(tick))

                    st = STATES.get(sym)
                    if st:
                        st.ticks.append(px)
                        await maybe_emit_signal(sym, px)
                    await asyncio.sleep(0.02)
        except Exception as e:
            print("[md] ticks error:", e)
            await asyncio.sleep(2.0)

async def kline_loop_main():
    """Klines TF principal — alimente ATR (clôtures uniquement)."""
    while True:
        symbols = EFFECTIVE_CFG.get("symbols_seed") or []
        tf = (EFFECTIVE_CFG.get("strategy") or {}).get("main_tf", "1m")
        if not symbols:
            await asyncio.sleep(1.0); continue
        streams = "/".join([f"{s.lower()}@kline_{tf}" for s in symbols])
        url = f"{STREAM_BASE}?streams={streams}"
        print(f"[md] kline main ws → {url}")
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                while True:
                    data = json.loads(await ws.recv())
                    k = (data.get("data") or {}).get("k") or {}
                    sym = (data.get("data") or {}).get("s")
                    if not sym or not k: continue
                    # only on candle close
                    if not k.get("x", False): continue
                    try:
                        c = float(k.get("c")); h = float(k.get("h")); l = float(k.get("l"))
                    except: continue
                    st = STATES.get(sym)
                    if not st: continue
                    st.main_closes.append(c)
                    st.main_highs.append(h)
                    st.main_lows.append(l)
                    atr_len = int((EFFECTIVE_CFG.get("strategy") or {}).get("atr_len", 14))
                    update_atr_wilder(st, atr_len)
        except Exception as e:
            print("[md] kline main error:", e)
            await asyncio.sleep(2.0)

async def kline_loop_trend():
    """Klines TF tendance (HTF) — alimente EMA tendance (clôtures uniquement)."""
    while True:
        symbols = EFFECTIVE_CFG.get("symbols_seed") or []
        strat = EFFECTIVE_CFG.get("strategy") or {}
        tf = str(strat.get("trend_tf", "4h"))
        ema_len = int(strat.get("ema_trend_len", 50))
        if not symbols:
            await asyncio.sleep(1.0); continue
        streams = "/".join([f"{s.lower()}@kline_{tf}" for s in symbols])
        url = f"{STREAM_BASE}?streams={streams}"
        print(f"[md] kline trend ws → {url}")
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                while True:
                    data = json.loads(await ws.recv())
                    k = (data.get("data") or {}).get("k") or {}
                    sym = (data.get("data") or {}).get("s")
                    if not sym or not k: continue
                    if not k.get("x", False): continue
                    try:
                        c = float(k.get("c"))
                    except: continue
                    st = STATES.get(sym)
                    if not st: continue
                    st.trend_closes.append(c)
                    st.trend_ema = ema_step(st.trend_ema, c, ema_len)
        except Exception as e:
            print("[md] kline trend error:", e)
            await asyncio.sleep(2.0)

# ---------- CONFIG WATCHER ----------
async def config_watcher():
    global LAST_RELOAD_ISO, EFFECTIVE_CFG
    pub = r.pubsub(); await pub.subscribe("config:updates")
    print("[md] config watcher subscribed → config:updates")
    while True:
        try:
            msg = await pub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if not msg:
                await asyncio.sleep(0.2); continue
            if msg["type"] != "message": continue
            try: ov = json.loads(msg["data"])
            except: ov = {}
            new_eff = deep_merge(EFFECTIVE_CFG, ov or {})
            apply_config(new_eff)
            LAST_RELOAD_ISO = iso_utc()
            await r.set("config:last_reload", LAST_RELOAD_ISO)
            print(f"[md] config reloaded @ {LAST_RELOAD_ISO}")
        except Exception as e:
            print("[md] watcher error:", e)
            await asyncio.sleep(1.0)

async def periodic_sync_effective():
    while True:
        await asyncio.sleep(600)
        try:
            eff = await fetch_effective_config()
            if json.dumps(eff, sort_keys=True) != json.dumps(EFFECTIVE_CFG, sort_keys=True):
                apply_config(eff)
                await r.set("config:last_reload", LAST_RELOAD_ISO or iso_utc())
                print("[md] periodic effective config refreshed")
        except Exception as e:
            print("[md] periodic sync error:", e)

# ---------- MAIN ----------
async def main():
    global r
    r = redis.from_url(REDIS_URL, decode_responses=True)
    await load_base_config()
    eff = await fetch_effective_config()
    apply_config(deep_merge(BASE_CFG, eff))
    await r.set("config:last_reload", LAST_RELOAD_ISO or iso_utc())

    tasks = [
        asyncio.create_task(ticks_loop()),
        asyncio.create_task(kline_loop_main()),
        asyncio.create_task(kline_loop_trend()),
        asyncio.create_task(config_watcher()),
        asyncio.create_task(periodic_sync_effective()),
    ]
    print("[md] runner started; ATR+Trend enabled")
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
