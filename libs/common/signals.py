from __future__ import annotations
from typing import List, Dict, Any, Optional
from math import fabs

# --- petites utils ---

def ema(values: List[float], period: int) -> List[Optional[float]]:
    if period <= 1 or len(values) == 0:
        return [None] * len(values)
    k = 2.0 / (period + 1.0)
    out: List[Optional[float]] = [None] * len(values)
    # seed: SMA
    if len(values) >= period:
        sma = sum(values[:period]) / period
        out[period-1] = sma
        prev = sma
        for i in range(period, len(values)):
            prev = values[i] * k + prev * (1.0 - k)
            out[i] = prev
    return out

def atr(high: List[float], low: List[float], close: List[float], period: int) -> List[Optional[float]]:
    n = len(close)
    if n == 0 or period < 1:
        return [None] * n
    trs: List[float] = []
    for i in range(n):
        if i == 0:
            trs.append(high[i] - low[i])
        else:
            tr = max(high[i] - low[i], fabs(high[i] - close[i-1]), fabs(low[i] - close[i-1]))
            trs.append(tr)
    out = [None] * n
    if n >= period:
        # Wilder-style smoothing: seed = SMA(TR, period), puis EMA-like
        seed = sum(trs[:period]) / period
        out[period-1] = seed
        alpha = 1.0 / period
        prev = seed
        for i in range(period, n):
            prev = (prev * (period - 1) + trs[i]) / period
            out[i] = prev
    return out

def rolling_max(values: List[float], lookback: int) -> List[Optional[float]]:
    out: List[Optional[float]] = [None]*len(values)
    if lookback <= 0: return out
    from collections import deque
    dq = deque()
    for i, v in enumerate(values):
        # pop old
        while dq and dq[0][0] <= i - lookback:
            dq.popleft()
        # push current (maintain decreasing by value)
        while dq and dq[-1][1] <= v:
            dq.pop()
        dq.append((i, v))
        if i >= lookback-1:
            out[i] = dq[0][1]
    return out

# --- signaux / scoring ---

class SignalsConfig(dict):
    """
    Config simple avec défauts raisonnables (modifiable via /config/overrides dans app.py).
    """
    DEFAULTS = {
        "trend_tf": "4h",
        "trend_ema_period": 200,      # EMA200 en 4h
        "atr_tf": "1m",
        "atr_period": 14,             # ATR14 en 1m
        "breakout_lookback": 20,      # plus haut N-1 sur 1m
        "spike_vol_lookback": 20,     # moyenne vol N
        "spike_vol_threshold": 1.8,   # ratio volume dernier / moy > seuil
        "momentum_k_candles": 1,      # delta close vs close[-k]
        "stop_atr_mult": 2.0,         # stop = k * ATR / price
        "w_breakout": 0.4, "w_spike": 0.3, "w_momo": 0.3,
        "cooldown_sec": 60            # cooldown “soft” informatif
    }

    def __init__(self, **kwargs):
        d = dict(self.DEFAULTS)
        d.update(kwargs or {})
        super().__init__(d)

def compute_trend_ok_4h(closes_4h: List[float], ema_period: int = 200) -> Dict[str, Any]:
    ema_vals = ema(closes_4h, ema_period)
    trend_ok = False
    ema_last = None
    if len(closes_4h) > 0 and ema_vals[-1] is not None:
        ema_last = float(ema_vals[-1])
        trend_ok = closes_4h[-1] >= ema_last
    return {"trend_ok": trend_ok, "ema_last": ema_last}

def compute_components_1m(high: List[float], low: List[float], close: List[float],
                          volume: List[float], cfg: SignalsConfig) -> Dict[str, Any]:
    n = len(close)
    if n < max(30, cfg["atr_period"]+2, cfg["breakout_lookback"]+2, cfg["spike_vol_lookback"]+2):
        return {"atr": None, "breakout": 0.0, "spike": 0.0, "momo": 0.0, "stop_pct": None, "details": {}}

    atr_vals = atr(high, low, close, cfg["atr_period"])
    atr_last = atr_vals[-1] if atr_vals[-1] is not None else None
    price_last = close[-1]

    # breakout: dernier close vs plus haut des (lookback) précédents (exclure le bar courant)
    lb = int(cfg["breakout_lookback"])
    highs_prev = rolling_max(high[:-1], lb)
    br = 0.0
    br_base = highs_prev[-1] if highs_prev[-1] is not None else None
    if br_base is not None and atr_last and atr_last > 0:
        # intensité: (close - prev_high)/ATR, clamp 0..1 sur une échelle 0..0.75 ATR
        x = max(0.0, (price_last - br_base) / atr_last)
        br = min(1.0, x / 0.75)

    # spike volume: ratio vs moyenne (N)
    sv_lb = int(cfg["spike_vol_lookback"])
    avg_vol = sum(volume[-sv_lb-1:-1]) / sv_lb
    vol_ratio = (volume[-1] / avg_vol) if avg_vol > 0 else 0.0
    sp = 0.0
    thr = float(cfg["spike_vol_threshold"])
    if vol_ratio > 1.0:
        # 1.0 -> 0 ; thr -> 1, au-delà clamp 1
        sp = max(0.0, min(1.0, (vol_ratio - 1.0) / max(1e-9, (thr - 1.0))))

    # momentum flash: |close - close[-k]| / ATR
    k = max(1, int(cfg["momentum_k_candles"]))
    if atr_last and atr_last > 0 and n > k:
        delta = fabs(price_last - close[-1 - k])
        mom_ratio = delta / atr_last
        mm = min(1.0, mom_ratio / 0.75)  # 0.75 ATR -> score 1
    else:
        mm = 0.0

    # stop suggéré: k_atr / prix
    stop_pct = (cfg["stop_atr_mult"] * atr_last / price_last) if (atr_last and price_last > 0) else None

    return {
        "atr": atr_last,
        "breakout": float(br),
        "spike": float(sp),
        "momo": float(mm),
        "stop_pct": stop_pct,
        "details": {
            "vol_ratio": vol_ratio,
            "prev_high": br_base,
        }
    }

def score_from_components(trend_ok: bool, comp: Dict[str, Any], cfg: SignalsConfig) -> float:
    if not trend_ok:
        return 0.0
    w1, w2, w3 = float(cfg["w_breakout"]), float(cfg["w_spike"]), float(cfg["w_momo"])
    s = w1*comp["breakout"] + w2*comp["spike"] + w3*comp["momo"]
    return max(0.0, min(1.0, s))

def compute_signal(
    candles_1m: List[Dict[str, float]],
    candles_4h: List[Dict[str, float]],
    cfg: Optional[SignalsConfig] = None
) -> Dict[str, Any]:
    cfg = cfg or SignalsConfig()
    close_4h = [c["close"] for c in candles_4h]
    trend = compute_trend_ok_4h(close_4h, cfg["trend_ema_period"])

    high_1m = [c["high"] for c in candles_1m]
    low_1m  = [c["low"]  for c in candles_1m]
    close_1m= [c["close"]for c in candles_1m]
    vol_1m  = [c["volume"] for c in candles_1m]

    comp = compute_components_1m(high_1m, low_1m, close_1m, vol_1m, cfg)
    score = score_from_components(trend["trend_ok"], comp, cfg)

    return {
        "trend_ok": trend["trend_ok"],
        "ema200_4h": trend["ema_last"],
        "atr_1m": comp["atr"],
        "stop_pct": comp["stop_pct"],
        "components": {
            "breakout": comp["breakout"],
            "spike": comp["spike"],
            "momentum": comp["momo"],
            "vol_ratio": comp["details"].get("vol_ratio"),
            "prev_high": comp["details"].get("prev_high"),
        },
        "score": score,
        "config_used": dict(cfg),
    }
