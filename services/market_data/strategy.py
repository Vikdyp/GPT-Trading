# services/market_data/strategy.py
from __future__ import annotations
from typing import List, Dict, Optional
import math
import time

def ema(values: List[float], period: int) -> List[float]:
    if not values or period <= 1:
        return values[:]
    k = 2 / (period + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(out[-1] + k * (v - out[-1]))
    return out

def rsi(closes: List[float], period: int = 14) -> List[float]:
    if len(closes) < period + 1:
        return [50.0] * len(closes)
    gains, losses = [], []
    for i in range(1, period + 1):
        chg = closes[i] - closes[i - 1]
        gains.append(max(chg, 0.0))
        losses.append(max(-chg, 0.0))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    rsis = [None] * (period)  # align
    for i in range(period + 1, len(closes)):
        chg = closes[i] - closes[i - 1]
        gain = max(chg, 0.0)
        loss = max(-chg, 0.0)
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
        rs = (avg_gain / avg_loss) if avg_loss > 1e-12 else 999999.0
        r = 100 - (100 / (1 + rs))
        rsis.append(r)
    # pad first values
    while len(rsis) < len(closes):
        rsis.insert(0, 50.0)
    return rsis

def atr(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> List[float]:
    if len(closes) < period + 1:
        return [0.0] * len(closes)
    trs = [0.0]
    for i in range(1, len(closes)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )
        trs.append(tr)
    out = []
    # Wilder smoothing
    atr_prev = sum(trs[1:period + 1]) / period
    out = [0.0] * (period) + [atr_prev]
    for i in range(period + 1, len(trs)):
        atr_prev = (atr_prev * (period - 1) + trs[i]) / period
        out.append(atr_prev)
    while len(out) < len(closes):
        out.insert(0, 0.0)
    return out

def compute_signal(symbol: str,
                   klines_1m: List[List],
                   klines_4h: List[List],
                   params: Dict) -> Optional[Dict]:
    """
    klines arrays are Binance format:
    [ openTime, open, high, low, close, volume, closeTime, ... ]
    """
    if len(klines_1m) < 50 or len(klines_4h) < 50:
        return None

    # parse 1m
    closes_1m = [float(k[4]) for k in klines_1m]
    highs_1m  = [float(k[2]) for k in klines_1m]
    lows_1m   = [float(k[3]) for k in klines_1m]
    ts_last   = int(klines_1m[-1][6])  # closeTime

    # parse 4h
    closes_4h = [float(k[4]) for k in klines_4h]

    # params (defaults)
    ema_fast_p = params.get("ema_fast", 9)
    ema_slow_p = params.get("ema_slow", 21)
    rsi_p      = params.get("rsi_len", 14)
    rsi_buy_max  = params.get("rsi_buy_max", 70)   # avoid overbought
    atr_p      = params.get("atr_len", 14)
    atr_mult   = params.get("atr_mult", 2.0)
    ema200_p   = params.get("trend_ema_4h", 200)

    # indicators
    ema_fast = ema(closes_1m, ema_fast_p)
    ema_slow = ema(closes_1m, ema_slow_p)
    rsi_1m   = rsi(closes_1m, rsi_p)
    atr_1m   = atr(highs_1m, lows_1m, closes_1m, atr_p)

    ema200_4h = ema(closes_4h, ema200_p)
    trend_ok = len(ema200_4h) >= 2 and (closes_4h[-1] > ema200_4h[-1] and ema200_4h[-1] > ema200_4h[-2])

    price = closes_1m[-1]
    stop_pct = max(0.003, (atr_1m[-1] * atr_mult) / price)  # plancher 0.3%

    # cross up fast/slow as entry
    cross_up = (ema_fast[-2] <= ema_slow[-2]) and (ema_fast[-1] > ema_slow[-1])
    # strength based on ema distance vs ATR
    ema_diff = max(0.0, ema_fast[-1] - ema_slow[-1])
    strength_raw = ema_diff / max(1e-9, (atr_1m[-1] * 0.8))
    score = max(0.0, min(1.0, strength_raw))  # 0..1
    # penalize if RSI too high
    if rsi_1m[-1] > rsi_buy_max:
        score *= 0.5

    if trend_ok and cross_up and score > 0.15:
        return {
            "symbol": symbol,
            "side": "BUY",
            "score": round(float(score), 4),
            "price": round(float(price), 8),
            "stop_pct": round(float(stop_pct), 6),
            "ts": ts_last
        }

    return None
