# services/market_data/strategy.py
from __future__ import annotations
from typing import List, Dict, Optional
from libs.common.signals import SignalsConfig, compute_signal as _compute_signal_lib

def _adapt_klines_1m(klines_1m: List[List]) -> List[Dict[str, float]]:
    """
    Binance kline: [ openTime, open, high, low, close, volume, closeTime, ... ]
    -> [{high, low, close, volume}, ...]
    """
    return [
        {
            "high": float(k[2]),
            "low": float(k[3]),
            "close": float(k[4]),
            "volume": float(k[5]),
        } for k in klines_1m
    ]

def _adapt_klines_4h(klines_4h: List[List]) -> List[Dict[str, float]]:
    return [{"close": float(k[4])} for k in klines_4h]

def compute_signal(symbol: str,
                   klines_1m: List[List],
                   klines_4h: List[List],
                   params: Dict) -> Optional[Dict]:
    """
    Retourne:
      { "symbol","side","score","price","stop_pct","ts" }  ou  None
    """
    if len(klines_1m) < 50 or len(klines_4h) < 50:
        return None

    cfg = SignalsConfig(
        trend_tf = params.get("trend_tf", "4h"),
        trend_ema_period = params.get("trend_ema_4h", 200),
        atr_period = params.get("atr_len", 14),
        stop_atr_mult = params.get("atr_mult", 2.0),
        w_breakout = params.get("w_breakout", 0.4),
        w_spike = params.get("w_spike", 0.3),
        w_momo = params.get("w_momo", 0.3),
        # optionnels si tu les exposes dans app.yaml :
        breakout_lookback = params.get("breakout_lookback", 20),
        spike_vol_lookback = params.get("spike_vol_lookback", 20),
        spike_vol_threshold = params.get("spike_vol_threshold", 1.8),
        momentum_k_candles = params.get("momentum_k_candles", 1),
    )

    s = _compute_signal_lib(
        candles_1m=_adapt_klines_1m(klines_1m),
        candles_4h=_adapt_klines_4h(klines_4h),
        cfg=cfg
    )

    # petit seuil pour éviter les micro-scores
    if not s.get("trend_ok", False) or float(s.get("score", 0.0)) <= 0.15:
        return None

    price = float(klines_1m[-1][4])
    ts_last = int(klines_1m[-1][6])
    stop_pct = float(s["stop_pct"]) if s["stop_pct"] is not None else 0.008  # fallback 0.8%

    return {
        "symbol": symbol,
        "side": "BUY",
        "score": round(float(s["score"]), 4),
        "price": round(price, 8),
        "stop_pct": round(stop_pct, 6),
        "ts": ts_last
    }
