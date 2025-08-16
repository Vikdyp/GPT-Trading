# services/api/backtest.py
from __future__ import annotations
import math, statistics
from typing import List, Dict, Any, Optional, Tuple

# Interface attendue par l’API :
# run_backtest(kl_1m, kl_4h, params, fee_bps, slip_bps)
# - kl_1m: [{"ts":ms,"high":..,"low":..,"close":..,"volume":..}, ...]
# - kl_4h: [{"ts":ms,"close":..}, ...]

# ------------------ indicateurs ------------------

def _ema(values: List[float], period: int) -> List[Optional[float]]:
    if period <= 1:
        return [float(v) for v in values]
    out: List[Optional[float]] = []
    alpha = 2.0 / (period + 1.0)
    ema = None
    for i, v in enumerate(values):
        x = float(v)
        if ema is None:
            if i + 1 >= period:
                ema = sum(float(values[j]) for j in range(i - period + 1, i + 1)) / period
            else:
                ema = x
        else:
            ema = alpha * x + (1 - alpha) * ema
        out.append(float(ema) if i + 1 >= period else None)
    return out

def _atr_wilder(kl_1m: List[Dict[str, float]], period: int) -> List[Optional[float]]:
    """ATR Wilder sur 1m (high/low/close). Retourne une liste alignée sur kl_1m."""
    n = len(kl_1m)
    if n == 0:
        return []
    trs = [0.0] * n
    prev_close = float(kl_1m[0]["close"])
    for i in range(n):
        h = float(kl_1m[i]["high"])
        l = float(kl_1m[i]["low"])
        c_prev = prev_close
        tr = max(h - l, abs(h - c_prev), abs(l - c_prev))
        trs[i] = tr
        prev_close = float(kl_1m[i]["close"])
    out: List[Optional[float]] = [None] * n
    if n < period:
        return out
    atr = sum(trs[0:period]) / float(period)  # amorce
    out[period - 1] = atr
    for i in range(period, n):
        atr = (atr * (period - 1) + trs[i]) / float(period)
        out[i] = atr
    return out

def _align_trend_gate(kl_1m: List[Dict[str, float]],
                      kl_4h: List[Dict[str, float]],
                      ema_len: int = 200) -> List[bool]:
    """Pour chaque minute : trend_ok = close_4h > EMA(4h)."""
    if not kl_4h:
        return [False] * len(kl_1m)
    c4 = [float(x["close"]) for x in kl_4h]
    ema200 = _ema(c4, ema_len)
    res: List[bool] = []
    j = 0
    for m in kl_1m:
        ts = int(m["ts"])
        while j + 1 < len(kl_4h) and int(kl_4h[j + 1]["ts"]) <= ts:
            j += 1
        close_4h = float(kl_4h[j]["close"])
        e = ema200[j]
        res.append(bool((e is not None) and (close_4h > float(e))))
    return res

def _clamp01(x: float) -> float:
    return 0.0 if x < 0 else 1.0 if x > 1 else x

def _score_components(
    kl_1m: List[Dict[str, float]],
    atr: List[Optional[float]],
    breakout_lookback: int,
    spike_vol_lookback: int,
    spike_vol_threshold: float,
    momentum_k: int,
    w_breakout: float,
    w_spike: float,
    w_momo: float,
) -> List[float]:
    n = len(kl_1m)
    scores = [0.0] * n
    from collections import deque
    vol_q = deque(maxlen=max(1, spike_vol_lookback))
    hi_q  = deque(maxlen=max(2, breakout_lookback))
    for i in range(n):
        c = float(kl_1m[i]["close"])
        h = float(kl_1m[i]["high"])
        v = float(kl_1m[i]["volume"])
        vol_q.append(v)
        hi_q.append(h)

        # breakout (close > max des highs précédents)
        if len(hi_q) >= breakout_lookback:
            prev_max = max(list(hi_q)[:-1])  # exclure bougie courante
            breakout = 1.0 if (c > prev_max) else 0.0
        else:
            breakout = 0.0

        # spike volume (ratio / moyenne)
        if len(vol_q) >= spike_vol_lookback:
            mv = sum(vol_q) / len(vol_q)
            spike_ratio = (v / mv) if mv > 0 else 0.0
            spike = 0.0 if spike_ratio < spike_vol_threshold else _clamp01((spike_ratio - spike_vol_threshold) / max(1.0, spike_vol_threshold))
        else:
            spike = 0.0

        # momentum k bougies, normalisé ATR
        if i - momentum_k >= 0:
            delta = c - float(kl_1m[i - momentum_k]["close"])
            atr_i = atr[i] or 0.0
            denom = max(1e-12, atr_i)
            momo = _clamp01(delta / denom)
            if delta <= 0:
                momo = 0.0
        else:
            momo = 0.0

        w_sum = max(1e-9, (w_breakout + w_spike + w_momo))
        scores[i] = _clamp01((w_breakout * breakout + w_spike * spike + w_momo * momo) / w_sum)
    return scores

# ------------------ métriques ------------------

def _sharpe_daily(equity_series: List[Tuple[int, float]]) -> float:
    """Sharpe sur retours journaliers (UTC)."""
    if len(equity_series) < 2:
        return 0.0
    import datetime
    by_day: Dict[str, float] = {}
    for ts, eq in equity_series:
        day = datetime.datetime.utcfromtimestamp(ts/1000).strftime("%Y-%m-%d")
        by_day[day] = eq  # dernière valeur de la journée
    days = sorted(by_day.keys())
    if len(days) < 3:
        return 0.0
    rets: List[float] = []
    prev = by_day[days[0]]
    for d in days[1:]:
        cur = by_day[d]
        r = (cur / prev - 1.0) if prev != 0 else 0.0
        rets.append(r)
        prev = cur
    if len(rets) < 2:
        return 0.0
    m = statistics.fmean(rets)
    s = statistics.stdev(rets) if len(rets) > 1 else 0.0
    if s == 0:
        return 0.0
    return float(m / s * math.sqrt(252.0))

def _cagr(equity_series: List[Tuple[int, float]]) -> float:
    if len(equity_series) < 2:
        return 0.0
    ts0, eq0 = equity_series[0]
    ts1, eq1 = equity_series[-1]
    dur_days = max(1e-9, (ts1 - ts0) / (1000.0 * 60 * 60 * 24))
    if eq0 <= 0:
        return 0.0
    return float((eq1 / eq0) ** (365.0 / dur_days) - 1.0)

# ------------------ moteur de backtest ------------------

def run_backtest(
    kl_1m: List[Dict[str, float]],
    kl_4h: List[Dict[str, float]],
    params: Dict[str, Any] | None = None,
    fee_bps: float = 2.0,
    slip_bps: float = 1.0,
) -> Dict[str, Any]:
    """
    Simule une logique LONG-only événementielle (trend 4h + impulsion 1m),
    avec deux modes d’exécution : 'market' ou 'limit_band'.
    """
    params = params or {}
    if len(kl_1m) < 200 or len(kl_4h) < 60:
        return {"error": "not enough data"}

    # --- hyperparams ---
    trend_ema_4h      = int(params.get("trend_ema_4h", 200))
    atr_len           = int(params.get("atr_len", 14))
    atr_mult          = float(params.get("atr_mult", 2.0))
    breakout_lookback = int(params.get("breakout_lookback", 20))
    spike_vol_lookback= int(params.get("spike_vol_lookback", 20))
    spike_vol_threshold = float(params.get("spike_vol_threshold", 2.0))
    momentum_k        = int(params.get("momentum_k_candles", 2))
    w_breakout        = float(params.get("w_breakout", 0.5))
    w_spike           = float(params.get("w_spike", 0.25))
    w_momo            = float(params.get("w_momo", 0.25))
    entry_threshold   = float(params.get("entry_threshold", 0.70))
    cooldown_min      = int(params.get("cooldown_min", 8))
    alloc_frac        = float(params.get("alloc_frac", 1.0))
    tp_r_multiple     = float(params.get("tp_r_multiple", 2.0))

    exec_mode         = str(params.get("exec_mode", "market")).lower()          # "market" | "limit_band"
    band_bps          = int(params.get("band_bps", 10))                         # pour "limit_band"
    spread_bps        = float(params.get("spread_bps", max(1.0, slip_bps)))     # approx ask/bid autour du mid

    fee_rate  = float(fee_bps) / 10_000.0
    slip_rate = float(slip_bps) / 10_000.0
    band_rate = float(band_bps) / 10_000.0
    spr_rate  = float(spread_bps) / 10_000.0

    # --- pré-calculs ---
    atr = _atr_wilder(kl_1m, atr_len)
    trend_ok = _align_trend_gate(kl_1m, kl_4h, trend_ema_4h)
    scores = _score_components(
        kl_1m, atr, breakout_lookback, spike_vol_lookback, spike_vol_threshold,
        momentum_k, w_breakout, w_spike, w_momo
    )

    equity_series: List[Tuple[int, float]] = []
    equity = 1.0

    open_trade: Optional[Dict[str, Any]] = None  # journal tampon d’un trade ouvert
    last_entry_i = -10_000

    # pour limit-band : ordre en attente (timeout 5 barres)
    resting: Optional[Dict[str, Any]] = None  # {"limit_px":..., "since":i}

    trades: List[Dict[str, Any]] = []

    for i in range(len(kl_1m)):
        ts = int(kl_1m[i]["ts"])
        c  = float(kl_1m[i]["close"])
        h  = float(kl_1m[i]["high"])
        l  = float(kl_1m[i]["low"])

        # book synthétique
        mid = c
        ask = c * (1.0 + spr_rate)
        bid = c * (1.0 - spr_rate)

        # point d’equity à chaque barre
        equity_series.append((ts, float(equity)))

        # ----- gestion de la sortie si en position -----
        if open_trade is not None:
            entry_px = float(open_trade["entry_px"])
            stop_pct = float(open_trade["stop_pct"])
            tp_mult  = float(open_trade["tp_mult"])

            sl_px = entry_px * (1.0 - stop_pct)
            tp_px = entry_px * (1.0 + tp_mult * stop_pct)

            exit_px = None
            exit_reason = None
            if h >= tp_px:
                exit_px = tp_px * (1.0 - slip_rate)  # vente → -slip
                exit_reason = "TP"
            elif l <= sl_px:
                exit_px = sl_px * (1.0 - slip_rate)
                exit_reason = "SL"

            if exit_px is not None:
                # PnL net (frais aller+retour comptés ici UNE SEULE fois)
                pnl_pct_gross = (exit_px / entry_px - 1.0)
                pnl_pct_net = pnl_pct_gross - (2.0 * fee_rate)

                # MAJ equity
                equity *= (1.0 + alloc_frac * pnl_pct_net)

                # log du trade
                R = pnl_pct_gross / max(1e-12, stop_pct)
                trade = {
                    "entry_ts": int(open_trade["entry_ts"]),
                    "exit_ts": ts,
                    "side": "LONG",
                    "entry_px": entry_px,
                    "exit_px": exit_px,
                    "stop_pct": stop_pct,
                    "tp_mult": tp_mult,
                    "pnl_pct": pnl_pct_net,
                    "R": R,
                    "reason": exit_reason,
                }
                trades.append(trade)
                open_trade = None
                resting = None
                last_entry_i = i
                continue  # bougie suivante

        # si ordre resting non exécuté (mode limit-band), essaye de le remplir
        if (open_trade is None) and (resting is not None) and exec_mode == "limit_band":
            limit_px = float(resting["limit_px"])
            since_i  = int(resting["since"])
            if l <= limit_px or (i - since_i) >= 5:
                # entrée au pire bid actuel (sécurité), + slippage
                px_in = max(limit_px, bid) * (1.0 + slip_rate)
                atr_i = atr[i] or 0.0
                stop_pct = max(1e-6, atr_mult * atr_i / max(1e-12, px_in))
                open_trade = {
                    "entry_ts": ts,
                    "entry_px": px_in,
                    "stop_pct": stop_pct,
                    "tp_mult": tp_r_multiple,
                    "reason": "LIMIT_FILL" if l <= limit_px else "LIMIT_TIMEOUT",
                }
                resting = None
                last_entry_i = i
                continue
            # sinon : on reste en attente, pas de nouvelle entrée
            continue

        # Cooldown avant nouvelle entrée
        if open_trade is not None or (i - last_entry_i) < max(0, cooldown_min):
            continue

        # Conditions d’entrée
        if not trend_ok[i]:
            continue
        if atr[i] is None or atr[i] <= 0:
            continue
        sc = float(scores[i])
        if sc < entry_threshold:
            continue

        # ----- entrée -----
        atr_i = atr[i] or 0.0
        if exec_mode == "market":
            px_in = ask * (1.0 + slip_rate)
            stop_pct = max(1e-6, atr_mult * atr_i / max(1e-12, px_in))
            open_trade = {
                "entry_ts": ts,
                "entry_px": px_in,
                "stop_pct": stop_pct,
                "tp_mult": tp_r_multiple,
                "reason": "MARKET",
            }
            last_entry_i = i
        else:
            # limit-band : prix limité = min(ask, mid*(1+band))
            cap = mid * (1.0 + band_rate)
            limit_px = min(ask, cap)
            marketable = (limit_px >= ask)
            if marketable:
                px_in = ask * (1.0 + slip_rate)
                stop_pct = max(1e-6, atr_mult * atr_i / max(1e-12, px_in))
                open_trade = {
                    "entry_ts": ts,
                    "entry_px": px_in,
                    "stop_pct": stop_pct,
                    "tp_mult": tp_r_multiple,
                    "reason": "LIMIT_MARKETABLE",
                }
                last_entry_i = i
            else:
                # poser l’ordre et attendre (timeout 5 barres)
                resting = {"limit_px": float(limit_px), "since": i}

    # clôture forcée si position encore ouverte à la fin
    if open_trade is not None:
        last_ts = int(kl_1m[-1]["ts"])
        last_mid = float(kl_1m[-1]["close"])
        exit_px = last_mid * (1.0 - slip_rate)
        entry_px = float(open_trade["entry_px"])
        stop_pct = float(open_trade["stop_pct"])
        pnl_pct_gross = (exit_px / entry_px - 1.0)
        pnl_pct_net = pnl_pct_gross - (2.0 * fee_rate)
        equity *= (1.0 + alloc_frac * pnl_pct_net)
        R = pnl_pct_gross / max(1e-12, stop_pct)
        trades.append({
            "entry_ts": int(open_trade["entry_ts"]),
            "exit_ts": last_ts,
            "side": "LONG",
            "entry_px": entry_px,
            "exit_px": exit_px,
            "stop_pct": stop_pct,
            "tp_mult": float(open_trade["tp_mult"]),
            "pnl_pct": pnl_pct_net,
            "R": R,
            "reason": "FORCED_CLOSE",
        })
        open_trade = None

    # ------------------ KPIs ------------------
    prof = [t["pnl_pct"] for t in trades if t.get("pnl_pct", 0.0) > 0]
    loss = [t["pnl_pct"] for t in trades if t.get("pnl_pct", 0.0) < 0]
    if loss and sum(loss) != 0:
        pf = (sum(prof) / abs(sum(loss))) if prof else 0.0
    else:
        pf = float("inf") if prof else 0.0
    wins = sum(1 for t in trades if t.get("pnl_pct", 0.0) > 0)
    ntr  = len([t for t in trades if "pnl_pct" in t])
    winrate = (wins / ntr * 100.0) if ntr > 0 else 0.0
    avg_R = statistics.fmean([t["R"] for t in trades if "R" in t]) if trades else 0.0

    # Max drawdown sur la courbe equity
    peak = -1e9
    maxdd = 0.0
    for _, e in equity_series:
        if e > peak:
            peak = e
        dd = e / peak - 1.0
        if dd < maxdd:
            maxdd = dd

    stats = {
        "CAGR": _cagr(equity_series),
        "Sharpe": _sharpe_daily(equity_series),
        "MaxDD": float(maxdd),
        "n_trades": int(ntr),
        "winrate": float(winrate),
        "profit_factor": float(pf),
        "avg_R": float(avg_R),
        "bars": len(kl_1m),
        "exec_mode": exec_mode,
    }

    return {
        "stats": stats,
        "equity": equity_series,   # [(ts_ms, equity_norm)]
        "trades": trades,          # liste de trades complétés
    }
