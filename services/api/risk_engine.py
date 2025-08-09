# services/api/risk_engine.py
from __future__ import annotations
from typing import Dict

def _get_min_notional(symbol_info: Dict) -> float:
    for f in symbol_info.get("filters", []):
        ft = f.get("filterType")
        if ft in ("NOTIONAL", "MIN_NOTIONAL"):
            try:
                return float(f.get("minNotional", 0))
            except Exception:
                pass
    return 0.0

def f_score(score: float) -> float:
    # courbe simple: 0.2x → 3x
    score = max(0.0, min(1.0, score))
    return 0.2 + 2.8 * score

def g_drawdown(drawdown_pct: float) -> float:
    # placeholder: 1.0 (réduction quand on suivra l'équity réel)
    return 1.0

def size_notional_eur(
    equity_eur: float,
    base_risk_pct: float,
    hard_cap_pct: float,
    score: float,
    stop_pct: float,
    symbol_info: Dict
) -> dict:
    """
    Retourne un notional EUR à envoyer (quoteOrderQty).
    On vise le plus petit des budgets: (par stop) vs (hard cap).
    S'assure d'être >= minNotional, sinon "bump" au minimum.
    """
    base_risk = max(0.0, base_risk_pct) / 100.0
    cap = max(0.0, hard_cap_pct) / 100.0

    # budget risque € (continu)
    risk_eur = equity_eur * base_risk * f_score(score) * g_drawdown(0.0)

    # sizing par stop (approx)
    stop_pct = max(stop_pct, 0.003)  # plancher 0.3% pour éviter stops trop serrés
    notional_by_stop = risk_eur / stop_pct

    # hard cap € absolu
    cap_eur = equity_eur * cap

    # notional initial
    notional = max(0.0, min(notional_by_stop, cap_eur))

    # respect du minNotional Binance
    mn = _get_min_notional(symbol_info)
    if notional < mn:
        notional = mn  # on rehausse au min, quitte à consommer le cap

    return {
        "notional_eur": float(notional),
        "min_notional": float(mn),
        "cap_eur": float(cap_eur),
        "risk_eur": float(risk_eur),
    }
