from typing import Dict, Any
import math

def _flt(filters, ftype):
    for f in filters:
        if f.get("filterType") == ftype:
            return f
    return None

def apply_rounding(price: float, qty: float, symbol_info: Dict[str, Any]):
    """Round price/qty to Binance tickSize/stepSize."""
    pf = _flt(symbol_info["filters"], "PRICE_FILTER")
    lot = _flt(symbol_info["filters"], "LOT_SIZE") or _flt(symbol_info["filters"], "MARKET_LOT_SIZE")
    if pf:
        tick = float(pf.get("tickSize", "0.00000001"))
        price = math.floor(price / tick) * tick
    if lot:
        step = float(lot.get("stepSize", "0.00000001"))
        qty = math.floor(qty / step) * step
    return price, qty

def enforce_min_notional(price: float, qty: float, symbol_info: Dict[str, Any]):
    """Ensure notional passes NOTIONAL/MIN_NOTIONAL."""
    notional_f = _flt(symbol_info["filters"], "NOTIONAL")
    min_notional = None
    if notional_f:
        min_notional = float(notional_f.get("minNotional", 0))
    else:
        mn = _flt(symbol_info["filters"], "MIN_NOTIONAL")
        if mn:
            min_notional = float(mn.get("minNotional", 0))
    if min_notional is None:
        return price, qty, True
    notional = price * qty
    if notional + 1e-12 >= min_notional:
        return price, qty, True
    # try raising qty up to min notional (caller ensures caps elsewhere)
    needed_qty = (min_notional / price) if price > 0 else qty
    return price, needed_qty, False

def guard_order(price: float, qty: float, symbol_info: Dict[str, Any]):
    """Round, enforce min notional, and re-round. Returns (price, qty)."""
    price, qty = apply_rounding(price, qty, symbol_info)
    price, qty2, ok = enforce_min_notional(price, qty, symbol_info)
    if not ok:
        price, qty2 = apply_rounding(price, qty2, symbol_info)
    return price, qty2
