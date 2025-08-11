from __future__ import annotations
import time
import math
from typing import Dict, Any, List, Literal
from decimal import Decimal, getcontext
from binance.spot import Spot

# précision suffisante pour éviter les artefacts binaires
getcontext().prec = 40


def build_spot_client(mode: str, key: str | None, secret: str | None) -> Spot:
    base_url = "https://api.binance.com" if mode == "mainnet" else "https://testnet.binance.vision"
    key = key or ""
    secret = secret or ""
    return Spot(api_key=key, api_secret=secret, base_url=base_url)


def _flt(filters, ftype):
    for f in filters:
        if f.get("filterType") == ftype:
            return f
    return None


def round_price_qty(price: float, qty: float, symbol_info: Dict[str, Any]):
    pf = _flt(symbol_info["filters"], "PRICE_FILTER")
    lot = _flt(symbol_info["filters"], "LOT_SIZE") or _flt(symbol_info["filters"], "MARKET_LOT_SIZE")
    if pf:
        tick = float(pf.get("tickSize", "0.00000001"))
        price = math.floor(price / tick) * tick
    if lot:
        step = float(lot.get("stepSize", "0.00000001"))
        qty = math.floor(qty / step) * step
    return price, qty


class BinanceExec:
    def __init__(self, mode: str, key: str | None, secret: str | None):
        self.mode = mode
        self.client = build_spot_client(mode, key, secret)
        self._exinfo_cache: dict[str, dict] = {}
        self._time_offset_ms: int = 0
        self.sync_time()

    def sync_time(self):
        try:
            st = self.client.time()
            server_ms = int(st["serverTime"])
            local_ms = int(time.time() * 1000)
            self._time_offset_ms = server_ms - local_ms
        except Exception:
            self._time_offset_ms = 0

    def _ts(self) -> int:
        return int(time.time() * 1000) + self._time_offset_ms

    def exchange_info(self, symbol: str) -> dict:
        if symbol in self._exinfo_cache:
            return self._exinfo_cache[symbol]
        data = self.client.exchange_info(symbol=symbol)
        info = data["symbols"][0]
        self._exinfo_cache[symbol] = info
        return info

    # ---------- qty formatter (anti notation scientifique, respecte stepSize/minQty)
    def _format_qty(self, symbol: str, qty_base: float) -> str:
        """
        Retourne une quantité formatée en décimal fixe (pas d'exponent) et alignée sur stepSize.
        Si la qty (après floor) < minQty -> "0" (à traiter côté appelant).
        """
        # qty <= 0 -> direct
        q = Decimal(str(qty_base))
        if q <= 0:
            return "0"

        info = self.exchange_info(symbol)
        lot = _flt(info["filters"], "LOT_SIZE") or _flt(info["filters"], "MARKET_LOT_SIZE")

        if not lot:
            # fallback très rare: pas de filtre -> renvoyer la qty au format fixe
            s = format(q.normalize(), "f")
            return s if s else "0"

        step = Decimal(lot.get("stepSize", "0.00000001") or "0.00000001")
        min_qty = Decimal(lot.get("minQty", "0") or "0")

        if step <= 0:
            step = Decimal("0.00000001")

        # floor à stepSize (sans flottants)
        q = (q // step) * step

        if q < min_qty:
            return "0"

        # string fixe, sans expo, sans trailing zeros inutiles
        s = format(q.normalize(), "f")
        return s if s else "0"

    # -------- ORDERS (TEST) --------
    def order_test_market_quote(self, symbol: str, side: str, quote_eur: float) -> dict:
        q = round(float(quote_eur), 2)
        try:
            return self.client.new_order_test(
                symbol=symbol, side=side, type="MARKET",
                quoteOrderQty=str(q),
                recvWindow=10000, timestamp=self._ts(),
            )
        except Exception:
            self.sync_time()
            return self.client.new_order_test(
                symbol=symbol, side=side, type="MARKET",
                quoteOrderQty=str(q),
                recvWindow=10000, timestamp=self._ts(),
            )

    def order_test_market_qty(self, symbol: str, side: str, qty_base: float) -> dict:
        qty_str = self._format_qty(symbol, qty_base)
        if qty_str == "0":
            raise ValueError(f"Quantity below minQty/stepSize for {symbol}")
        try:
            return self.client.new_order_test(
                symbol=symbol, side=side, type="MARKET",
                quantity=qty_str,
                recvWindow=10000, timestamp=self._ts(),
            )
        except Exception:
            self.sync_time()
            return self.client.new_order_test(
                symbol=symbol, side=side, type="MARKET",
                quantity=qty_str,
                recvWindow=10000, timestamp=self._ts(),
            )

    # -------- ORDERS (REAL) --------
    def order_market_quote(self, symbol: str, side: str, quote_eur: float) -> dict:
        q = round(float(quote_eur), 2)
        return self.client.new_order(
            symbol=symbol, side=side, type="MARKET",
            quoteOrderQty=str(q),
            recvWindow=10000, timestamp=self._ts(),
        )

    def order_market_qty(self, symbol: str, side: str, qty_base: float) -> dict:
        qty_str = self._format_qty(symbol, qty_base)
        if qty_str == "0":
            raise ValueError(f"Quantity below minQty/stepSize for {symbol}")
        return self.client.new_order(
            symbol=symbol, side=side, type="MARKET",
            quantity=qty_str,
            recvWindow=10000, timestamp=self._ts(),
        )

    def new_oco_sell(self, symbol: str, qty_base: float, tp_price: float, sl_stop: float, sl_limit: float) -> dict:
        # OCO SELL: prix TP, et stop / stopLimit un peu en-dessous
        info = self.exchange_info(symbol)
        tp_price, _ = round_price_qty(tp_price, qty_base, info)
        sl_stop, _ = round_price_qty(sl_stop, qty_base, info)
        sl_limit, _ = round_price_qty(sl_limit, qty_base, info)

        qty_str = self._format_qty(symbol, qty_base)
        if qty_str == "0":
            raise ValueError(f"Quantity below minQty/stepSize for {symbol}")

        return self.client.new_oco_order(
            symbol=symbol, side="SELL",
            quantity=qty_str,
            price=str(tp_price),
            stopPrice=str(sl_stop),
            stopLimitPrice=str(sl_limit),
            stopLimitTimeInForce="GTC",
            recvWindow=10000, timestamp=self._ts(),
        )

Side = Literal["BUY", "SELL"]

def _bps_to_float(bps: int) -> float:
    """Convertit des basis points en ratio (20 bps -> 0.002)."""
    return float(bps) / 10_000.0

def generate_ladder_prices(mid: float, side: Side, n_levels: int, step_bps: int) -> List[float]:
    """
    Génère une grille de prix autour du MID.
      BUY  : niveaux SOUS le mid (pour être servi sur repli)
      SELL : niveaux AU-DESSUS du mid (scale-out)
    Ex: step_bps=20 => 0.20% par marche.
    """
    if n_levels < 1:
        raise ValueError("n_levels must be >= 1")
    side = side.upper()
    if side not in ("BUY", "SELL"):
        raise ValueError("side must be BUY or SELL")

    step = _bps_to_float(step_bps)
    levels: List[float] = []
    for i in range(1, n_levels + 1):
        if side == "BUY":
            levels.append(mid * (1.0 - step * i))
        else:
            levels.append(mid * (1.0 + step * i))
    return levels

def equal_split(total_quote_eur: float, n: int) -> List[float]:
    """Répartition égale du budget (notionnel €) par niveau."""
    if n <= 0:
        return []
    part = max(0.0, float(total_quote_eur)) / float(n)
    return [part] * n