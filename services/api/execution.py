from __future__ import annotations
import time
import math
from typing import Dict, Any
from binance.spot import Spot

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
        q = max(0.0, float(qty_base))
        try:
            return self.client.new_order_test(
                symbol=symbol, side=side, type="MARKET",
                quantity=str(q),
                recvWindow=10000, timestamp=self._ts(),
            )
        except Exception:
            self.sync_time()
            return self.client.new_order_test(
                symbol=symbol, side=side, type="MARKET",
                quantity=str(q),
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
        q = max(0.0, float(qty_base))
        return self.client.new_order(
            symbol=symbol, side=side, type="MARKET",
            quantity=str(q),
            recvWindow=10000, timestamp=self._ts(),
        )

    def new_oco_sell(self, symbol: str, qty_base: float, tp_price: float, sl_stop: float, sl_limit: float) -> dict:
        # OCO SELL: prix TP, et stop / stopLimit un chouïa en-dessous
        info = self.exchange_info(symbol)
        tp_price, _ = round_price_qty(tp_price, qty_base, info)
        sl_stop, _ = round_price_qty(sl_stop, qty_base, info)
        sl_limit, qty_base = round_price_qty(sl_limit, qty_base, info)
        return self.client.new_oco_order(
            symbol=symbol, side="SELL",
            quantity=str(qty_base),
            price=str(tp_price),
            stopPrice=str(sl_stop),
            stopLimitPrice=str(sl_limit),
            stopLimitTimeInForce="GTC",
            recvWindow=10000, timestamp=self._ts(),
        )
