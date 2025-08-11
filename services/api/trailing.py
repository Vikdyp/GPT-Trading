# services/api/trailing.py
import json
import asyncio
from typing import Optional, Dict, Any
import redis.asyncio as redis
from db import get_session, insert_fill, upsert_position, get_position, metrics_add_trade
from notify import notify as _notify

class TrailingManager:
    """
    Paper-mode OCO+trailing (100% position par défaut).
    État par symbole stocké dans Redis: hash 'trailing:states' -> JSON
    {
      "symbol": "BTCEUR",
      "entry_px": 25000.0,
      "qty": 0.002,                # qty totale suivie
      "percent": 1.0,              # fraction 0..1 à couvrir par OCO
      "tp_pct": 0.016,             # +1.6%
      "sl_pct": 0.008,             # -0.8% (basé ATR/stop_pct signal)
      "trail_mult": 1.2,           # trailing = high * (1 - sl_pct*trail_mult)
      "high": 25000.0,             # plus-haut depuis l'entrée
      "active": true
    }
    """
    def __init__(self, r: redis.Redis):
        self.r = r

    async def _get_last_price(self, symbol: str) -> Optional[float]:
        last = await self.r.hget("last_ticks", symbol)
        if not last:
            return None
        try:
            return float(json.loads(last)["close"])
        except Exception:
            return None

    async def list_states(self) -> Dict[str, Any]:
        return await self.r.hgetall("trailing:states")

    async def start(self, symbol: str, entry_px: float, qty: float,
                    tp_pct: float, sl_pct: float, percent: float = 1.0, trail_mult: float = 1.2):
        state = {
            "symbol": symbol,
            "entry_px": float(entry_px),
            "qty": float(qty),
            "percent": max(0.0, min(1.0, float(percent))),
            "tp_pct": max(0.0, float(tp_pct)),
            "sl_pct": max(0.0, float(sl_pct)),
            "trail_mult": max(0.0, float(trail_mult)),
            "high": float(entry_px),
            "active": True
        }
        await self.r.hset("trailing:states", symbol, json.dumps(state))

    async def stop(self, symbol: str):
        await self.r.hdel("trailing:states", symbol)

    async def loop(self):
        """Vérifie chaque seconde si TP/SL/trailing est touché → simule une vente & ferme le suivi."""
        while True:
            try:
                all_states = await self.r.hgetall("trailing:states")
                if not all_states:
                    await asyncio.sleep(1.0)
                    continue

                for symbol, raw in list(all_states.items()):
                    try:
                        st = json.loads(raw)
                    except Exception:
                        continue
                    if not st or not st.get("active", False):
                        continue

                    price = await self._get_last_price(symbol)
                    if price is None:
                        continue

                    # maj du plus-haut
                    if price > st["high"]:
                        st["high"] = price

                    # niveaux TP/SL
                    tp_px = st["entry_px"] * (1.0 + float(st["tp_pct"]))
                    # trailing stop basé sur le plus-haut
                    base_sl = st["entry_px"] * (1.0 - float(st["sl_pct"]))
                    dyn_sl = st["high"] * (1.0 - float(st["sl_pct"]) * float(st.get("trail_mult", 1.2)))
                    sl_px = max(base_sl, dyn_sl)

                    hit_tp = price >= tp_px
                    hit_sl = price <= sl_px

                    if hit_tp or hit_sl:
                        sell_qty = float(st["qty"]) * float(st["percent"])
                        # Simuler le fill SELL sur le prix courant + realized PnL approximatif
                        async with get_session() as s:
                            realized = 0.0
                            prev = await get_position(s, symbol)
                            if prev:
                                prev_qty, prev_avg = prev
                                if prev_qty > 0:
                                    closed = min(prev_qty, sell_qty)
                                    realized = closed * (price - prev_avg)
                            await insert_fill(s, None, price, -sell_qty, 0.0)
                            await upsert_position(s, symbol, -sell_qty, price)
                            try:
                                await metrics_add_trade(s, realized, 0.0, sell_qty * price)
                            except Exception:
                                pass
                            await s.commit()

                        # Désactiver ce tracker
                        st["active"] = False
                        await self.r.hset("trailing:states", symbol, json.dumps(st))
                        await self.r.hdel("trailing:states", symbol)
                        print(f"[trail] close {symbol} @ {price:.8f} ({'TP' if hit_tp else 'SL'}) qty={sell_qty}")
                        try:
                            await _notify("TP_HIT" if hit_tp else "SL_HIT", {"symbol": symbol, "price": price, "qty": sell_qty})
                        except Exception:
                            pass
                        continue

                    # persister l'état (high évolue)
                    await self.r.hset("trailing:states", symbol, json.dumps(st))
            except Exception as e:
                print("[trail] loop error:", e)
            await asyncio.sleep(1.0)
