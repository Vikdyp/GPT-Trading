from __future__ import annotations
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine, AsyncSession
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import text, bindparam
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timezone, timedelta
from typing import Any, List, Dict
import json

_engine: AsyncEngine | None = None
_sessionmaker = None

def get_engine(dsn: str) -> AsyncEngine:
    global _engine, _sessionmaker
    if _engine is None:
        _engine = create_async_engine(dsn, echo=False, pool_pre_ping=True)
        _sessionmaker = sessionmaker(_engine, expire_on_commit=False, class_=AsyncSession)
    return _engine

def get_session() -> AsyncSession:
    return _sessionmaker()

# ---------- ORDERS/FILLS/POSITIONS ----------
async def insert_order_test(session: AsyncSession, symbol: str, side: str, notional_eur: float, status: str = "TEST_OK"):
    q = text("""
        INSERT INTO orders (client_id, symbol, side, type, qty, price, status, created_at)
        VALUES (:cid, :sym, :side, 'MARKET', 0, NULL, :status, now())
        RETURNING id
    """)
    cid = f"TEST-{symbol}-{side}"
    res = await session.execute(q, {"cid": cid, "sym": symbol, "side": side, "status": status})
    row = res.fetchone()
    return row[0] if row else None

async def recent_orders(session: AsyncSession, limit: int = 50):
    q = text("""
        SELECT id, client_id, symbol, side, type, qty, price, status, created_at
        FROM orders
        ORDER BY id DESC
        LIMIT :lim
    """)
    res = await session.execute(q, {"lim": limit})
    return [dict(r._mapping) for r in res.fetchall()]

async def recent_fills(session: AsyncSession, limit: int = 100):
    q = text("""
        SELECT id, order_id, price, qty, fee, ts
        FROM fills
        ORDER BY id DESC
        LIMIT :lim
    """)
    res = await session.execute(q, {"lim": limit})
    rows = []
    for r in res.fetchall():
        m = dict(r._mapping)
        # cast decimal->float pour JSON propre
        m["price"] = float(m["price"])
        m["qty"] = float(m["qty"])
        m["fee"] = float(m["fee"]) if m["fee"] is not None else 0.0
        rows.append(m)
    return rows

async def insert_fill(session: AsyncSession, order_id: int | None, price: float, qty: float, fee: float = 0.0):
    q = text("""
        INSERT INTO fills (order_id, price, qty, fee, ts)
        VALUES (:oid, :px, :qty, :fee, now())
        RETURNING id
    """)
    res = await session.execute(q, {"oid": order_id, "px": price, "qty": qty, "fee": fee})
    return res.fetchone()[0]

async def upsert_position(session: AsyncSession, symbol: str, qty_change: float, px: float):
    sel = text("SELECT id, qty, avg_px FROM positions WHERE symbol=:s FOR UPDATE")
    res = await session.execute(sel, {"s": symbol})
    row = res.fetchone()
    if row is None:
        if qty_change <= 0:
            return None
        ins = text("""
            INSERT INTO positions (symbol, qty, avg_px, updated_at)
            VALUES (:s, :q, :px, now())
            RETURNING id
        """)
        r = await session.execute(ins, {"s": symbol, "q": qty_change, "px": px})
        return r.fetchone()[0]
    else:
        pid, qty_old, avg_old = row
        qty_new = float(qty_old) + float(qty_change)
        if abs(qty_new) < 1e-12:
            await session.execute(text("DELETE FROM positions WHERE id=:id"), {"id": pid})
            return pid
        if qty_change > 0:
            avg_new = (float(qty_old) * float(avg_old) + float(qty_change) * float(px)) / qty_new
        else:
            avg_new = float(avg_old)
        upd = text("UPDATE positions SET qty=:q, avg_px=:avg, updated_at=now() WHERE id=:id")
        await session.execute(upd, {"q": qty_new, "avg": avg_new, "id": pid})
        return pid

async def list_positions(session: AsyncSession):
    q = text("""SELECT symbol, qty, avg_px, updated_at FROM positions ORDER BY symbol""")
    res = await session.execute(q)
    rows = []
    for r in res.fetchall():
        m = dict(r._mapping)
        m["qty"] = float(m["qty"])
        m["avg_px"] = float(m["avg_px"])
        rows.append(m)
    return rows

# ---------- SETTINGS (overrides) ----------
async def ensure_settings_table(session: AsyncSession):
    await session.execute(text("""
        CREATE TABLE IF NOT EXISTS settings (
          key TEXT PRIMARY KEY,
          value JSONB NOT NULL,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """))
    await session.commit()

async def settings_get(session: AsyncSession, key: str):
    res = await session.execute(text("SELECT value FROM settings WHERE key=:k"), {"k": key})
    row = res.fetchone()
    if not row:
        return None
    val = row[0]
    if isinstance(val, (dict, list)):
        return val
    try:
        return json.loads(val)
    except Exception:
        return None

async def settings_set(session: AsyncSession, key: str, value: Any):
    if isinstance(value, (str, bytes)):
        try:
            value = json.loads(value)
        except Exception:
            value = {"raw": str(value)}

    q = text("""
        INSERT INTO settings (key, value, updated_at)
        VALUES (:k, :v, now())
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()
    """).bindparams(bindparam("v", type_=JSONB))

    await session.execute(q, {"k": key, "v": value})
    await session.commit()

# ---------- EQUITY HISTORY ----------
async def ensure_equity_table(session):
    # 1/ créer la table
    await session.execute(text("""
        CREATE TABLE IF NOT EXISTS equity_points (
          id BIGSERIAL PRIMARY KEY,
          ts TIMESTAMPTZ NOT NULL,
          equity_eur NUMERIC NOT NULL
        )
    """))
    # 2/ créer l'index dans un appel séparé
    await session.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_equity_ts ON equity_points(ts)
    """))
    await session.commit()

async def insert_equity_point(session: AsyncSession, ts: datetime, equity_eur: float):
    q = text("""
        INSERT INTO equity_points (ts, equity_eur)
        VALUES (:ts, :eq)
    """)
    await session.execute(q, {"ts": ts, "eq": equity_eur})
    # pas de commit ici: laisse l'appelant décider

async def get_equity_series(session: AsyncSession, since: datetime):
    q = text("""
        SELECT ts, equity_eur
        FROM equity_points
        WHERE ts >= :since
        ORDER BY ts ASC
    """)
    res = await session.execute(q, {"since": since})
    out = []
    for r in res.fetchall():
        m = dict(r._mapping)
        m["equity_eur"] = float(m["equity_eur"])
        out.append(m)
    return out
