from pydantic import BaseModel, Field
from typing import Optional, Literal, List, Dict

class Candle(BaseModel):
    symbol: str
    tf: str
    ts: int  # ms epoch
    open: float
    high: float
    low: float
    close: float
    volume: float

class Signal(BaseModel):
    symbol: str
    score: float = Field(ge=0.0, le=1.0)
    stop_pct: float
    atr: float
    trend_ok: bool

class OrderIntent(BaseModel):
    symbol: str
    side: Literal["BUY", "SELL"]
    qty: float
    price: Optional[float] = None
    type: Literal["LIMIT", "MARKET"] = "LIMIT"
    client_id: Optional[str] = None
    meta: Optional[Dict] = None
