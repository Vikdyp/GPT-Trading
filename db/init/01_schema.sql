-- Basic schema for MVP
CREATE TABLE IF NOT EXISTS candles (
  id BIGSERIAL PRIMARY KEY,
  symbol TEXT NOT NULL,
  tf TEXT NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  open NUMERIC,
  high NUMERIC,
  low NUMERIC,
  close NUMERIC,
  volume NUMERIC,
  UNIQUE(symbol, tf, ts)
);

CREATE TABLE IF NOT EXISTS orders (
  id BIGSERIAL PRIMARY KEY,
  client_id TEXT,
  symbol TEXT NOT NULL,
  side TEXT NOT NULL,
  type TEXT NOT NULL,
  qty NUMERIC NOT NULL,
  price NUMERIC,
  status TEXT,
  error TEXT,
  created_at TIMESTAMPTZ DEFAULT now(),
  done_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS fills (
  id BIGSERIAL PRIMARY KEY,
  order_id BIGINT REFERENCES orders(id),
  price NUMERIC,
  qty NUMERIC,
  fee NUMERIC,
  ts TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS positions (
  id BIGSERIAL PRIMARY KEY,
  symbol TEXT NOT NULL,
  qty NUMERIC NOT NULL,
  avg_px NUMERIC NOT NULL,
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS metrics_daily (
  day DATE PRIMARY KEY,
  pnl NUMERIC DEFAULT 0,
  fees NUMERIC DEFAULT 0,
  turnover NUMERIC DEFAULT 0,
  drawdown NUMERIC DEFAULT 0,
  trades INT DEFAULT 0,
  winrate NUMERIC DEFAULT 0
);
