-- 02_events_and_idempotence.sql

-- Events table for alerting/ops
CREATE TABLE IF NOT EXISTS events (
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL DEFAULT now(),
  level TEXT NOT NULL,
  source TEXT NOT NULL,
  type TEXT NOT NULL,
  payload JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts DESC);

-- Add trade_id to fills for idempotence (safe if already exists)
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='fills' AND column_name='trade_id') THEN
    ALTER TABLE fills ADD COLUMN trade_id TEXT;
  END IF;
END $$;

-- Unique index on (order_id, trade_id) when both present
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname='uq_fills_order_trade') THEN
    CREATE UNIQUE INDEX uq_fills_order_trade ON fills(order_id, trade_id) WHERE order_id IS NOT NULL AND trade_id IS NOT NULL;
  END IF;
END $$;
