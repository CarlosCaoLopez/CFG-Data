-- Migration: add last_trade column to insiders_trades
-- Mirrors the same column in the trades table.
-- last_trade = TRUE only for the most recent trade before market close.

ALTER TABLE insiders_trades
    ADD COLUMN IF NOT EXISTS last_trade BOOLEAN NOT NULL DEFAULT FALSE;
