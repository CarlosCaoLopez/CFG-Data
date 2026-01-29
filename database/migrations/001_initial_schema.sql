-- Initial schema migration
-- Creates tables: wallets, markets, trades, scores, alerts

-- Wallets analizadas
CREATE TABLE wallets (
    address TEXT PRIMARY KEY,
    first_seen_polymarket TIMESTAMPTZ,
    first_seen_polygon TIMESTAMPTZ,
    total_volume_usdc DECIMAL DEFAULT 0,
    total_markets INT DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Mercados procesados
CREATE TABLE markets (
    market_id TEXT PRIMARY KEY,
    question TEXT,
    end_date TIMESTAMPTZ,
    closed BOOLEAN DEFAULT FALSE,
    winning_outcome TEXT,
    volume_usdc DECIMAL,
    processed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Trades indexados
CREATE TABLE trades (
    id SERIAL PRIMARY KEY,
    tx_hash TEXT UNIQUE NOT NULL,
    wallet TEXT REFERENCES wallets(address),
    market_id TEXT REFERENCES markets(market_id),
    timestamp TIMESTAMPTZ NOT NULL,
    side TEXT NOT NULL,
    outcome TEXT NOT NULL,
    price DECIMAL NOT NULL,
    size_usdc DECIMAL NOT NULL,
    size_tokens DECIMAL NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Scores calculados
CREATE TABLE scores (
    id SERIAL PRIMARY KEY,
    wallet TEXT REFERENCES wallets(address),
    market_id TEXT REFERENCES markets(market_id),
    trade_id INT REFERENCES trades(id),

    i_score DECIMAL NOT NULL,
    percentile DECIMAL,

    component_a DECIMAL NOT NULL,
    component_b DECIMAL NOT NULL,
    component_c DECIMAL NOT NULL,

    delta_elo DECIMAL,
    volume_usdc DECIMAL,
    won BOOLEAN,
    price_at_trade DECIMAL,
    age_days DECIMAL,
    hours_to_resolution DECIMAL,
    slippage DECIMAL,
    portfolio_entropy DECIMAL,

    parameters_version TEXT DEFAULT 'v1.0',
    calculated_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE(wallet, market_id, trade_id)
);

-- Alertas
CREATE TABLE alerts (
    id SERIAL PRIMARY KEY,
    wallet TEXT NOT NULL,
    market_id TEXT NOT NULL,
    score_id INT REFERENCES scores(id),

    i_score DECIMAL NOT NULL,
    percentile DECIMAL NOT NULL,
    alert_type TEXT NOT NULL,

    status TEXT DEFAULT 'NEW',
    reviewed_by TEXT,
    reviewed_at TIMESTAMPTZ,
    notes TEXT,

    created_at TIMESTAMPTZ DEFAULT NOW()
);
