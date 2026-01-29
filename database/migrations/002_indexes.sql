-- Index creation for query optimization

-- Trades indexes
CREATE INDEX idx_trades_wallet ON trades(wallet);
CREATE INDEX idx_trades_market ON trades(market_id);
CREATE INDEX idx_trades_timestamp ON trades(timestamp DESC);

-- Scores indexes
CREATE INDEX idx_scores_wallet ON scores(wallet);
CREATE INDEX idx_scores_market ON scores(market_id);
CREATE INDEX idx_scores_iscore ON scores(i_score DESC);
CREATE INDEX idx_scores_percentile ON scores(percentile DESC);

-- Alerts indexes
CREATE INDEX idx_alerts_status ON alerts(status, created_at DESC);
CREATE INDEX idx_alerts_wallet ON alerts(wallet);
CREATE INDEX idx_alerts_percentile ON alerts(percentile DESC);
