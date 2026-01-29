# Database Migrations

SQL migrations for the Polymarket Insider Detection System.

## Migration Order

Migrations must be applied in numerical order:

1. **001_initial_schema.sql** - Creates core tables (wallets, markets, trades, scores, alerts)
2. **002_indexes.sql** - Adds indexes for query optimization

## Running Migrations

### Using psql

```bash
# Connect to database
psql -U insider -d insider_detection

# Run migrations in order
\i database/migrations/001_initial_schema.sql
\i database/migrations/002_indexes.sql
```

### Using Docker

```bash
# Copy migrations to container
docker cp database/migrations/ postgres_container:/tmp/migrations/

# Execute in container
docker exec -i postgres_container psql -U insider -d insider_detection < /tmp/migrations/001_initial_schema.sql
docker exec -i postgres_container psql -U insider -d insider_detection < /tmp/migrations/002_indexes.sql
```

### Using Alembic (recommended for production)

```bash
# Generate migration from SQLAlchemy models
alembic revision --autogenerate -m "initial schema"

# Apply migrations
alembic upgrade head

# Rollback
alembic downgrade -1
```

## Table Schema

### wallets
Stores wallet metadata and aggregated statistics.

| Column | Type | Description |
|--------|------|-------------|
| address | TEXT | Primary key, 0x-prefixed lowercase |
| first_seen_polymarket | TIMESTAMPTZ | First tx on Polymarket |
| first_seen_polygon | TIMESTAMPTZ | First tx on Polygon chain |
| total_volume_usdc | DECIMAL | Cumulative trading volume |
| total_markets | INT | Number of markets traded |

### markets
Stores market metadata and resolution data.

| Column | Type | Description |
|--------|------|-------------|
| market_id | TEXT | Primary key, conditionId |
| question | TEXT | Market question |
| end_date | TIMESTAMPTZ | Resolution date |
| closed | BOOLEAN | Whether market is resolved |
| winning_outcome | TEXT | "Yes" or "No" if resolved |
| volume_usdc | DECIMAL | Total market volume |

### trades
Individual trade records.

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| tx_hash | TEXT | Unique transaction hash |
| wallet | TEXT | Foreign key to wallets |
| market_id | TEXT | Foreign key to markets |
| timestamp | TIMESTAMPTZ | Trade execution time |
| side | TEXT | "BUY" or "SELL" |
| outcome | TEXT | "Yes" or "No" |
| price | DECIMAL | Price (0.0 - 1.0) |
| size_usdc | DECIMAL | Trade size in USDC |
| size_tokens | DECIMAL | Trade size in tokens |

### scores
Calculated I_scores with component breakdown.

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| wallet | TEXT | Foreign key to wallets |
| market_id | TEXT | Foreign key to markets |
| trade_id | INT | Foreign key to trades |
| i_score | DECIMAL | Combined insider score |
| percentile | DECIMAL | Score percentile in market |
| component_a | DECIMAL | Impacto del Éxito |
| component_b | DECIMAL | Factor Fantasma |
| component_c | DECIMAL | Agresividad Focalizada |
| delta_elo | DECIMAL | ELO change |
| volume_usdc | DECIMAL | Trade volume |
| won | BOOLEAN | Whether trade won |
| price_at_trade | DECIMAL | Market price at trade |
| age_days | DECIMAL | Wallet age in days |
| hours_to_resolution | DECIMAL | Hours until market close |
| slippage | DECIMAL | Price impact |
| portfolio_entropy | DECIMAL | Portfolio concentration |
| parameters_version | TEXT | Parameters version (e.g., "v1.0") |

### alerts
Generated alerts for suspicious activity.

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| wallet | TEXT | Wallet address |
| market_id | TEXT | Market condition ID |
| score_id | INT | Foreign key to scores |
| i_score | DECIMAL | Triggering score |
| percentile | DECIMAL | Score percentile |
| alert_type | TEXT | "P95", "P99", or "EXTREME" |
| status | TEXT | "NEW", "REVIEWED", "DISMISSED", "CONFIRMED" |
| reviewed_by | TEXT | Reviewer identifier |
| reviewed_at | TIMESTAMPTZ | Review timestamp |
| notes | TEXT | Review notes |

## Indexes

Performance-critical indexes on frequently queried columns:

- **Trades**: wallet, market_id, timestamp (DESC)
- **Scores**: wallet, market_id, i_score (DESC), percentile (DESC)
- **Alerts**: (status, created_at DESC), wallet, percentile (DESC)

## Notes

- All timestamps use `TIMESTAMPTZ` (timezone-aware)
- Foreign keys maintain referential integrity
- Unique constraint on `(wallet, market_id, trade_id)` prevents duplicate scores
- Indexes are optimized for top-N queries (DESC order) and filtering
