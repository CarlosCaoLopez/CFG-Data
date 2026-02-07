-- compute_variables.sql
-- Llamado desde la task Celery compute_variables para un :market_id dado
-- Calcula variables intermedias por trade y las inserta en closed_market_variables
--
-- Variables calculadas:
--   delta_t         = (Market.end_date - Trade.timestamp) en horas
--   age             = MAX(1, (Market.end_date - Wallet.first_seen_at)) en días
--   winning_volume  = SUM(size_usdc * prize * ±1) por wallet en el market
--   alpha_slippage  = size_usdc / volume_usdc (simplificación algebraica) si prize > 0, sino 0
--   entropy (H)     = -SUM(p * ln(p)) donde p = volumen por categoría / volumen total de la wallet

WITH
-- 1. Trades del market con datos de wallet y market
trade_data AS (
    SELECT
        t.id              AS trade_id,
        t.wallet_id,
        t.market_id,
        t.timestamp,
        t.outcome,
        t.prize,
        t.size_usdc,
        m.end_date,
        m.volume_usdc     AS market_volume,
        m.winning_outcome,
        w.first_seen_at
    FROM trades t
    JOIN markets m ON m.market_id = t.market_id
    JOIN wallets w ON w.address   = t.wallet_id
    WHERE t.market_id = :market_id
),

-- 2. Winning volume por wallet (agregado, se duplicará en cada fila de trade)
wallet_winning_volume AS (
    SELECT
        wallet_id,
        SUM(
            size_usdc * prize *
            CASE WHEN outcome = winning_outcome THEN 1 ELSE -1 END
        ) AS winning_volume
    FROM trade_data
    GROUP BY wallet_id
),

-- 3. Entropía: volumen por wallet por categoría (todos los trades de la wallet, no solo este market)
--    Unnest de categories del market para asignar cada trade a sus categorías
wallet_category_volume AS (
    SELECT
        t.wallet_id,
        cat.category,
        SUM(t.size_usdc) AS cat_volume
    FROM trades t
    JOIN markets m ON m.market_id = t.market_id
    CROSS JOIN LATERAL unnest(m.categories) AS cat(category)
    WHERE t.wallet_id IN (SELECT DISTINCT wallet_id FROM trade_data)
      AND m.categories IS NOT NULL
    GROUP BY t.wallet_id, cat.category
),

wallet_total_volume AS (
    SELECT
        wallet_id,
        SUM(cat_volume) AS total_volume
    FROM wallet_category_volume
    GROUP BY wallet_id
),

wallet_entropy AS (
    SELECT
        wcv.wallet_id,
        -SUM(
            (wcv.cat_volume / wtv.total_volume) *
            LN(wcv.cat_volume / wtv.total_volume)
        ) AS entropy
    FROM wallet_category_volume wcv
    JOIN wallet_total_volume wtv ON wcv.wallet_id = wtv.wallet_id
    WHERE wtv.total_volume > 0
      AND wcv.cat_volume > 0
    GROUP BY wcv.wallet_id
)

-- INSERT final con ON CONFLICT para idempotencia
INSERT INTO closed_market_variables (wallet_id, market_id, trade_id, delta_t, age, winning_volume, alpha_slippage, entropy)
SELECT
    td.wallet_id,
    td.market_id,
    td.trade_id,

    -- delta_t: horas entre cierre del market y timestamp del trade
    EXTRACT(EPOCH FROM (td.end_date - td.timestamp)) / 3600.0
        AS delta_t,

    -- age: días entre cierre del market y primera aparición de la wallet (mínimo 1)
    GREATEST(1, EXTRACT(EPOCH FROM (td.end_date - td.first_seen_at)) / 86400.0)
        AS age,

    -- winning_volume: agregado por wallet (duplicado en cada trade)
    COALESCE(wv.winning_volume, 0)
        AS winning_volume,

    -- alpha_slippage: simplificación de (prize*(1+size/vol) - prize) / prize = size/vol
    CASE
        WHEN td.prize > 0 AND td.market_volume > 0
        THEN td.size_usdc / td.market_volume
        ELSE 0
    END
        AS alpha_slippage,

    -- entropy: H de la wallet (duplicado en cada trade)
    COALESCE(we.entropy, 0)
        AS entropy

FROM trade_data td
LEFT JOIN wallet_winning_volume wv ON wv.wallet_id = td.wallet_id
LEFT JOIN wallet_entropy we        ON we.wallet_id = td.wallet_id

ON CONFLICT (wallet_id, market_id, trade_id) DO UPDATE SET
    delta_t        = EXCLUDED.delta_t,
    age            = EXCLUDED.age,
    winning_volume = EXCLUDED.winning_volume,
    alpha_slippage = EXCLUDED.alpha_slippage,
    entropy        = EXCLUDED.entropy;
