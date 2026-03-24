-- compute_variables.sql
-- Llamado desde la task Celery compute_variables para un :market_id dado
-- Calcula variables intermedias y las inserta en closed_market_variables
--
-- Variables calculadas:
--   delta_t         = (Market.end_date - Trade.timestamp) en horas
--   age             = MAX(1, (Market.end_date - Wallet.first_seen_at)) en días
--   winning_volume  = SUM(size_usdc * prize * ±1) por wallet en el market
--   alpha_slippage  = size_usdc / volume_usdc (simplificación algebraica) si prize > 0, sino 0
--   entropy (H)     = -SUM(p * ln(p)) donde p = volumen por categoría / volumen total de la wallet

WITH
-- 1. Trades del market con datos de wallet y market
-- Usamos LEFT JOIN para no perder trades si la wallet aún no se ha guardado
trade_data AS (
    SELECT
        t.wallet_id,
        t.market_id,
        t.timestamp,
        t.outcome,
        t.prize,
        t.size_usdc,
        m.end_date,
        m.volume_usdc AS market_volume,
        m.winning_outcome,
        w.first_seen_at
    FROM trades t
    JOIN markets m ON m.market_id = t.market_id
    LEFT JOIN wallets w ON w.address = t.wallet_id
    WHERE t.market_id = %(market_id)s
),

-- 2. Agrupación base por wallet para este market
wallet_market_stats AS (
    SELECT
        wallet_id,
        market_id,

        -- delta_t: cogemos el trade más cercano al final (el tiempo mínimo)
        MIN(EXTRACT(EPOCH FROM (end_date - timestamp)) / 3600.0) AS delta_t,

        -- age: días desde que se vio la wallet (mínimo 1). Si no existe la wallet, asignamos 1.
        COALESCE(
            MAX(GREATEST(1, EXTRACT(EPOCH FROM (end_date - first_seen_at)) / 86400.0)),
            1
        ) AS age,

        -- winning_volume: suma de size * prize * (+1 o -1)
        SUM(
            size_usdc * prize * CASE WHEN outcome = winning_outcome THEN 1 ELSE -1 END
        ) AS winning_volume,

        -- alpha_slippage: suma del tamaño en outcomes ganadores / volumen total del mercado
        CASE
            WHEN MAX(market_volume) > 0
            THEN SUM(CASE WHEN prize > 0 THEN size_usdc ELSE 0 END) / MAX(market_volume)
            ELSE 0
        END AS alpha_slippage

    FROM trade_data
    GROUP BY wallet_id, market_id
),

-- 3. Entropía: volumen por wallet por categoría (se mantiene tu lógica original)
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
            (wcv.cat_volume / wtv.total_volume) * LN(wcv.cat_volume / wtv.total_volume)
        ) AS entropy
    FROM wallet_category_volume wcv
    JOIN wallet_total_volume wtv ON wcv.wallet_id = wtv.wallet_id
    WHERE wtv.total_volume > 0
      AND wcv.cat_volume > 0
    GROUP BY wcv.wallet_id
)

-- 4. INSERT final (Una fila por wallet y market)
INSERT INTO closed_market_variables (wallet_id, market_id, delta_t, age, winning_volume, alpha_slippage, entropy)
SELECT
    wms.wallet_id,
    wms.market_id,
    wms.delta_t,
    wms.age,
    wms.winning_volume,
    wms.alpha_slippage,
    COALESCE(we.entropy, 0) AS entropy
FROM wallet_market_stats wms
LEFT JOIN wallet_entropy we ON we.wallet_id = wms.wallet_id
ON CONFLICT (wallet_id, market_id) DO UPDATE SET
    delta_t        = EXCLUDED.delta_t,
    age            = EXCLUDED.age,
    winning_volume = EXCLUDED.winning_volume,
    alpha_slippage = EXCLUDED.alpha_slippage,
    entropy        = EXCLUDED.entropy;
