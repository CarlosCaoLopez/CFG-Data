-- cleanup_market.sql
-- Elimina todos los trades y closed_market_variables de un market específico
-- Uso: psql -v market_id="'<condition_id>'" -f scripts/sql/cleanup_market.sql
-- Ejemplo: psql -v market_id="'0xabc123...'" -U insider -d insider_detection -f scripts/sql/cleanup_market.sql

DO $$
DECLARE
    trade_count INT;
    now_ts TEXT := TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS');
BEGIN
    -- Contar registros antes de eliminar
    SELECT COUNT(*) INTO trade_count
    FROM trades
    WHERE market_id = :market_id;

    RAISE NOTICE '[CLEANUP_MARKET %] market_id=% | trades a eliminar: %', now_ts, :market_id, trade_count;

    -- Eliminar closed_market_variables
    DELETE FROM closed_market_variables
    WHERE market_id = :market_id;

    -- Eliminar trades
    DELETE FROM trades
    WHERE market_id = :market_id;

    RAISE NOTICE '[CLEANUP_MARKET %] Limpieza completada para market_id=%', now_ts, :market_id;
END $$;
