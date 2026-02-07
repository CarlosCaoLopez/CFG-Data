-- cleanup_stale_trades.sql
-- Ejecutar periódicamente cada hora (pg_cron / crontab)
-- Elimina trades cuyo updated_at sea de hace más de 1 hora (asumiendo error de procesamiento)
-- y los closed_market_variables asociados a esos market_id
-- Loguea market_id, número de filas eliminadas y timestamp

DO $$
DECLARE
    rec RECORD;
    total_trades_deleted INT := 0;
    total_cmv_deleted INT := 0;
    cmv_count INT;
    now_ts TEXT := TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS');
BEGIN
    RAISE NOTICE '[CLEANUP %] Iniciando limpieza de trades estancados y closed_market_variables asociados', now_ts;

    -- Agrupar por market_id los trades que se van a eliminar y loguear antes del DELETE
    FOR rec IN
        SELECT market_id, COUNT(*) AS cnt
        FROM trades
        WHERE updated_at < NOW() - INTERVAL '1 hour'
        GROUP BY market_id
    LOOP
        -- Contar closed_market_variables asociados a este market_id
        SELECT COUNT(*) INTO cmv_count
        FROM closed_market_variables
        WHERE market_id = rec.market_id;

        RAISE NOTICE '[CLEANUP %] market_id=% | trades_eliminados=% | cmv_eliminados=%', now_ts, rec.market_id, rec.cnt, cmv_count;
        total_trades_deleted := total_trades_deleted + rec.cnt;
        total_cmv_deleted := total_cmv_deleted + cmv_count;
    END LOOP;

    -- Eliminar closed_market_variables PRIMERO (FK constraint: cmv.trade_id -> trades.id)
    DELETE FROM closed_market_variables
    WHERE market_id IN (
        SELECT DISTINCT market_id
        FROM trades
        WHERE updated_at < NOW() - INTERVAL '1 hour'
    );

    -- Eliminar trades estancados
    DELETE FROM trades
    WHERE updated_at < NOW() - INTERVAL '1 hour';

    RAISE NOTICE '[CLEANUP %] Total trades eliminados: % | Total cmv eliminados: %', now_ts, total_trades_deleted, total_cmv_deleted;
END $$;
