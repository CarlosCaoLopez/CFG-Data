import os
import csv
import time
import math
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import text
import httpx
from celery.utils.log import get_task_logger
from workers.celery_app import app
from contracts.processing_job import JobResult
from database.connection import get_sync_session

# from database.manager import DatabaseManager  # Commented out - not used after removing score persistence

# Ruta al script SQL de compute_variables
_SQL_DIR = Path(__file__).resolve().parent.parent / "scripts" / "sql"

# CSV debug logs for CMV
_LOGS_DIR = Path(__file__).resolve().parent.parent / "logs"
_CMV_DIR = _LOGS_DIR / "cmv"


def _cmv_inputs_path(market_id: str) -> Path:
    return _CMV_DIR / f"cmv_inputs_{market_id}.csv"


def _cmv_outputs_path(market_id: str) -> Path:
    return _CMV_DIR / f"cmv_outputs_{market_id}.csv"

_CMV_INPUT_HEADERS = [
    "timestamp", "market_id", "wallet_id", "trade_timestamp", "outcome",
    "prize", "size_usdc", "side", "end_date", "market_volume",
    "winning_outcome", "first_seen_at",
]
_CMV_OUTPUT_HEADERS = [
    "timestamp", "market_id", "wallet_id",
    "delta_t", "age", "winning_volume", "alpha_slippage", "entropy",
]


def _append_csv(path: Path, headers: list[str], rows: list[list]):
    """Append rows to a CSV file, writing headers if the file is new."""
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists() or path.stat().st_size == 0
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(headers)
        writer.writerows(rows)


# Logger for tasks
logger = get_task_logger(__name__)


@app.task(bind=True, max_retries=3)
def calculate_i_score(self, job_dict: dict) -> JobResult:
    """
    Celery main task.
    We have a dictionary because Celery serializes to JSON by default
    """
    logger.info(f"Iniciando calculate_i_score para job_id={job_dict.get('job_id')}")
    start_time = time.time()
    load_dotenv()

    job_id = job_dict.get("job_id")
    payload = job_dict.get("payload", {})
    market = payload.get("market", {})
    trade = payload.get("trade", {})

    # Component 1
    trade_volume = float(trade.get("size_usdc", 0))
    # No tenemos un winning_outcome. Como sé cuál de los dos ha ganado????????????????????????????????????????????????????????
    is_win = 1 if market.get("closed") and market.get("winning_outcome") == trade.get("outcome") else 0
    p_market = float(trade.get("price"))
    delta_elo = float(os.environ.get("ELO_K_FACTOR", "32")) * math.log(1 + trade_volume) * (is_win - p_market)

    # Component 2 and 3
    fmt = "%Y-%m-%dT%H:%M:%S.%fZ"
    end_date = datetime.strptime(market.get("end_date"), fmt)
    trade_date = datetime.strptime(trade.get("timestamp"), fmt)
    wallet_first_date = datetime.strptime(payload.get("wallet_age"), fmt)

    delta_time = ((end_date - trade_date).total_seconds() + 1) / 3600 # +1 second to avoid division by zero. Measured in hours
    account_age = max((end_date - wallet_first_date).days, 1) # 1 to avoid division by zero. Measured in days

    # Component 4
    proportions = [] # De donde los saco ???????????????????????????????????????????????????????????????????
    H = - sum(p * math.log(p) for p in proportions) # Market entropy

    p_initial = p_market # Price before the trade
    market_volume = float(market.get("volume_usdc"))
    p_execution = p_market * (1 + (trade_volume / market_volume)) # Estimation of the price after the trade
    alpha = (p_execution - p_initial) / p_initial if p_initial > 0 else 0

    component_a = delta_elo
    component_b = 1 / delta_time
    component_c = 1 / account_age
    component_d = math.exp(-H) * alpha

    I_score = float(os.environ.get("THETA_1")) * component_a + float(os.environ.get("THETA_2")) * component_b + \
        float(os.environ.get("THETA_3")) * component_c + float(os.environ.get("THETA_4")) * component_d



    #############################################################################################################
    # TODO: Implementar guardado de resultados
    # La tabla scores fue eliminada. Decidir cómo guardar los resultados del cálculo de I_score:
    # - Opción 1: Crear nueva tabla de resultados
    # - Opción 2: Guardar en closed_market_variables
    # - Opción 3: Devolver solo el resultado sin persistir
    #############################################################################################################

    # async def save_to_db():
    #     async with async_session() as session:
    #         db = DatabaseManager(session)
    #         # TODO: Implementar guardado
    #
    # asyncio.run(save_to_db())


    end_time = time.time()
    processing_time_ms = int((end_time - start_time) * 1000)

    return {
        "job_id": job_id,
        "success": True,
        "scores_calculated": 1,
        "processing_time_ms": processing_time_ms
    }


@app.task(bind=True, max_retries=3)
def compute_variables(self, market_id: str) -> dict:
    """
    Calcula las variables intermedias de un market cerrado y las guarda en closed_market_variables.
    Ejecuta el script SQL compute_variables.sql con el market_id como parámetro.
    """
    start_time = time.time()

    sql_path = _SQL_DIR / "compute_variables.sql"
    sql_text = sql_path.read_text(encoding="utf-8")

    try:
        with get_sync_session() as session:
            # Use exec_driver_sql to handle complex CTEs with PostgreSQL driver directly
            # text() + session.execute() has issues with complex CTEs and INSERT statements
            connection = session.connection()

            # --- Log INPUTS: trades + wallet/market data that feed the SQL ---
            now_str = datetime.utcnow().isoformat()
            try:
                input_rows_result = connection.exec_driver_sql(
                    "SELECT t.wallet_id, t.timestamp, t.outcome, t.prize, "
                    "       t.size_usdc, t.side, "
                    "       m.end_date, m.volume_usdc, m.winning_outcome, "
                    "       w.first_seen_at "
                    "FROM trades t "
                    "JOIN markets m ON m.market_id = t.market_id "
                    "LEFT JOIN wallets w ON w.address = t.wallet_id "
                    "WHERE t.market_id = %(market_id)s",
                    {"market_id": market_id}
                )
                input_csv_rows = []
                for r in input_rows_result:
                    input_csv_rows.append([
                        now_str, market_id,
                        r[0], r[1], r[2], r[3], r[4], r[5],
                        r[6], r[7], r[8], r[9],
                    ])
                if input_csv_rows:
                    _append_csv(_cmv_inputs_path(market_id), _CMV_INPUT_HEADERS, input_csv_rows)
                    logger.info(f"CMV debug: logged {len(input_csv_rows)} input rows for market={market_id}")
            except Exception as log_err:
                logger.warning(f"CMV debug: failed to log inputs for market={market_id}: {log_err}")

            # --- Execute the actual computation ---
            result = connection.exec_driver_sql(
                sql_text,
                {"market_id": market_id}
            )
            rows_affected = result.rowcount

            # --- Log OUTPUTS: computed CMV values ---
            try:
                output_rows_result = connection.exec_driver_sql(
                    "SELECT wallet_id, delta_t, age, winning_volume, alpha_slippage, entropy "
                    "FROM closed_market_variables WHERE market_id = %(market_id)s",
                    {"market_id": market_id}
                )
                output_csv_rows = []
                for r in output_rows_result:
                    output_csv_rows.append([
                        now_str, market_id,
                        r[0], r[1], r[2], r[3], r[4], r[5],
                    ])
                if output_csv_rows:
                    _append_csv(_cmv_outputs_path(market_id), _CMV_OUTPUT_HEADERS, output_csv_rows)
                    logger.info(f"CMV debug: logged {len(output_csv_rows)} output rows for market={market_id}")
            except Exception as log_err:
                logger.warning(f"CMV debug: failed to log outputs for market={market_id}: {log_err}")

        processing_time_ms = int((time.time() - start_time) * 1000)

        # Dispatch D: send market_id to the R/Plumber inference engine
        run_market_inference.delay(market_id)

        return {
            "market_id": market_id,
            "success": True,
            "rows_inserted": rows_affected,
            "processing_time_ms": processing_time_ms
        }
    except Exception as e:
        logger.error(f"Error en compute_variables para market_id={market_id}: {str(e)}", exc_info=True)
        raise


@app.task(bind=True, max_retries=3)
def cleanup_stale_trades(self) -> dict:
    """
    Limpia trades estancados (updated_at > 1 hora) y sus closed_market_variables asociados.
    Ejecuta el script SQL cleanup_stale_trades.sql.
    Esta tarea se ejecuta periódicamente cada hora vía Celery Beat.
    """
    logger.info("Iniciando cleanup_stale_trades")
    start_time = time.time()

    sql_path = _SQL_DIR / "cleanup_stale_trades.sql"
    sql_text = sql_path.read_text(encoding="utf-8")

    try:
        with get_sync_session() as session:
            # Use exec_driver_sql to handle PL/pgSQL blocks (DO $$ $$) directly
            connection = session.connection()
            connection.exec_driver_sql(sql_text)

        processing_time_ms = int((time.time() - start_time) * 1000)

        return {
            "success": True,
            "task": "cleanup_stale_trades",
            "processing_time_ms": processing_time_ms
        }
    except Exception as e:
        logger.error(f"Error en cleanup_stale_trades: {str(e)}", exc_info=True)
        raise


@app.task(bind=True, max_retries=3)
def cleanup_market(self, market_id: str) -> dict:
    """
    Task T4 (reactive): Deletes all trades and closed_market_variables for a processed
    market. Triggered by E immediately after D finishes, so data is removed as soon as
    the motor de cálculo has consumed it.

    FK order: CMV first (cmv.trade_id → trades.id), then trades.
    If E already deleted CMV inline, the first DELETE is a no-op.
    """
    logger.info(f"T4: cleanup_market starting for market_id={market_id}")
    start_time = time.time()

    try:
        with get_sync_session() as session:
            cmv_result = session.execute(
                text("DELETE FROM closed_market_variables WHERE market_id = :mid"),
                {"mid": market_id},
            )
            trade_result = session.execute(
                text("DELETE FROM trades WHERE market_id = :mid"),
                {"mid": market_id},
            )
            session.commit()

        processing_time_ms = int((time.time() - start_time) * 1000)
        logger.info(
            f"T4: cleanup_market done for market_id={market_id} "
            f"cmv_rows={cmv_result.rowcount} trade_rows={trade_result.rowcount} "
            f"time={processing_time_ms}ms"
        )
        return {
            "success": True,
            "task": "cleanup_market",
            "market_id": market_id,
            "cmv_rows_deleted": cmv_result.rowcount,
            "trade_rows_deleted": trade_result.rowcount,
            "processing_time_ms": processing_time_ms,
        }
    except Exception as e:
        logger.error(f"T4: cleanup_market failed for {market_id}: {e}", exc_info=True)
        raise self.retry(exc=e, countdown=2 ** self.request.retries)


@app.task(bind=True, max_retries=3)
def run_market_inference(self, market_id: str) -> dict:
    """
    Task D: calls the R/Plumber inference service (Motor de Cálculo) for a market.

    The inference service:
      1. Reads closed_market_variables for this market
      2. Fits distributions and detects statistical outliers (insiders)
      3. POSTs results to the REST API (E) → /insider-detection

    INFERENCE_URL env var (default: http://inference:8000).
    Retries up to 3 times with exponential backoff (30s, 60s, 120s).
    """
    inference_url = os.environ.get("INFERENCE_URL", "http://inference:8000") + "/analyze"

    try:
        response = httpx.post(
            inference_url,
            json={"market_id": market_id},
            timeout=180.0,  # R analysis may take a while on large datasets
        )
        response.raise_for_status()
        result = response.json()

        # R/Plumber may return scalars as single-element arrays; normalise to str
        raw_status = result.get("status")
        status_str = raw_status[0] if isinstance(raw_status, list) else raw_status

        insiders_found = len(result.get("insiders", []))
        logger.info(
            f"Inference completed for market_id={market_id}: "
            f"status={status_str}, insiders={insiders_found}"
        )

        return {
            "market_id":         market_id,
            "success":           True,
            "inference_status":  status_str,
            "insiders_found":    insiders_found,
        }

    except Exception as e:
        logger.error(f"Inference failed for market_id={market_id}: {e}", exc_info=True)
        raise self.retry(exc=e, countdown=30 * (2 ** self.request.retries))


@app.task(bind=True, max_retries=3)
def publish_insider_command(self, command_dict: dict) -> dict:
    """
    Task T3: Publishes an InsiderTradeCommand to Redis stream `insider_commands`.
    Called after the API updates the wallets table.
    Each invocation publishes one command for one wallet.

    XADD insider_commands * payload '<json>'
    """
    import json
    import redis

    wallet = command_dict.get("wallet")
    logger.info(
        f"T3: Publishing insider command for wallet={wallet}, "
        f"type={command_dict.get('command_type')}"
    )

    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    client = redis.from_url(redis_url)
    try:
        payload = json.dumps(command_dict)
        msg_id = client.xadd("insider_commands", {"payload": payload})
        msg_id_str = msg_id.decode() if isinstance(msg_id, bytes) else str(msg_id)

        logger.info(f"T3: Published msg_id={msg_id_str} for wallet={wallet}")
        return {
            "success": True,
            "msg_id": msg_id_str,
            "wallet": wallet,
            "command_type": command_dict.get("command_type"),
        }
    except Exception as e:
        logger.error(f"T3: XADD failed for wallet={wallet}: {e}", exc_info=True)
        raise self.retry(exc=e, countdown=2 ** self.request.retries)
    finally:
        client.close()