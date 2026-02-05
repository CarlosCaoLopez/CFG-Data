import os
import time
import math
# import asyncio  # Commented out - not used after removing score persistence
from dotenv import load_dotenv
from datetime import datetime
from workers.celery_app import app
from contracts.processing_job import JobResult

# from database.connection import async_session  # Commented out - not used after removing score persistence
# from database.manager import DatabaseManager  # Commented out - not used after removing score persistence


@app.task(bind=True, max_retries=3)
def calculate_i_score(self, job_dict: dict) -> JobResult:
    """
    Celery main task.
    We have a dictionary because Celery serializes to JSON by default
    """
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
    p_market = float(trade.get("prize"))
    delta_elo = float(os.environ.get("ELO_K_FACTOR")) * math.log(1 + trade_volume) * (is_win - p_market)

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