import os
import time
import math
from dotenv import load_dotenv
from datetime import datetime
from workers.celery_app import app
from contracts.processing_job import JobResult


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
    p_market = float(trade.get("price"))
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

    I_score = float(os.environ.get("THETA_1")) * delta_elo + float(os.environ.get("THETA_2")) / delta_time + \
        float(os.environ.get("THETA_3")) / account_age + float(os.environ.get("THETA_4")) * math.exp(-H) * alpha



    #############################################################################################################
    # Aquí debería ir el insert a la base de datos, pero no sé si es buena idea hacer un insert por cada elemento
    # Igual es mejor hacerlo en batches o que se ocupe otro
    # También se podría devolver el I_score
    print(f"Trabajo {job_id}, {market.get("question")}: {I_score}")
    #############################################################################################################


    end_time = time.time()
    processing_time_ms = int((end_time - start_time) * 1000)

    return {
        "job_id": job_id,
        "success": True,
        "scores_calculated": 1,
        "processing_time_ms": processing_time_ms
    }