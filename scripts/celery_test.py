import uuid
from workers.tasks import calculate_i_score
from datetime import datetime, timedelta, UTC

def generate_mock_job(description, size, price, hours_before, winning, outcome="YES"):
    # Tiempos simulados
    end_date = datetime.now(UTC)
    trade_time = end_date - timedelta(hours=hours_before)
    wallet_first_tx = end_date - timedelta(days=100) # Cuenta con 100 días de edad

    return {
        "job_id": str(uuid.uuid4()),
        "payload": {
            "market": {
                "market_id": f"mkt_{description[:5]}",
                "question": f"Test: {description}",
                "end_date": end_date.isoformat().replace("+00:00", "Z"),
                "closed": True,
                "winning_outcome": "YES" if winning else "NO",
                "outcome_prices": [0.95, 0.05] if description == "Insider" else [0.5, 0.5],
                "volume_usdc": 1000000
            },
            "trade": {
                "id": 1,
                "wallet": "0xTester",
                "outcome": outcome,
                "price": price,
                "size_usdc": size,
                "timestamp": trade_time.isoformat().replace("+00:00", "Z")
            },
            "wallet_age": wallet_first_tx.isoformat().replace("+00:00", "Z")
        }
    }

# 🚀 Lanzar pruebas
test_cases = [
    generate_mock_job("Insider", size=50000, price=0.90, hours_before=1, winning=True),
    generate_mock_job("Normal", size=100, price=0.50, hours_before=48, winning=True),
    generate_mock_job("Error", size=500, price=0.30, hours_before=10, winning=False)
]

print("📡 Enviando trabajos a Celery...")
for case in test_cases:
    calculate_i_score.delay(case)
    print(f"✔️ Enviado: {case['payload']['market']['question']}")