"""
Reset completo del sistema para arrancar en tiempo real desde cero.

Limpia:
  1. Datos de prueba del test_full_pipeline (IDs conocidos)
  2. Todos los datos reales de la BD (wallets, markets, trades, etc.)
  3. Redis streams (closed_markets, raw_data, insider_trades, insider_commands)
  4. Consumer groups de Redis

Uso:
  poetry run python -m scripts.reset_for_realtime
  poetry run python -m scripts.reset_for_realtime --only-test-data   # solo limpia IDs de prueba
  poetry run python -m scripts.reset_for_realtime --yes              # sin confirmación
"""

import argparse
import sys

import redis as redis_sync
from sqlalchemy import text

from database.connection import get_sync_session

# ──────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────

REDIS_URL = "redis://localhost:6379/0"

STREAMS = [
    "closed_markets",
    "raw_data",
    "insider_trades",
    "insider_commands",
    "jobs_stream",
    "results_stream",
]

# IDs inyectados por test_full_pipeline
TEST_WALLET         = "0x" + "ab" * 20   # 42 chars
TEST_MARKET_ID      = "0x" + "cd" * 32  # 66 chars
TEST_TRADE_ID       = "0x" + "ef" * 32  # 66 chars
INSIDER_TRADE_ID    = "0x" + "c"  * 64  # 66 chars

OK   = "\033[32m✔\033[0m"
FAIL = "\033[31m✘\033[0m"
INFO = "\033[36m→\033[0m"


# ──────────────────────────────────────────────
# DB helpers
# ──────────────────────────────────────────────

def delete_test_data(session) -> None:
    """Borra únicamente los registros con IDs de prueba conocidos."""
    print(f"\n{INFO} Borrando datos de prueba (IDs fijos del test_full_pipeline)...")

    steps = [
        ("closed_market_variables", f"market_id = '{TEST_MARKET_ID}'"),
        ("insiders_trades",         f"market_id = '{TEST_MARKET_ID}'"),
        ("trades",                  f"market_id = '{TEST_MARKET_ID}'"),
        ("markets",                 f"market_id = '{TEST_MARKET_ID}'"),
        ("wallets",                 f"address   = '{TEST_WALLET}'"),
    ]

    for table, condition in steps:
        result = session.execute(text(f"DELETE FROM {table} WHERE {condition}"))
        n = result.rowcount
        print(f"  {OK} {table}: {n} fila(s) eliminada(s)")


def delete_all_data(session) -> None:
    """Borra TODOS los datos de todas las tablas (orden FK correcto)."""
    print(f"\n{INFO} Borrando TODOS los datos de la BD...")

    tables = [
        "closed_market_variables",
        "insiders_trades",
        "trades",
        "alerts",
        "markets",
        "wallets",
    ]

    for table in tables:
        try:
            session.execute(text("SAVEPOINT sp"))
            result = session.execute(text(f"DELETE FROM {table}"))
            n = result.rowcount
            session.execute(text("RELEASE SAVEPOINT sp"))
            print(f"  {OK} {table}: {n} fila(s) eliminada(s)")
        except Exception as e:
            session.execute(text("ROLLBACK TO SAVEPOINT sp"))
            print(f"  {FAIL} {table}: {e}")


# ──────────────────────────────────────────────
# Redis helpers
# ──────────────────────────────────────────────

def reset_redis(r: redis_sync.Redis) -> None:
    """Elimina todos los streams del sistema."""
    print(f"\n{INFO} Limpiando Redis streams...")

    for stream in STREAMS:
        try:
            existed = r.exists(stream)
            if existed:
                r.delete(stream)
                print(f"  {OK} stream '{stream}' eliminado")
            else:
                print(f"  {INFO} stream '{stream}' no existía")
        except Exception as e:
            print(f"  {FAIL} stream '{stream}': {e}")

    # Limpiar también colas internas de Celery en Redis
    celery_keys = r.keys("celery*")
    if celery_keys:
        r.delete(*celery_keys)
        print(f"  {OK} {len(celery_keys)} clave(s) de Celery eliminadas")
    else:
        print(f"  {INFO} no había claves de Celery")


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Reset del sistema para arrancar en tiempo real")
    parser.add_argument(
        "--only-test-data",
        action="store_true",
        help="Borra solo los IDs de prueba del test_full_pipeline (no toca datos reales)",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Salta la confirmación interactiva",
    )
    args = parser.parse_args()

    # ── Confirmación ──
    if not args.yes:
        if args.only_test_data:
            msg = "Esto borrará los datos de prueba del test_full_pipeline y limpiará Redis."
        else:
            msg = (
                "⚠️  Esto borrará TODOS los datos de la BD y los streams de Redis.\n"
                "   No hay vuelta atrás. ¿Continuar?"
            )
        print(f"\n{msg}")
        resp = input("  Escribe 'si' para continuar: ").strip().lower()
        if resp != "si":
            print("Cancelado.")
            sys.exit(0)

    # ── BD ──
    try:
        with get_sync_session() as session:
            if args.only_test_data:
                delete_test_data(session)
            else:
                delete_all_data(session)
        print(f"\n  {OK} BD limpiada correctamente")
    except Exception as e:
        print(f"\n  {FAIL} Error conectando a la BD: {e}")
        print("     ¿Está postgres corriendo? (docker-compose up -d postgres)")
        sys.exit(1)

    # ── Redis ──
    try:
        r = redis_sync.from_url(REDIS_URL, decode_responses=True)
        r.ping()
        reset_redis(r)
        print(f"\n  {OK} Redis limpiado correctamente")
    except Exception as e:
        print(f"\n  {FAIL} Error conectando a Redis: {e}")
        print("     ¿Está Redis corriendo? (docker-compose up -d redis)")
        sys.exit(1)

    # ── Resumen ──
    print("\n" + "─" * 50)
    print("  Sistema reseteado. Para arrancar en tiempo real:")
    print()
    print("  docker-compose up -d redis postgres orchestrator worker beat api inference ingestion")
    print()
    print("  Y para ver los logs:")
    print("  docker-compose logs -f ingestion orchestrator worker")
    print("─" * 50 + "\n")


if __name__ == "__main__":
    main()
