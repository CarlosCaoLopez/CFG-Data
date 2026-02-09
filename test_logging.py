#!/usr/bin/env python3
"""
Script de prueba para verificar que el logging de Celery funciona correctamente.
Envía la tarea compute_variables a través de Celery.

PREREQUISITO: Debes tener un worker de Celery corriendo:
    celery -A workers.celery_app worker --loglevel=info
"""
import time
from workers.tasks import compute_variables

if __name__ == "__main__":
    print("=" * 80)
    print("TEST: Enviando tarea compute_variables a través de Celery")
    print("=" * 80)
    print("\nAsegúrate de tener un worker corriendo:")
    print("  celery -A workers.celery_app worker --loglevel=info\n")

    # Enviar la tarea a través de Celery
    print("Enviando tarea...")
    task = compute_variables.delay("test_market_123")

    print(f"Tarea enviada con ID: {task.id}")
    print("Esperando resultado...")

    # Esperar hasta 10 segundos
    try:
        result = task.get(timeout=10)
        print("\n" + "=" * 80)
        print("RESULTADO:")
        print(result)
        print("=" * 80)
        print("\nREVISA:")
        print("1. La consola del worker debe mostrar los logs")
        print("2. El archivo logs/celery_*.log debe contener los logs")
    except Exception as e:
        print(f"\nError: {e}")
        print("\nAsegúrate de que:")
        print("1. Redis está corriendo (docker-compose up -d redis)")
        print("2. PostgreSQL está corriendo (docker-compose up -d postgres)")
        print("3. Celery worker está corriendo")
        print("=" * 80)
