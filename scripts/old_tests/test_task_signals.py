#!/usr/bin/env python3
"""
Script para probar que los logs de task_success se escriben en archivo.
"""
import time
from workers.tasks import compute_variables

print("=" * 80)
print("PRUEBA: Enviando tarea para verificar logs de señales")
print("=" * 80)
print("\nPREREQUISITO: Worker debe estar corriendo:")
print("  celery -A workers.celery_app worker --loglevel=info\n")

# Enviar tarea
print("Enviando tarea...")
task = compute_variables.delay("market_test_123")
print(f"Tarea enviada: {task.id}")

try:
    result = task.get(timeout=10)
    print(f"\n✓ Tarea completada: {result}")

    # Esperar un momento para que se escriba el archivo
    time.sleep(1)

    print("\n" + "=" * 80)
    print("VERIFICACIÓN DE LOGS:")
    print("=" * 80)

    # Leer archivo de log
    import glob
    log_files = glob.glob("logs/celery_*.log")
    if log_files:
        print(f"\nContenido de {log_files[0]}:")
        with open(log_files[0], 'r') as f:
            content = f.read()
            if content:
                print(content)
            else:
                print("⚠️  ARCHIVO VACÍO - Los logs NO se están escribiendo")
    else:
        print("⚠️  No se encontró archivo de log")

except Exception as e:
    print(f"\nError: {e}")
