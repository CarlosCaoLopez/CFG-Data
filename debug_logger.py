#!/usr/bin/env python3
"""
Script para debuggear el estado del logger de Celery.
Ejecutar DESPUÉS de que el worker haya procesado al menos una tarea.
"""
import time
from workers.tasks import compute_variables

print("=" * 80)
print("DEBUG: Verificando configuración de logger")
print("=" * 80)

# Enviar una tarea
print("\n1. Enviando tarea...")
task = compute_variables.delay("debug_market_123")
print(f"   Tarea enviada: {task.id}")

# Esperar resultado
try:
    result = task.get(timeout=10)
    print(f"\n2. Tarea completada: {result}")
except Exception as e:
    print(f"\n2. Error en tarea: {e}")

# Esperar un momento para que se procesen las señales
time.sleep(2)

# Verificar el logger configurado
print("\n3. Verificando logger configurado...")
from workers import logging_config

if logging_config._configured_logger:
    logger = logging_config._configured_logger
    print(f"   ✓ Logger encontrado: {logger.name}")
    print(f"   ✓ Nivel: {logger.level} ({logger.level == 20 and 'INFO' or 'OTRO'})")
    print(f"   ✓ Número de handlers: {len(logger.handlers)}")

    for i, handler in enumerate(logger.handlers):
        print(f"\n   Handler [{i}]: {type(handler).__name__}")
        print(f"      Nivel: {handler.level}")
        if hasattr(handler, 'baseFilename'):
            print(f"      Archivo: {handler.baseFilename}")
        if hasattr(handler, 'stream'):
            print(f"      Stream: {handler.stream.name if hasattr(handler.stream, 'name') else handler.stream}")
else:
    print("   ✗ _configured_logger es None")

# Verificar archivos de log
print("\n4. Verificando archivos de log...")
import glob
log_files = glob.glob("logs/celery_*.log")
if log_files:
    for log_file in log_files:
        import os
        size = os.path.getsize(log_file)
        print(f"   {log_file}: {size} bytes")

        if size > 0:
            print(f"\n   Últimas 10 líneas de {log_file}:")
            with open(log_file, 'r') as f:
                lines = f.readlines()
                for line in lines[-10:]:
                    print(f"      {line.rstrip()}")
        else:
            print(f"      ⚠️  Archivo vacío")
else:
    print("   ⚠️  No se encontraron archivos de log")

print("\n" + "=" * 80)
