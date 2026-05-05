#!/usr/bin/env python3
"""
Script básico para probar el funcionamiento del sistema de colas.

Este script verifica:
1. Producer: Publicar un MarketDataBatch
2. Orchestrator: Convertir batch en jobs
3. Consumer: Consumir y procesar jobs
4. Healthchecks: Conectividad Redis

Requiere: Redis corriendo en localhost:6379
"""

import asyncio
import sys
from datetime import datetime, timedelta
from redis.asyncio import Redis
from uuid import uuid4

# Imports del sistema de colas
from queue_system import RedisProducer, RedisConsumer, QueueOrchestrator
from queue_system.config import QueueConfig
from queue_system.streams import RawDataStream, JobsStream
from queue_system.scheduler import JobCreator
from queue_system.reliability import RetryHandler, DeadLetterQueue

# Imports de contratos
from contracts.raw_market_data import (
    MarketDataBatch,
    RawMarketMetadata,
    RawTrade,
    RawWalletAge
)
from contracts.processing_job import JobType, JobResult


def print_header(text: str):
    """Imprime un encabezado decorado"""
    print(f"\n{'='*60}")
    print(f"  {text}")
    print(f"{'='*60}\n")


def print_step(step: int, text: str):
    """Imprime un paso numerado"""
    print(f"[Paso {step}] {text}")


def print_success(text: str):
    """Imprime mensaje de éxito"""
    print(f"  ✓ {text}")


def print_error(text: str):
    """Imprime mensaje de error"""
    print(f"  ✗ {text}")


def print_info(text: str):
    """Imprime información"""
    print(f"  → {text}")


def create_test_batch() -> MarketDataBatch:
    """Crea un MarketDataBatch de prueba con datos sintéticos"""
    now = datetime.utcnow()

    metadata = RawMarketMetadata(
        market_id="test-market-12345",
        question="¿Funcionará el sistema de colas?",
        end_date=now + timedelta(days=7),
        closed=False,
        outcome_prices=None,
        volume_usdc=10000.0
    )

    trades = [
        RawTrade(
            tx_hash=f"0x{'a'*64}",
            wallet="0x1234567890abcdef1234567890abcdef12345678",
            market_id=metadata.market_id,
            timestamp=now - timedelta(hours=2),
            side="BUY",
            outcome="Yes",
            price=0.65,
            size_usdc=100.0,
            size_tokens=153.85
        ),
        RawTrade(
            tx_hash=f"0x{'b'*64}",
            wallet="0xabcdefabcdefabcdefabcdefabcdefabcdefabcd",
            market_id=metadata.market_id,
            timestamp=now - timedelta(hours=1),
            side="SELL",
            outcome="No",
            price=0.45,
            size_usdc=200.0,
            size_tokens=444.44
        ),
        RawTrade(
            tx_hash=f"0x{'c'*64}",
            wallet="0x9876543210987654321098765432109876543210",
            market_id=metadata.market_id,
            timestamp=now - timedelta(minutes=30),
            side="BUY",
            outcome="Yes",
            price=0.70,
            size_usdc=50.0,
            size_tokens=71.43
        )
    ]

    wallet_ages = {
        "0x1234567890abcdef1234567890abcdef12345678": RawWalletAge(
            wallet="0x1234567890abcdef1234567890abcdef12345678",
            first_tx_polymarket=now - timedelta(days=90),
            first_tx_polygon=now - timedelta(days=365)
        ),
        "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd": RawWalletAge(
            wallet="0xabcdefabcdefabcdefabcdefabcdefabcdefabcd",
            first_tx_polymarket=now - timedelta(days=30),
            first_tx_polygon=now - timedelta(days=180)
        ),
        "0x9876543210987654321098765432109876543210": RawWalletAge(
            wallet="0x9876543210987654321098765432109876543210",
            first_tx_polymarket=now - timedelta(days=5),
            first_tx_polygon=now - timedelta(days=10)
        )
    }

    return MarketDataBatch(
        metadata=metadata,
        trades=trades,
        wallet_ages=wallet_ages,
        ingested_at=now
    )


async def test_producer(redis: Redis, config: QueueConfig) -> tuple[bool, str]:
    """
    Test 1: Producer publica un batch
    Retorna: (success, batch_id)
    """
    print_step(1, "Probando Producer (publicar MarketDataBatch)")

    try:
        # Crear batch de prueba
        batch = create_test_batch()
        print_info(f"Batch creado con {len(batch.trades)} trades")

        # Crear RawDataStream y Producer manualmente
        raw_stream = RawDataStream(redis, config.raw_data_stream_name)
        producer = RedisProducer(raw_stream)

        # Health check
        is_healthy = await producer.health_check()
        if not is_healthy:
            print_error("Producer health check falló")
            return False, ""
        print_success("Producer health check OK")

        # Publicar batch
        batch_id = await producer.publish_market_batch(batch)
        print_success(f"Batch publicado con ID: {batch_id}")

        # Verificar que el batch está en el stream
        raw_stream = RawDataStream(redis, config.raw_data_stream_name)
        stream_length = await raw_stream.get_stream_length()
        print_info(f"Stream 'raw_data' tiene {stream_length} mensaje(s)")

        return True, batch_id

    except Exception as e:
        print_error(f"Error en Producer: {e}")
        return False, ""


async def test_orchestrator(redis: Redis, config: QueueConfig) -> tuple[bool, int]:
    """
    Test 2: Orchestrator consume batch y crea jobs
    Retorna: (success, num_jobs_created)
    """
    print_step(2, "Probando Orchestrator (batch → jobs)")

    try:
        # Crear componentes
        raw_stream = RawDataStream(redis, config.raw_data_stream_name)
        jobs_stream = JobsStream(redis, config.jobs_stream_name)
        job_creator = JobCreator()

        # Crear consumer group si no existe
        await raw_stream.create_consumer_group("test_scheduler", "0")
        print_success("Consumer group 'test_scheduler' creado/verificado")

        # Crear orchestrator
        orchestrator = QueueOrchestrator(
            raw_data_stream=raw_stream,
            jobs_stream=jobs_stream,
            job_creator=job_creator,
            config=config,
            consumer_group="test_scheduler",
            consumer_name="test_orchestrator"
        )

        # Setup orchestrator (crear consumer groups)
        await orchestrator.setup()

        # Leer y procesar manualmente un batch del stream
        print_info("Procesando batch...")
        batches = await raw_stream.read(
            consumer_group="test_scheduler",
            consumer_name="test_orchestrator",
            count=1,
            block_ms=2000
        )

        if batches:
            message_id, batch = batches[0]
            await orchestrator._process_batch(message_id, batch)
            print_success("Batch procesado exitosamente")
        else:
            print_error("No se encontró ningún batch en el stream")
            return False, 0

        # Verificar jobs creados
        stream_length = await jobs_stream.get_stream_length()
        print_success(f"{stream_length} job(s) creado(s) en 'jobs_stream'")

        # Obtener métricas
        metrics = await orchestrator.get_metrics()
        print_info(f"Métricas: batches={metrics['batches_processed']}, "
                  f"jobs={metrics['jobs_created']}, errors={metrics['errors']}")

        return True, stream_length

    except Exception as e:
        print_error(f"Error en Orchestrator: {e}")
        return False, 0


async def test_consumer(redis: Redis, config: QueueConfig, expected_jobs: int) -> bool:
    """
    Test 3: Consumer consume y procesa jobs
    Retorna: success
    """
    print_step(3, "Probando Consumer (consumir y procesar jobs)")

    try:
        # Crear componentes
        jobs_stream = JobsStream(redis, config.jobs_stream_name)
        retry_handler = RetryHandler(config)
        dlq = DeadLetterQueue(redis)

        # Crear consumer group si no existe
        await jobs_stream.create_consumer_group()
        print_success("Consumer group 'workers' creado/verificado")

        # Crear consumer
        consumer = RedisConsumer(jobs_stream, retry_handler, dlq, config)

        # Procesar todos los jobs disponibles
        jobs_processed = 0
        worker_id = f"test_worker_{uuid4().hex[:8]}"

        print_info(f"Worker ID: {worker_id}")
        print_info(f"Esperando procesar {expected_jobs} job(s)...")

        while jobs_processed < expected_jobs:
            # Obtener siguiente job (timeout 2s)
            job = await consumer.get_next_job(worker_id, timeout_ms=2000)

            if job is None:
                print_info("No hay más jobs disponibles")
                break

            print_info(f"Job recibido: tipo={job.job_type}, id={job.job_id[:16]}...")

            # Simular procesamiento exitoso
            await asyncio.sleep(0.1)

            # Crear resultado del job
            result = JobResult(
                job_id=job.job_id,
                success=True,
                error_message=None,
                scores_calculated=1,
                alerts_generated=0,
                processing_time_ms=100
            )

            # Acknowledge job
            await consumer.ack_job(job.job_id, result)
            jobs_processed += 1
            print_success(f"Job {jobs_processed}/{expected_jobs} procesado y confirmado")

        # Verificar pending count
        pending = await consumer.get_pending_count()
        print_info(f"Jobs pendientes en cola: {pending}")

        if jobs_processed == expected_jobs:
            print_success(f"Todos los {jobs_processed} jobs procesados correctamente")
            return True
        else:
            print_error(f"Solo se procesaron {jobs_processed}/{expected_jobs} jobs")
            return False

    except Exception as e:
        print_error(f"Error en Consumer: {e}")
        return False


async def cleanup_test_streams(redis: Redis, config: QueueConfig):
    """Limpia los streams de prueba"""
    print_step(4, "Limpiando streams de prueba")

    try:
        # Eliminar streams
        await redis.delete(config.raw_data_stream_name)
        await redis.delete(config.jobs_stream_name)
        await redis.delete("dead_letter_queue")

        print_success("Streams limpiados correctamente")

    except Exception as e:
        print_error(f"Error al limpiar: {e}")


async def main():
    """Función principal del test"""
    print_header("TEST BÁSICO DEL SISTEMA DE COLAS")

    # Configuración
    config = QueueConfig()
    print_info(f"Redis URL: {config.redis_url}")
    print_info(f"Raw data stream: {config.raw_data_stream_name}")
    print_info(f"Jobs stream: {config.jobs_stream_name}")

    # Conectar a Redis
    redis = None
    try:
        print_info("Conectando a Redis...")
        redis = Redis.from_url(config.redis_url, decode_responses=False)

        # Verificar conexión
        await redis.ping()
        print_success("Conexión a Redis establecida\n")

    except Exception as e:
        print_error(f"No se puede conectar a Redis: {e}")
        print_info("Asegúrate de que Redis esté corriendo en localhost:6379")
        return 1

    # Variables de resultado
    all_tests_passed = True
    batch_id = ""
    num_jobs = 0

    try:
        # Test 1: Producer
        success, batch_id = await test_producer(redis, config)
        all_tests_passed &= success

        if not success:
            print_error("Test de Producer falló, abortando tests restantes")
            return 1

        # Test 2: Orchestrator
        success, num_jobs = await test_orchestrator(redis, config)
        all_tests_passed &= success

        if not success or num_jobs == 0:
            print_error("Test de Orchestrator falló, abortando tests restantes")
            return 1

        # Test 3: Consumer
        success = await test_consumer(redis, config, num_jobs)
        all_tests_passed &= success

        # Resumen final
        print_header("RESUMEN DE PRUEBAS")

        if all_tests_passed:
            print_success("TODOS LOS TESTS PASARON")
            print_info(f"Batch ID: {batch_id}")
            print_info(f"Jobs creados: {num_jobs}")
            print_info(f"Jobs procesados: {num_jobs}")
            print("\n¡El sistema de colas funciona correctamente!\n")
            return 0
        else:
            print_error("ALGUNOS TESTS FALLARON")
            return 1

    except Exception as e:
        print_error(f"Error inesperado: {e}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        # Cleanup
        if redis:
            await cleanup_test_streams(redis, config)
            await redis.close()
            print_info("Conexión Redis cerrada")


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\nTest interrumpido por el usuario")
        sys.exit(130)
