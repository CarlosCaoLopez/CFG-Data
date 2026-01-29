# Worker - Main event loop for job processing
import os
import time
from multiprocessing import Process, cpu_count
from datetime import datetime
from typing import List

from contracts.processing_job import JobType, JobPriority, ProcessingJob, JobResult, WalletScore, Alert
from _queue.interfaces.consumer import QueueConsumer
from concurrent.futures import ProcessPoolExecutor, TimeoutError

def calculate_i_score(job: ProcessingJob) -> (List[WalletScore], JobResult):
    """
    Simulación de la lógica de cálculo.
    Aquí es donde procesarías el payload y generarías los scores.
    """
    start_time = time.time()

    # --- ESPACIO PARA LA LÓGICA REAL ---
    # Simulamos que procesamos N registros del payload
    num_to_process = job.payload.get("count", 10)

    # Placeholder de salida
    results = []
    # Aquí iría la lógica de: i_score = delta_elo * ln(1+V) ...

    end_time = time.time()
    processing_time_ms = int((end_time - start_time) * 1000)

    # Construimos el resultado del contrato
    job_result = JobResult(
        job_id=job.job_id,
        success=True,
        scores_calculated=num_to_process,
        alerts_generated=1 if num_to_process > 5 else 0, # Ejemplo trivial
        processing_time_ms=processing_time_ms
    )

    return results, job_result


def worker_process(job_queue: Queue, result_queue: Queue, worker_id: int):
    """Bucle infinito del proceso worker"""
    print(f"[Worker {worker_id}] Iniciado y esperando tareas...")

    while True:
        # Obtener trabajo de la cola
        job: ProcessingJob = job_queue.get()

        # Sentinel (Poison Pill) para cerrar el worker
        if job is None:
            print(f"[Worker {worker_id}] Finalizando...")
            break

        try:
            if job.job_type == JobType.CALCULATE_SCORES:
                print(f"[Worker {worker_id}] Procesando Job {job.job_id}...")
                scores, summary = calculate_scores_logic(job)

                # Enviamos el resumen a la cola de resultados
                result_queue.put(summary)
            else:
                print(f"[Worker {worker_id}] Tipo de job no soportado: {job.job_type}")

        except Exception as e:
            result_queue.put(JobResult(
                job_id=job.job_id,
                success=False,
                error_message=str(e),
                processing_time_ms=0
            ))


class WorkerManager:
    def __init__(self):
        # Configuración por variable de entorno
        self.num_workers = int(os.environ.get("NUM_WORKERS", cpu_count()))
        self.processes = []
        self.my_queue = () # De donde saco la cola ???????????????????????????????????????????????????????????????????????????????????????????????????

    def start_workers(self):
        with ProcessPoolExecutor() as executor:
            futures = [executor.submit(calculate_i_score, my_queue) for _ in self.num_workers]

            for future in futures:
                try:
                    result = future.result(timeout=10)
                    print(f"[*] Resultado recibido: Success={result.success} | Time={result.processing_time_ms}ms")
                    my_queue.ack_job(result.job_id, result)
                except Exception as e:
                    print("[*] El trabajo tardó demasiado o falló.")
                    my_queue.nack_job(result.job_id, str(e))




if __name__ == "__main__":
    manager = WorkerManager()
    manager.start_workers()

    # Escuchar resultados (en una app real esto podría ser un hilo aparte)
    try:
        result = manager.result_queue.get(timeout=10)
        print(f"[Main] Resultado recibido: Success={result.success} | Time={result.processing_time_ms}ms")
    except Exception as e:
        print("[Main] El trabajo tardó demasiado o falló.")