from celery.signals import task_success, task_failure
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

@task_success.connect
def on_task_success(calculate_i_score, result, **kwargs):
    logger.info(f"✅ TAREA COMPLETADA: {calculate_i_score.name} | RESULTADO: {result}")

@task_failure.connect
def on_task_failure(calculate_i_score, exception, **kwargs):
    logger.error(f"❌ TAREA FALLIDA: {calculate_i_score.name} | ERROR: {exception}")