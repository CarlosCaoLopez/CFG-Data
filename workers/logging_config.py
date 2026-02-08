import os
import logging
import socket
from logging.handlers import TimedRotatingFileHandler
from celery.signals import after_setup_task_logger, task_success, task_failure
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

class HostnameFilter(logging.Filter):
    def __init__(self):
        self.hostname = socket.gethostname()
    def filter(self, record):
        record.hostname = self.hostname
        return True

@after_setup_task_logger.connect
def setup_task_logger(logger, *args, **kwargs):
    # Route to keep the logs
    log_dir = os.environ.get("LOG_DIR", "logs")

    if not os.path.exists(log_dir):
        try:
            os.makedirs(log_dir)
        except Exception as e:
            print(f"Failed to create directory {log_dir}: {e}")
            return

    # Get the container ID/hostname to avoid file collisions between replicas
    hostname = socket.gethostname()
    log_file = os.path.join(log_dir, f"celery_{hostname}.log")

    # At midnight, change file every day, keep the last 30 days
    handler = TimedRotatingFileHandler(
        log_file,
        when="midnight",
        interval=1,
        backupCount=30,
        encoding='utf-8'
    )
    handler.addFilter(HostnameFilter())

    formatter = logging.Formatter(
        '[%(asctime)s] %(levelname)s - %(processName)s - [%(hostname)s] - %(message)s'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)


@task_success.connect
def on_task_success(sender, result, **kwargs):
    task_name = getattr(sender, 'name', 'Unknown Task')
    logger.info(f"TAREA COMPLETADA: {task_name} | RESULTADO: {result}")

@task_failure.connect
def on_task_failure(sender, exception, **kwargs):
    task_name = getattr(sender, 'name', 'Unknown Task')
    logger.error(f"TAREA FALLIDA: {task_name} | ERROR: {exception}")