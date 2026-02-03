import os
import logging
import socket
from logging.handlers import TimedRotatingFileHandler
from celery.signals import after_setup_task_logger


@after_setup_task_logger.connect
def setup_task_logger(logger, *args, **kwargs):
    # Route to keep the logs
    log_dir = "/app/logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

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

    formatter = logging.Formatter(
        '[%(asctime)s] %(levelname)s - %(processName)s - [%(hostname)s] - %(message)s'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)