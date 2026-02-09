import os
import logging
import socket
from logging.handlers import TimedRotatingFileHandler
from celery.signals import after_setup_task_logger, task_success, task_failure

# Esta variable guardará el logger configurado para usarlo en las señales
_configured_logger = None

class HostnameFilter(logging.Filter):
    def __init__(self):
        self.hostname = socket.gethostname()
    def filter(self, record):
        record.hostname = self.hostname
        return True

@after_setup_task_logger.connect
def setup_task_logger(logger, *args, **kwargs):
    """
    Configura el logger de Celery para escribir logs tanto en consola como en archivo.
    Se conecta a la señal after_setup_task_logger que se ejecuta después de que Celery
    configura su logger por defecto.
    """
    # Set the logger level to INFO to ensure logs are processed
    logger.setLevel(logging.INFO)

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

    # Create formatter
    formatter = logging.Formatter(
        '[%(asctime)s] %(levelname)s - %(processName)s - [%(hostname)s] - %(message)s'
    )

    # File handler - At midnight, change file every day, keep the last 30 days
    file_handler = TimedRotatingFileHandler(
        log_file,
        when="midnight",
        interval=1,
        backupCount=30,
        encoding='utf-8'
    )
    file_handler.addFilter(HostnameFilter())
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)

    # Check if file handler already exists to avoid duplicates
    if not any(isinstance(h, TimedRotatingFileHandler) for h in logger.handlers):
        logger.addHandler(file_handler)
        print(f"✓ Logger configurado: archivo={log_file}")

    # Ensure console handler exists (Celery should have added it, but verify)
    has_console_handler = any(
        isinstance(h, logging.StreamHandler) and h.stream.name in ['<stdout>', '<stderr>']
        for h in logger.handlers
    )

    if not has_console_handler:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        console_handler.setLevel(logging.INFO)
        logger.addHandler(console_handler)
        print(f"✓ Console handler agregado")

    # Don't propagate to root logger to avoid duplicate logs
    logger.propagate = False

    # Guardar referencia al logger configurado para usarlo en las señales
    global _configured_logger
    _configured_logger = logger

    print(f"✓ Logger configurado con {len(logger.handlers)} handlers")


@task_success.connect
def on_task_success(sender, result, **kwargs):
    if _configured_logger:
        task_name = getattr(sender, 'name', 'Unknown Task')
        _configured_logger.info(f"TAREA COMPLETADA: {task_name} | RESULTADO: {result}")

@task_failure.connect
def on_task_failure(sender, exception, **kwargs):
    if _configured_logger:
        task_name = getattr(sender, 'name', 'Unknown Task')
        _configured_logger.error(f"TAREA FALLIDA: {task_name} | ERROR: {exception}")