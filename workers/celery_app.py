import os
from celery import Celery

app = Celery(
    'processing_provider',
    broker=os.environ.get("REDIS_URL", "redis://localhost:6379/0"),
    backend=os.environ.get("REDIS_URL", "redis://localhost:6379/0"),
    include=['workers.tasks'],
)

app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    worker_pool='prefork',

    task_prefetch_multiplier=4, # Number of "reserved tasks". Set to 1 if tasks are heavy
    worker_max_tasks_per_child=200, # Set to 10-20 if tasks are heavy
    worker_max_memory_per_child=1000000, # 1 GB of RAM
    task_soft_time_limit=10, # 10 seconds to finish the task, or else cleanup
    task_time_limit=15, # 15 seconds hard limit before killing the process

    # Reliability
    task_acks_late = True,
    task_reject_on_worker_lost = True,
    broker_connection_retry_on_startup = True,
)

import workers.logging_config
import workers.logging