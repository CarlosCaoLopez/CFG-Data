# Queue interfaces for cross-persona communication

from job_queue.interfaces.producer import QueueProducer
from job_queue.interfaces.consumer import QueueConsumer

__all__ = [
    "QueueProducer",
    "QueueConsumer",
]
