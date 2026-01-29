# Queue interfaces for cross-persona communication

from queue.interfaces.producer import QueueProducer
from queue.interfaces.consumer import QueueConsumer

__all__ = [
    "QueueProducer",
    "QueueConsumer",
]
