# Queue interfaces for cross-persona communication

from queue_system.interfaces.producer import QueueProducer
from queue_system.interfaces.consumer import QueueConsumer

__all__ = [
    "QueueProducer",
    "QueueConsumer",
]
