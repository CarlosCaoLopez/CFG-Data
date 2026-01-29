# Queue interfaces for cross-persona communication

from _queue.interfaces.producer import QueueProducer
from _queue.interfaces.consumer import QueueConsumer

__all__ = [
    "QueueProducer",
    "QueueConsumer",
]
