# Queue interfaces for cross-persona communication

from queue_layer.interfaces.producer import QueueProducer
from queue_layer.interfaces.consumer import QueueConsumer

__all__ = [
    "QueueProducer",
    "QueueConsumer",
]
