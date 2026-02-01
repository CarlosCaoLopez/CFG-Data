# Custom exceptions for queue operations

class QueueConnectionError(Exception):
    """Raised when Redis connection fails or times out"""
    pass


class JobNotFoundError(Exception):
    """Raised when a job ID is not found in internal tracking"""
    pass
