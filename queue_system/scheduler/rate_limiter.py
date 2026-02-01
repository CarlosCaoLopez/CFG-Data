# Rate Limiter - Prevents worker saturation
# Global (Redis-based) token bucket rate limiting for jobs_stream

import asyncio
import structlog
import time
from typing import Optional
from redis.asyncio import Redis

logger = structlog.get_logger(__name__)


class RateLimiter:
    """
    Global rate limiter using token bucket algorithm with Redis-based state.

    Coordinates rate limiting across multiple workers to prevent system saturation.
    Uses Redis for distributed token bucket state, ensuring consistent rate limits
    even with multiple producer processes.
    """

    def __init__(
        self,
        redis_client: Redis,
        max_jobs_per_second: float,
        burst_size: int,
        key_prefix: str = "rate_limiter:jobs_stream"
    ):
        """
        Initialize rate limiter with token bucket parameters.

        Args:
            redis_client: Async Redis client for distributed state
            max_jobs_per_second: Maximum sustained rate (tokens per second)
            burst_size: Maximum tokens available for bursts
            key_prefix: Redis key prefix for rate limiter state
        """
        self.redis = redis_client
        self.max_rate = max_jobs_per_second
        self.burst_size = burst_size
        self.key_prefix = key_prefix

        # Redis keys for state
        self.tokens_key = f"{key_prefix}:tokens"
        self.last_refill_key = f"{key_prefix}:last_refill"

        logger.info(
            "rate_limiter_initialized",
            max_jobs_per_second=max_jobs_per_second,
            burst_size=burst_size,
            key_prefix=key_prefix
        )

    async def acquire(self, tokens: int = 1) -> bool:
        """
        Try to acquire tokens from the bucket (non-blocking).

        Args:
            tokens: Number of tokens to acquire (default 1 job = 1 token)

        Returns:
            True if tokens were acquired, False if insufficient capacity
        """
        # Refill tokens based on elapsed time
        await self._refill_tokens()

        # Get current token count
        current_tokens = await self._get_current_tokens()

        if current_tokens >= tokens:
            # Consume tokens
            new_tokens = current_tokens - tokens
            await self.redis.set(
                self.tokens_key,
                str(new_tokens),
                ex=3600  # Expire after 1 hour of inactivity
            )

            logger.debug(
                "tokens_acquired",
                tokens_acquired=tokens,
                remaining_tokens=new_tokens
            )
            return True
        else:
            logger.debug(
                "tokens_unavailable",
                tokens_requested=tokens,
                current_tokens=current_tokens
            )
            return False

    async def wait_for_capacity(self, tokens: int = 1, timeout: Optional[float] = None) -> bool:
        """
        Block until tokens are available or timeout is reached.

        Args:
            tokens: Number of tokens to acquire
            timeout: Maximum time to wait in seconds (None = wait forever)

        Returns:
            True if tokens were acquired, False if timeout reached
        """
        start_time = time.time()

        while True:
            # Try to acquire tokens
            if await self.acquire(tokens):
                wait_time = time.time() - start_time
                logger.debug(
                    "capacity_acquired",
                    tokens=tokens,
                    wait_time_seconds=round(wait_time, 2)
                )
                return True

            # Check timeout
            if timeout is not None:
                elapsed = time.time() - start_time
                if elapsed >= timeout:
                    logger.warning(
                        "capacity_timeout",
                        tokens_requested=tokens,
                        timeout_seconds=timeout
                    )
                    return False

            # Calculate sleep duration based on refill rate
            # Sleep for time needed to generate 1 token
            sleep_duration = min(1.0 / self.max_rate, 0.1)  # Cap at 100ms
            await asyncio.sleep(sleep_duration)

    async def get_current_rate(self) -> float:
        """
        Get the current instantaneous rate (for observability).

        This is an approximation based on recent token consumption.

        Returns:
            Estimated jobs per second
        """
        current_tokens = await self._get_current_tokens()

        # If tokens are at or near burst, rate is effectively 0
        if current_tokens >= self.burst_size * 0.9:
            return 0.0

        # Otherwise, approximate based on token deficit
        tokens_consumed = self.burst_size - current_tokens
        return min(tokens_consumed * self.max_rate / self.burst_size, self.max_rate)

    def get_available_capacity(self) -> int:
        """
        Get available tokens (synchronous, uses cached value).

        Note: This is a local approximation. Use acquire() for accurate check.

        Returns:
            Estimated available tokens
        """
        # This would require a sync Redis call, which is not ideal
        # For now, return burst_size as a safe approximation
        # In production, this could be enhanced with local caching
        return self.burst_size

    async def _refill_tokens(self) -> None:
        """
        Refill tokens based on elapsed time since last refill.

        Uses token bucket algorithm:
        - Tokens refill at max_rate per second
        - Capped at burst_size maximum
        """
        current_time = time.time()

        # Get last refill time (default to now if not set)
        last_refill_str = await self.redis.get(self.last_refill_key)
        if last_refill_str is None:
            # Initialize bucket with full capacity
            await self.redis.set(
                self.tokens_key,
                str(self.burst_size),
                ex=3600
            )
            await self.redis.set(
                self.last_refill_key,
                str(current_time),
                ex=3600
            )
            return

        last_refill = float(last_refill_str.decode() if isinstance(last_refill_str, bytes) else last_refill_str)
        elapsed = current_time - last_refill

        if elapsed <= 0:
            return

        # Calculate tokens to add
        tokens_to_add = elapsed * self.max_rate

        # Get current tokens
        current_tokens = await self._get_current_tokens()

        # Add tokens, capped at burst_size
        new_tokens = min(current_tokens + tokens_to_add, self.burst_size)

        # Update Redis state
        await self.redis.set(
            self.tokens_key,
            str(new_tokens),
            ex=3600
        )
        await self.redis.set(
            self.last_refill_key,
            str(current_time),
            ex=3600
        )

        if tokens_to_add > 0:
            logger.debug(
                "tokens_refilled",
                tokens_added=round(tokens_to_add, 2),
                new_total=round(new_tokens, 2),
                elapsed_seconds=round(elapsed, 2)
            )

    async def _get_current_tokens(self) -> float:
        """
        Get current token count from Redis.

        Returns:
            Current token count (defaults to burst_size if not set)
        """
        tokens_str = await self.redis.get(self.tokens_key)

        if tokens_str is None:
            # Initialize with full capacity
            await self.redis.set(
                self.tokens_key,
                str(self.burst_size),
                ex=3600
            )
            return self.burst_size

        return float(tokens_str.decode() if isinstance(tokens_str, bytes) else tokens_str)

    async def reset(self) -> None:
        """
        Reset rate limiter to full capacity.

        Useful for testing or manual intervention.
        """
        await self.redis.set(
            self.tokens_key,
            str(self.burst_size),
            ex=3600
        )
        await self.redis.set(
            self.last_refill_key,
            str(time.time()),
            ex=3600
        )

        logger.info(
            "rate_limiter_reset",
            tokens=self.burst_size
        )
