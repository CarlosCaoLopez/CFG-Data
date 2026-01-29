# HTTP Client with rate limiting and retry logic

import asyncio
import aiohttp
import structlog
from typing import Any
from enum import Enum

from ingestion.config import get_settings

logger = structlog.get_logger()


class APIType(Enum):
    """API types with different rate limits"""
    POLYMARKET = "polymarket"
    POLYGONSCAN = "polygonscan"


class RateLimiter:
    """Token bucket rate limiter for API requests"""

    def __init__(self, requests_per_second: float):
        self.min_interval = 1.0 / requests_per_second
        self.last_request_time = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Wait until a request can be made"""
        async with self._lock:
            now = asyncio.get_event_loop().time()
            time_since_last = now - self.last_request_time
            if time_since_last < self.min_interval:
                await asyncio.sleep(self.min_interval - time_since_last)
            self.last_request_time = asyncio.get_event_loop().time()


class HTTPClient:
    """
    Async HTTP client with:
    - Rate limiting per API type
    - Exponential backoff retry
    - Structured logging
    """

    def __init__(self):
        self.settings = get_settings()
        self._session: aiohttp.ClientSession | None = None

        # Rate limiters per API type
        self._rate_limiters = {
            APIType.POLYMARKET: RateLimiter(
                self.settings.polymarket_requests_per_minute / 60.0
            ),
            APIType.POLYGONSCAN: RateLimiter(
                self.settings.polygonscan_requests_per_second
            ),
        }

    async def _get_session(self) -> aiohttp.ClientSession:
        """Lazy session creation"""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self.settings.timeout_seconds)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def close(self) -> None:
        """Close the HTTP session"""
        if self._session and not self._session.closed:
            await self._session.close()

    async def get(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        api_type: APIType = APIType.POLYMARKET,
    ) -> dict[str, Any] | list[Any]:
        """
        Make a GET request with rate limiting and retry.

        Args:
            url: Full URL to request
            params: Query parameters
            api_type: Which API (for rate limiting)

        Returns:
            Parsed JSON response

        Raises:
            aiohttp.ClientError: After all retries exhausted
        """
        rate_limiter = self._rate_limiters[api_type]
        session = await self._get_session()

        last_exception: Exception | None = None

        for attempt in range(self.settings.max_retries):
            try:
                await rate_limiter.acquire()

                logger.debug(
                    "http_request",
                    url=url,
                    params=params,
                    attempt=attempt + 1,
                    api_type=api_type.value,
                )

                async with session.get(url, params=params) as response:
                    response.raise_for_status()
                    data = await response.json()

                    logger.debug(
                        "http_response",
                        url=url,
                        status=response.status,
                        api_type=api_type.value,
                    )

                    return data

            except aiohttp.ClientResponseError as e:
                last_exception = e
                if e.status == 429:  # Rate limited
                    delay = self._calculate_backoff(attempt, multiplier=2.0)
                    logger.warning(
                        "rate_limited",
                        url=url,
                        status=e.status,
                        retry_in=delay,
                    )
                    await asyncio.sleep(delay)
                elif e.status >= 500:  # Server error, retry
                    delay = self._calculate_backoff(attempt)
                    logger.warning(
                        "server_error",
                        url=url,
                        status=e.status,
                        retry_in=delay,
                    )
                    await asyncio.sleep(delay)
                else:  # Client error, don't retry
                    logger.error(
                        "client_error",
                        url=url,
                        status=e.status,
                        message=str(e),
                    )
                    raise

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                last_exception = e
                delay = self._calculate_backoff(attempt)
                logger.warning(
                    "request_failed",
                    url=url,
                    error=str(e),
                    attempt=attempt + 1,
                    retry_in=delay,
                )
                await asyncio.sleep(delay)

        # All retries exhausted
        logger.error(
            "all_retries_exhausted",
            url=url,
            attempts=self.settings.max_retries,
        )
        if last_exception:
            raise last_exception
        raise aiohttp.ClientError(f"Failed after {self.settings.max_retries} attempts")

    def _calculate_backoff(self, attempt: int, multiplier: float | None = None) -> float:
        """Calculate exponential backoff delay"""
        mult = multiplier or self.settings.backoff_multiplier
        delay = self.settings.base_delay_seconds * (mult ** attempt)
        return min(delay, self.settings.max_delay_seconds)

    async def __aenter__(self) -> "HTTPClient":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()


# Singleton instance
_client: HTTPClient | None = None


def get_http_client() -> HTTPClient:
    """Get singleton HTTP client instance"""
    global _client
    if _client is None:
        _client = HTTPClient()
    return _client
