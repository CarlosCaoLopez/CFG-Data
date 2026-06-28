# Wallet Age Collector - Fetch wallet first-seen dates from Data API + Etherscan V2

import asyncio
import json
import structlog
from datetime import datetime
from typing import Dict

import redis.asyncio as redis

from ingestion.config import get_settings
from ingestion.http_client import get_http_client, APIType

logger = structlog.get_logger()

# A wallet's first transaction is immutable, so cached ages never go stale.
# The TTL only bounds Redis memory on the 2GB Pi; a wallet re-seen within the
# window stays cached and never re-hits Etherscan (the rate-limited bottleneck).
WALLET_AGE_CACHE_PREFIX = "wallet_age:"
WALLET_AGE_CACHE_TTL_SECONDS = 30 * 24 * 3600  # 30 days


class WalletAgeCollector:
    """
    Collects wallet age information from two sources:
    1. Polymarket Data API - First activity on Polymarket
    2. Etherscan API V2 - First transaction on Polygon blockchain

    The blockchain age is more accurate but requires an API key.
    Uses Etherscan API V2 unified endpoint with chainid=137 for Polygon.

    Two-level cache to minimise Etherscan calls (free tier = 3 req/s, the main
    throughput bottleneck at market close):
    - L1: process-local dict (fast, lost on restart)
    - L2: Redis (shared, survives restart). Only SUCCESSFUL polygon lookups are
      cached, so a rate-limit failure never poisons a wallet's age permanently.
    """

    def __init__(self):
        self.settings = get_settings()
        self.data_api_url = f"{self.settings.data_api_url}/activity"
        # Etherscan API V2 unified endpoint (replaces deprecated polygonscan.com/api)
        self.etherscan_url = self.settings.etherscan_api_url
        self._cache: Dict[str, dict] = {}  # L1: in-memory cache
        self._redis: redis.Redis | None = None  # L2: shared Redis cache

    async def _get_redis(self) -> "redis.Redis | None":
        """Lazy Redis connection for the L2 cache. Returns None if unavailable."""
        if self._redis is None:
            try:
                self._redis = redis.from_url(
                    self.settings.redis_url, decode_responses=True
                )
            except Exception as e:
                logger.warning("wallet_age_redis_unavailable", error=str(e))
                return None
        return self._redis

    @staticmethod
    def _serialize(result: dict) -> str:
        def _iso(v):
            return v.isoformat() if isinstance(v, datetime) else None
        return json.dumps({
            "wallet": result["wallet"],
            "first_tx_polymarket": _iso(result.get("first_tx_polymarket")),
            "first_tx_polygon": _iso(result.get("first_tx_polygon")),
        })

    @staticmethod
    def _deserialize(raw: str) -> dict:
        d = json.loads(raw)
        def _dt(v):
            return datetime.fromisoformat(v) if v else None
        return {
            "wallet": d["wallet"],
            "first_tx_polymarket": _dt(d.get("first_tx_polymarket")),
            "first_tx_polygon": _dt(d.get("first_tx_polygon")),
        }

    async def fetch_wallet_age(
        self,
        wallet: str,
        use_polygonscan: bool = True,
    ) -> dict:
        """
        Fetch age information for a single wallet.

        Args:
            wallet: Wallet address
            use_polygonscan: Whether to also query Polygonscan (requires API key)

        Returns:
            Dict with wallet age info:
            {
                "wallet": str,
                "first_tx_polymarket": datetime | None,
                "first_tx_polygon": datetime | None,
            }
        """
        wallet = wallet.lower()

        # L1: process-local cache
        if wallet in self._cache:
            return self._cache[wallet]

        # L2: shared Redis cache (survives restarts, avoids re-hitting Etherscan)
        r = await self._get_redis()
        if r is not None:
            try:
                raw = await r.get(WALLET_AGE_CACHE_PREFIX + wallet)
            except Exception:
                raw = None
            if raw:
                result = self._deserialize(raw)
                self._cache[wallet] = result
                return result

        result = {
            "wallet": wallet,
            "first_tx_polymarket": None,
            "first_tx_polygon": None,
        }

        # Fetch from Polymarket
        polymarket_age = await self._fetch_polymarket_first_tx(wallet)
        if polymarket_age:
            result["first_tx_polymarket"] = polymarket_age

        # Fetch from Etherscan API V2 if enabled and API key is set
        if use_polygonscan and self.settings.etherscan_api_key:
            polygon_age = await self._fetch_polygonscan_first_tx(wallet)
            if polygon_age:
                result["first_tx_polygon"] = polygon_age

        # Only cache SUCCESSFUL polygon lookups. A None from Etherscan is almost
        # always a rate-limit/transient failure (Polymarket traders must have
        # Polygon activity), and caching it would poison the age permanently.
        if result["first_tx_polygon"] is not None:
            self._cache[wallet] = result
            if r is not None:
                try:
                    await r.set(
                        WALLET_AGE_CACHE_PREFIX + wallet,
                        self._serialize(result),
                        ex=WALLET_AGE_CACHE_TTL_SECONDS,
                    )
                except Exception as e:
                    logger.debug("wallet_age_cache_write_failed", error=str(e))

        return result

    async def fetch_wallet_ages_batch(
        self,
        wallets: set[str],
        use_polygonscan: bool = True,
    ) -> Dict[str, dict]:
        """
        Fetch age information for multiple wallets.

        Args:
            wallets: Set of wallet addresses
            use_polygonscan: Whether to also query Polygonscan

        Returns:
            Dict mapping wallet address to age info
        """
        results = {}
        wallets = {w.lower() for w in wallets}

        logger.info(
            "fetching_wallet_ages_batch",
            wallet_count=len(wallets),
            use_polygonscan=use_polygonscan,
        )

        # Bulk-load the L2 cache in a single round-trip so we only call Etherscan
        # for wallets we have never resolved before. After a few days most
        # wallets repeat across markets, so this collapses the bottleneck.
        misses = set(wallets)
        r = await self._get_redis()
        if r is not None and wallets:
            ordered = list(wallets)
            try:
                cached = await r.mget(
                    [WALLET_AGE_CACHE_PREFIX + w for w in ordered]
                )
            except Exception as e:
                logger.warning("wallet_age_cache_mget_failed", error=str(e))
                cached = [None] * len(ordered)
            for w, raw in zip(ordered, cached):
                if raw:
                    result = self._deserialize(raw)
                    self._cache[w] = result
                    results[w] = result
                    misses.discard(w)

        logger.info(
            "wallet_age_cache_stats",
            cached=len(wallets) - len(misses),
            to_fetch=len(misses),
        )

        for i, wallet in enumerate(misses):
            try:
                result = await self.fetch_wallet_age(wallet, use_polygonscan)
                results[wallet] = result

                if (i + 1) % 50 == 0:
                    logger.debug(
                        "wallet_age_progress",
                        processed=i + 1,
                        total=len(misses),
                    )
            except Exception as e:
                logger.warning(
                    "wallet_age_fetch_failed",
                    wallet=wallet[:10] + "...",
                    error=str(e),
                )
                # Store partial result
                results[wallet.lower()] = {
                    "wallet": wallet.lower(),
                    "first_tx_polymarket": None,
                    "first_tx_polygon": None,
                }

        logger.info(
            "finished_fetching_wallet_ages",
            total=len(results),
        )

        return results

    async def _fetch_polymarket_first_tx(self, wallet: str) -> datetime | None:
        """Fetch first transaction timestamp from Polymarket Data API"""
        client = get_http_client()

        params = {
            "user": wallet,
            "sortBy": "TIMESTAMP",
            "sortDirection": "ASC",
            "limit": "1",
        }

        try:
            response = await client.get(
                self.data_api_url,
                params=params,
                api_type=APIType.POLYMARKET,
            )

            if response and isinstance(response, list) and len(response) > 0:
                first_activity = response[0]
                timestamp = first_activity.get("timestamp")

                if timestamp:
                    # Handle both string ISO format and unix timestamp
                    if isinstance(timestamp, str):
                        return datetime.fromisoformat(
                            timestamp.replace("Z", "+00:00")
                        ).replace(tzinfo=None)
                    else:
                        # Unix timestamp (seconds or milliseconds)
                        if timestamp > 1e12:  # Milliseconds
                            timestamp = timestamp / 1000
                        return datetime.utcfromtimestamp(timestamp)

        except Exception as e:
            logger.warning(
                "polymarket_age_fetch_failed",
                wallet=wallet[:10] + "...",
                error=str(e),
            )

        return None

    async def _fetch_polygonscan_first_tx(self, wallet: str) -> datetime | None:
        """
        Fetch first transaction timestamp from Polygonscan API.
        Combines both normal transactions (txlist) and internal transactions (txlistinternal)
        to find the true first activity on the blockchain.

        Internal transactions are important because wallets funded via bridges or
        smart contracts may not have normal transactions as their first activity.
        """
        # Fetch both normal and internal transactions in parallel concept,
        # but sequentially to respect rate limits
        normal_tx_date = await self._fetch_first_tx_by_action(wallet, "txlist")
        internal_tx_date = await self._fetch_first_tx_by_action(wallet, "txlistinternal")

        # Return the oldest date (earliest activity)
        if normal_tx_date and internal_tx_date:
            oldest = min(normal_tx_date, internal_tx_date)
            logger.debug(
                "polygonscan_wallet_age_combined",
                wallet=wallet[:10] + "...",
                normal_tx=normal_tx_date.isoformat() if normal_tx_date else None,
                internal_tx=internal_tx_date.isoformat() if internal_tx_date else None,
                oldest=oldest.isoformat(),
            )
            return oldest
        elif normal_tx_date:
            return normal_tx_date
        elif internal_tx_date:
            return internal_tx_date
        else:
            return None

    async def _fetch_first_tx_by_action(
        self, wallet: str, action: str, max_retries: int = 3
    ) -> datetime | None:
        """
        Fetch first transaction timestamp for a specific action type.
        Includes automatic retry on rate limit errors.

        Args:
            wallet: Wallet address
            action: Either "txlist" (normal) or "txlistinternal" (internal)
            max_retries: Maximum number of retries on rate limit errors

        Returns:
            datetime of first transaction or None
        """
        client = get_http_client()

        # Etherscan API V2 parameters - unified endpoint with chainid
        params = {
            "chainid": str(self.settings.polygon_chain_id),  # 137 for Polygon
            "module": "account",
            "action": action,
            "address": wallet,
            "startblock": "0",
            "endblock": "99999999",
            "sort": "asc",
            "page": "1",
            "offset": "1",
            "apikey": self.settings.etherscan_api_key,
        }

        for attempt in range(max_retries):
            try:
                response = await client.get(
                    self.etherscan_url,
                    params=params,
                    api_type=APIType.POLYGONSCAN,
                )

                if response and isinstance(response, dict):
                    status = response.get("status")
                    message = response.get("message", "")
                    result = response.get("result", [])

                    # status="1" means success with results
                    if status == "1" and result and len(result) > 0:
                        first_tx = result[0]
                        timestamp = first_tx.get("timeStamp")

                        if timestamp:
                            return datetime.utcfromtimestamp(int(timestamp))

                    # status="0" with "No transactions found" is not an error
                    elif status == "0" and "No transactions found" in message:
                        logger.debug(
                            "polygonscan_no_transactions",
                            wallet=wallet[:10] + "...",
                            action=action,
                        )
                        return None

                    # Rate limit error - wait and retry
                    elif status == "0" and "rate limit" in str(result).lower():
                        if attempt < max_retries - 1:
                            logger.debug(
                                "etherscan_rate_limit_retry",
                                wallet=wallet[:10] + "...",
                                action=action,
                                attempt=attempt + 1,
                                max_retries=max_retries,
                            )
                            await asyncio.sleep(1.0)  # Wait 1 second before retry
                            continue
                        else:
                            logger.warning(
                                "etherscan_rate_limit_exhausted",
                                wallet=wallet[:10] + "...",
                                action=action,
                                attempts=max_retries,
                            )
                            return None

                    # Other status="0" cases are actual errors
                    elif status == "0":
                        logger.warning(
                            "etherscan_api_error",
                            wallet=wallet[:10] + "...",
                            action=action,
                            message=message,
                            result=str(result)[:100] if result else None,
                        )
                        return None

            except Exception as e:
                logger.warning(
                    "etherscan_age_fetch_failed",
                    wallet=wallet[:10] + "...",
                    action=action,
                    error=str(e),
                )
                return None

        return None

    def clear_cache(self):
        """Clear the in-memory cache"""
        self._cache.clear()
        logger.debug("wallet_age_cache_cleared")
