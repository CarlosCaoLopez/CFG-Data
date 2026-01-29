# Wallet Age Collector - Fetch wallet first-seen dates from Data API + Polygonscan

import structlog
from datetime import datetime
from typing import Dict

from ingestion.config import get_settings
from ingestion.http_client import get_http_client, APIType

logger = structlog.get_logger()


class WalletAgeCollector:
    """
    Collects wallet age information from two sources:
    1. Polymarket Data API - First activity on Polymarket
    2. Polygonscan API - First transaction on Polygon blockchain

    The blockchain age is more accurate but requires an API key.
    """

    def __init__(self):
        self.settings = get_settings()
        self.data_api_url = f"{self.settings.data_api_url}/activity"
        self.polygonscan_url = self.settings.polygonscan_api_url
        self._cache: Dict[str, dict] = {}  # Simple in-memory cache

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

        # Check cache first
        if wallet in self._cache:
            return self._cache[wallet]

        result = {
            "wallet": wallet,
            "first_tx_polymarket": None,
            "first_tx_polygon": None,
        }

        # Fetch from Polymarket
        polymarket_age = await self._fetch_polymarket_first_tx(wallet)
        if polymarket_age:
            result["first_tx_polymarket"] = polymarket_age

        # Fetch from Polygonscan if enabled and API key is set
        if use_polygonscan and self.settings.polygonscan_api_key:
            polygon_age = await self._fetch_polygonscan_first_tx(wallet)
            if polygon_age:
                result["first_tx_polygon"] = polygon_age

        # Cache the result
        self._cache[wallet] = result

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

        logger.info(
            "fetching_wallet_ages_batch",
            wallet_count=len(wallets),
            use_polygonscan=use_polygonscan,
        )

        for i, wallet in enumerate(wallets):
            try:
                result = await self.fetch_wallet_age(wallet, use_polygonscan)
                results[wallet.lower()] = result

                if (i + 1) % 50 == 0:
                    logger.debug(
                        "wallet_age_progress",
                        processed=i + 1,
                        total=len(wallets),
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
        """Fetch first transaction timestamp from Polygonscan API"""
        client = get_http_client()

        params = {
            "module": "account",
            "action": "txlist",
            "address": wallet,
            "startblock": "0",
            "endblock": "99999999",
            "sort": "asc",
            "page": "1",
            "offset": "1",
            "apikey": self.settings.polygonscan_api_key,
        }

        try:
            response = await client.get(
                self.polygonscan_url,
                params=params,
                api_type=APIType.POLYGONSCAN,
            )

            if response and isinstance(response, dict):
                status = response.get("status")
                result = response.get("result", [])

                if status == "1" and result and len(result) > 0:
                    first_tx = result[0]
                    timestamp = first_tx.get("timeStamp")

                    if timestamp:
                        return datetime.utcfromtimestamp(int(timestamp))

        except Exception as e:
            logger.warning(
                "polygonscan_age_fetch_failed",
                wallet=wallet[:10] + "...",
                error=str(e),
            )

        return None

    def clear_cache(self):
        """Clear the in-memory cache"""
        self._cache.clear()
        logger.debug("wallet_age_cache_cleared")
