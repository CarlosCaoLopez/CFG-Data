# Wallet Age Collector - Fetch wallet first-seen dates from Data API + Etherscan V2

import asyncio
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
    2. Etherscan API V2 - First transaction on Polygon blockchain

    The blockchain age is more accurate but requires an API key.
    Uses Etherscan API V2 unified endpoint with chainid=137 for Polygon.
    """

    def __init__(self):
        self.settings = get_settings()
        self.data_api_url = f"{self.settings.data_api_url}/activity"
        # Etherscan API V2 unified endpoint (replaces deprecated polygonscan.com/api)
        self.etherscan_url = self.settings.etherscan_api_url
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

        # Fetch from Etherscan API V2 if enabled and API key is set
        if use_polygonscan and self.settings.etherscan_api_key:
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
