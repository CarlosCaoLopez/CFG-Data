# Trades Collector - Fetch trades per market from Data API

import structlog
from typing import AsyncIterator

from ingestion.config import get_settings
from ingestion.http_client import get_http_client, APIType

logger = structlog.get_logger()


class TradesCollector:
    """
    Collects trades for a specific market from Polymarket Data API.

    Data API endpoints:
    - GET /activity - requires user parameter, can filter by market
    - GET /trades - trades by market (conditionId)
    """

    def __init__(self):
        self.settings = get_settings()
        self.activity_url = f"{self.settings.data_api_url}/activity"
        self.trades_url = f"{self.settings.data_api_url}/trades"

    async def fetch_trades_for_market(
        self,
        market_id: str,
        limit: int | None = None,
    ) -> AsyncIterator[dict]:
        """
        Fetch all trades for a specific market with pagination.
        Uses the /trades endpoint which accepts conditionId directly.

        Args:
            market_id: The market's conditionId
            limit: Maximum number of trades to return (None = all)

        Yields:
            Raw trade data dicts from Data API
        """
        client = get_http_client()

        # /trades endpoint with market parameter (conditionId)
        params = {
            "market": market_id,
            "limit": "500",
            "offset": "0",
        }

        count = 0
        offset = 0

        logger.info(
            "fetching_trades_for_market",
            market_id=market_id,
            max_limit=limit,
        )

        while True:
            params["offset"] = str(offset)

            try:
                response = await client.get(
                    self.trades_url,
                    params=params,
                    api_type=APIType.POLYMARKET,
                )
            except Exception as e:
                logger.error(
                    "fetch_trades_failed",
                    market_id=market_id,
                    offset=offset,
                    error=str(e),
                )
                raise

            # Response is a list of trades
            trades = response if isinstance(response, list) else []

            if not trades:
                logger.info(
                    "no_more_trades",
                    market_id=market_id,
                    total_fetched=count,
                )
                break

            for trade in trades:
                yield trade
                count += 1

                if limit and count >= limit:
                    logger.info(
                        "reached_trade_limit",
                        market_id=market_id,
                        limit=limit,
                    )
                    return

            logger.debug(
                "fetched_trades_page",
                market_id=market_id,
                page_size=len(trades),
                total_so_far=count,
                offset=offset,
            )

            # If we got less than the limit, we've reached the end
            if len(trades) < 500:
                break

            offset += len(trades)

        logger.info(
            "finished_fetching_trades",
            market_id=market_id,
            total_fetched=count,
        )

    async def fetch_trades_for_wallet(
        self,
        wallet: str,
        market_id: str | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[dict]:
        """
        Fetch trades for a specific wallet.

        Args:
            wallet: Wallet address
            market_id: Optional filter by market
            limit: Maximum number of trades to return

        Yields:
            Raw trade data dicts
        """
        client = get_http_client()

        params = {
            "user": wallet.lower(),
            "type": "TRADE",
            "sortBy": "TIMESTAMP",
            "sortDirection": "DESC",
            "limit": "500",
            "offset": "0",
        }

        if market_id:
            params["market"] = market_id

        count = 0
        offset = 0

        logger.info(
            "fetching_trades_for_wallet",
            wallet=wallet[:10] + "...",
            market_id=market_id,
        )

        while True:
            params["offset"] = str(offset)

            try:
                response = await client.get(
                    self.activity_url,
                    params=params,
                    api_type=APIType.POLYMARKET,
                )
            except Exception as e:
                logger.error(
                    "fetch_wallet_trades_failed",
                    wallet=wallet[:10] + "...",
                    error=str(e),
                )
                raise

            if not isinstance(response, list):
                logger.warning(
                    "unexpected_wallet_trades_response_format",
                    wallet=wallet[:10] + "...",
                    response_type=type(response).__name__,
                    response_preview=str(response)[:200],
                )
                break

            trades = response

            if not trades:
                break

            for trade in trades:
                yield trade
                count += 1

                if limit and count >= limit:
                    return

            if len(trades) < 500:
                break

            offset += len(trades)

        logger.info(
            "finished_fetching_wallet_trades",
            wallet=wallet[:10] + "...",
            total_fetched=count,
        )

    def extract_wallets_from_trades(self, trades: list[dict]) -> set[str]:
        """
        Extract unique wallet addresses from a list of trades.

        Args:
            trades: List of trade dicts

        Returns:
            Set of unique wallet addresses (lowercase)
        """
        wallets = set()
        for trade in trades:
            wallet = trade.get("proxyWallet") or trade.get("user")
            if wallet:
                wallets.add(wallet.lower())
        return wallets
