# Markets Collector - Fetch closed markets from Gamma API

import structlog
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator

from ingestion.config import get_settings
from ingestion.http_client import get_http_client, APIType

logger = structlog.get_logger()


class MarketsCollector:
    """
    Collects closed markets from Polymarket Gamma API.

    Gamma API endpoint: GET /markets
    Supports filtering by:
    - closed: bool
    - volume_num_min: minimum volume in USDC
    - order: field to sort by
    - ascending: sort direction
    - next_cursor: pagination
    """

    def __init__(self):
        self.settings = get_settings()
        self.base_url = f"{self.settings.gamma_api_url}/markets"

    async def fetch_closed_markets(
        self,
        min_volume_usdc: float | None = None,
        days_back: int | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[dict]:
        """
        Fetch closed markets with pagination.

        Args:
            min_volume_usdc: Minimum market volume (default from config)
            days_back: Only markets closed in last N days (default from config)
            limit: Maximum number of markets to return (None = all)

        Yields:
            Raw market data dicts from Gamma API
        """
        client = get_http_client()

        min_volume = min_volume_usdc or self.settings.backfill_min_volume_usdc
        days = days_back or self.settings.backfill_default_days

        # Calculate cutoff date
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)

        params = {
            "closed": "true",
            "volume_num_min": str(int(min_volume)),
            "order": "closedTime",
            "ascending": "false",
            "limit": "100",  # Max per page
        }

        count = 0
        next_cursor: str | None = None

        logger.info(
            "fetching_closed_markets",
            min_volume_usdc=min_volume,
            days_back=days,
            cutoff_date=cutoff_date.isoformat(),
        )

        while True:
            if next_cursor:
                params["next_cursor"] = next_cursor

            try:
                response = await client.get(
                    self.base_url,
                    params=params,
                    api_type=APIType.POLYMARKET,
                )
            except Exception as e:
                logger.error("fetch_markets_failed", error=str(e))
                raise

            # Handle both list response and paginated response
            if isinstance(response, list):
                markets = response
                next_cursor = None
            else:
                markets = response.get("data", response.get("markets", []))
                next_cursor = response.get("next_cursor")

            if not markets:
                logger.info("no_more_markets", total_fetched=count)
                break

            for market in markets:
                # Check if market closed after cutoff
                closed_time = market.get("closedTime") or market.get("endDate")
                if closed_time:
                    try:
                        if isinstance(closed_time, str):
                            # Handle ISO format (ensure timezone aware)
                            closed_dt = datetime.fromisoformat(
                                closed_time.replace("Z", "+00:00")
                            )
                            # If naive, assume UTC
                            if closed_dt.tzinfo is None:
                                closed_dt = closed_dt.replace(tzinfo=timezone.utc)
                        else:
                            # Handle unix timestamp (timezone aware)
                            closed_dt = datetime.fromtimestamp(closed_time / 1000, tz=timezone.utc)

                        if closed_dt < cutoff_date:
                            logger.info(
                                "reached_cutoff_date",
                                total_fetched=count,
                                cutoff=cutoff_date.isoformat(),
                            )
                            return
                    except (ValueError, TypeError) as e:
                        logger.warning(
                            "invalid_closed_time",
                            market_id=market.get("conditionId"),
                            closed_time=closed_time,
                            error=str(e),
                        )

                yield market
                count += 1

                if limit and count >= limit:
                    logger.info("reached_limit", limit=limit, total_fetched=count)
                    return

            logger.debug(
                "fetched_markets_page",
                page_size=len(markets),
                total_so_far=count,
                has_more=bool(next_cursor),
            )

            if not next_cursor:
                break

        logger.info("finished_fetching_markets", total_fetched=count)

    async def fetch_open_markets(
        self,
        min_volume_usdc: float | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[dict]:
        """
        Fetch open (active) markets with pagination.
        Used by L1 for Gamma API discovery of markets to track.

        Args:
            min_volume_usdc: Minimum market volume (default from L1 config)
            limit: Maximum number of markets to return (None = all)

        Yields:
            Raw market data dicts from Gamma API
        """
        client = get_http_client()

        min_volume = min_volume_usdc or self.settings.l1_min_volume_usdc

        params = {
            "closed": "false",
            "volume_num_min": str(int(min_volume)),
            "order": "volume",
            "ascending": "false",
            "limit": "100",
        }

        count = 0
        next_cursor: str | None = None

        logger.info(
            "fetching_open_markets",
            min_volume_usdc=min_volume,
        )

        while True:
            if next_cursor:
                params["next_cursor"] = next_cursor

            try:
                response = await client.get(
                    self.base_url,
                    params=params,
                    api_type=APIType.POLYMARKET,
                )
            except Exception as e:
                logger.error("fetch_open_markets_failed", error=str(e))
                raise

            if isinstance(response, list):
                markets = response
                next_cursor = None
            else:
                markets = response.get("data", response.get("markets", []))
                next_cursor = response.get("next_cursor")

            if not markets:
                break

            for market in markets:
                yield market
                count += 1

                if limit and count >= limit:
                    logger.info("open_markets_reached_limit", limit=limit, total_fetched=count)
                    return

            logger.debug(
                "fetched_open_markets_page",
                page_size=len(markets),
                total_so_far=count,
                has_more=bool(next_cursor),
            )

            if not next_cursor:
                break

        logger.info("finished_fetching_open_markets", total_fetched=count)

    async def fetch_market_by_id(self, condition_id: str) -> dict | None:
        """
        Fetch a single market by its condition ID.

        Args:
            condition_id: The market's conditionId

        Returns:
            Market data dict or None if not found
        """
        client = get_http_client()

        params = {"condition_ids": condition_id}

        try:
            response = await client.get(
                self.base_url,
                params=params,
                api_type=APIType.POLYMARKET,
            )

            if isinstance(response, list) and response:
                return response[0]
            elif isinstance(response, dict):
                markets = response.get("data", response.get("markets", []))
                if markets:
                    return markets[0]

            logger.warning("market_not_found", condition_id=condition_id)
            return None

        except Exception as e:
            logger.error(
                "fetch_market_by_id_failed",
                condition_id=condition_id,
                error=str(e),
            )
            raise

    async def fetch_event_by_id(self, event_id: str) -> dict | None:
        """
        Fetch a single event from the Gamma Events API.

        The Events API returns tags for ALL markets (active and closed)
        and category for closed markets. The /markets endpoint does NOT
        return category/tags when queried by condition_ids or clob_token_ids.

        Args:
            event_id: The event ID (from events[0].id in market response)

        Returns:
            Event data dict or None if not found
        """
        client = get_http_client()
        events_url = f"{self.settings.gamma_api_url}/events/{event_id}"

        try:
            response = await client.get(
                events_url,
                api_type=APIType.POLYMARKET,
            )

            if isinstance(response, dict) and response.get("id"):
                return response
            elif isinstance(response, list) and response:
                return response[0]

            logger.warning("event_not_found", event_id=event_id)
            return None

        except Exception as e:
            logger.warning(
                "fetch_event_by_id_failed",
                event_id=event_id,
                error=str(e),
            )
            return None

    async def fetch_market_by_token_id(self, token_id: str) -> dict | None:
        """
        Fetch a market by its CLOB token ID (outcome token).

        The token_id is the asset ID from OrderFilled events on-chain.

        Strategy (two calls):
        1. GET /markets?clob_token_ids=X → market data + events[0].id
        2. GET /events/{event_id} → tags (always) + category (if closed)

        Then merges event tags/category into the market dict so the
        normalizer can extract them.

        Args:
            token_id: The outcome token ID (from on-chain events)

        Returns:
            Market data dict enriched with category/tags, or None
        """
        client = get_http_client()

        # Step 1: Get market data + event_id from token_id
        params = {"clob_token_ids": token_id}

        try:
            response = await client.get(
                self.base_url,
                params=params,
                api_type=APIType.POLYMARKET,
            )

            # Extract market from response
            market = None
            if isinstance(response, list) and response:
                market = response[0]
            elif isinstance(response, dict):
                markets = response.get("data", response.get("markets", []))
                if markets:
                    market = markets[0]

            if not market:
                logger.debug(
                    "market_not_found_for_token",
                    token_id=token_id[:30] + "...",
                )
                return None

            # Step 2: Get event_id and fetch from Events API for tags/category
            events = market.get("events", [])
            event_id = None
            if events and isinstance(events, list) and len(events) > 0:
                event_id = events[0].get("id")

            if event_id:
                event_data = await self.fetch_event_by_id(str(event_id))

                if event_data:
                    # Merge category from Events API into market
                    event_category = event_data.get("category")
                    if event_category:
                        market["category"] = event_category

                    # Merge tags into events[0] so normalizer can extract them
                    event_tags = event_data.get("tags", [])
                    if event_tags and market.get("events"):
                        market["events"][0]["tags"] = event_tags

                    logger.debug(
                        "market_enriched_from_events_api",
                        token_id=token_id[:20] + "...",
                        condition_id=market.get("conditionId", "")[:20] + "...",
                        category=market.get("category"),
                        tags_count=len(event_tags),
                    )
            else:
                logger.debug(
                    "no_event_id_for_market",
                    token_id=token_id[:20] + "...",
                )

            return market

        except Exception as e:
            logger.warning(
                "fetch_market_by_token_id_failed",
                token_id=token_id[:20] + "...",
                error=str(e),
            )
            return None
