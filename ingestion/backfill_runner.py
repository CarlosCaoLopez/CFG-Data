# Backfill Runner - Historical data ingestion orchestrator

import asyncio
import json
import argparse
import structlog
from pathlib import Path
from datetime import datetime, timezone

from ingestion.config import get_settings
from ingestion.config_store import CategoriesStore
from ingestion.http_client import get_http_client
from ingestion.collectors.markets_collector import MarketsCollector
from ingestion.collectors.trades_collector import TradesCollector
from ingestion.collectors.wallet_age_collector import WalletAgeCollector
from ingestion.normalizers.data_normalizer import DataNormalizer
from ingestion.publisher import get_publisher

logger = structlog.get_logger()


class BackfillRunner:
    """
    Orchestrates historical data ingestion from Polymarket.

    Flow:
    1. Fetch closed markets from Gamma API
    2. For each market:
       a. Fetch all trades from Data API
       b. Extract unique wallets
       c. Fetch wallet ages (Polymarket + Polygonscan)
       d. Normalize all data
       e. Publish MarketDataBatch to Redis
    3. Save checkpoint for resumability
    """

    def __init__(
        self,
        days_back: int | None = None,
        min_volume: float | None = None,
        use_polygonscan: bool = True,
        dry_run: bool = False,
        max_wallets_per_market: int | None = None,
        max_trades_per_market: int | None = None,
    ):
        self.settings = get_settings()
        self.days_back = days_back or self.settings.backfill_default_days
        self.min_volume = min_volume or self.settings.backfill_min_volume_usdc
        self.use_polygonscan = use_polygonscan
        self.dry_run = dry_run
        self.max_wallets_per_market = max_wallets_per_market
        self.max_trades_per_market = max_trades_per_market

        self.markets_collector = MarketsCollector()
        self.trades_collector = TradesCollector()
        self.wallet_age_collector = WalletAgeCollector()
        self.normalizer = DataNormalizer(
            categorias_de_interes=self.settings.categorias_de_interes,
        )

        self.checkpoint_file = Path(self.settings.checkpoint_file)
        self.processed_markets: set[str] = set()

    async def run(self, limit: int | None = None) -> dict:
        """
        Run the backfill process.

        Args:
            limit: Maximum number of markets to process (None = all)

        Returns:
            Summary dict with stats
        """
        logger.info(
            "backfill_starting",
            days_back=self.days_back,
            min_volume=self.min_volume,
            use_polygonscan=self.use_polygonscan,
            dry_run=self.dry_run,
            limit=limit,
        )

        # Load checkpoint
        self._load_checkpoint()

        publisher = get_publisher()
        http_client = get_http_client()

        # Respect the persisted category filter (dashboard edits live in the
        # durable JSON store; .env is only the seed). No live updates needed
        # here — backfill is a one-shot batch job, so a single read is enough.
        if not self.dry_run:
            try:
                persisted = await CategoriesStore(self.settings.config_store_path).get()
                if persisted:
                    self.normalizer.set_interests(persisted)
                    logger.info("backfill_interests_loaded_from_store", categories=persisted)
            except Exception as e:
                logger.warning("backfill_interests_load_failed", error=str(e))

        stats = {
            "markets_processed": 0,
            "markets_skipped": 0,
            "total_trades": 0,
            "total_wallets": 0,
            "errors": 0,
            "started_at": datetime.now(timezone.utc).isoformat(),
        }

        try:
            # Check Redis health (skip in dry run)
            if not self.dry_run:
                healthy = await publisher.health_check()
                if not healthy:
                    logger.error("redis_not_available")
                    raise ConnectionError("Redis is not available")

            # Fetch and process markets
            market_count = 0
            async for market in self.markets_collector.fetch_closed_markets(
                min_volume_usdc=self.min_volume,
                days_back=self.days_back,
                limit=limit,
            ):
                market_id = market.get("conditionId")

                # Skip if already processed
                if market_id in self.processed_markets:
                    stats["markets_skipped"] += 1
                    logger.debug("market_already_processed", market_id=market_id[:20])
                    continue

                try:
                    result = await self._process_market(market, publisher)
                    stats["markets_processed"] += 1
                    stats["total_trades"] += result["trades_count"]
                    stats["total_wallets"] += result["wallets_count"]

                    # Save checkpoint after each market
                    self.processed_markets.add(market_id)
                    self._save_checkpoint()

                    market_count += 1
                    if market_count % 10 == 0:
                        logger.info(
                            "backfill_progress",
                            markets_processed=stats["markets_processed"],
                            total_trades=stats["total_trades"],
                        )

                except Exception as e:
                    stats["errors"] += 1
                    logger.error(
                        "market_processing_failed",
                        market_id=market_id[:20] if market_id else "unknown",
                        error=str(e),
                    )

        finally:
            await http_client.close()
            if not self.dry_run:
                await publisher.close()

        stats["finished_at"] = datetime.now(timezone.utc).isoformat()

        logger.info("backfill_completed", **stats)

        return stats

    async def _process_market(self, market: dict, publisher) -> dict:
        """
        Process a single market: fetch trades, wallet ages, normalize, publish.

        Args:
            market: Raw market data from Gamma API
            publisher: RedisPublisher instance

        Returns:
            Dict with processing stats
        """
        market_id = market["conditionId"]
        question = market.get("question", "")[:50]

        logger.info(
            "processing_market",
            market_id=market_id[:20] + "...",
            question=question + "...",
        )

        # 1. Fetch trades for this market (with optional limit for testing)
        trades_raw = []
        async for trade in self.trades_collector.fetch_trades_for_market(
            market_id, limit=self.max_trades_per_market
        ):
            trades_raw.append(trade)

        if not trades_raw:
            logger.warning("no_trades_found", market_id=market_id[:20])
            return {"trades_count": 0, "wallets_count": 0}

        # 2. Extract unique wallets (with optional limit for testing)
        wallets = self.trades_collector.extract_wallets_from_trades(trades_raw)
        if self.max_wallets_per_market and len(wallets) > self.max_wallets_per_market:
            wallets = set(list(wallets)[:self.max_wallets_per_market])
            logger.info(
                "wallets_limited",
                original_count=len(self.trades_collector.extract_wallets_from_trades(trades_raw)),
                limited_to=self.max_wallets_per_market,
            )

        # 3. Fetch wallet ages
        wallet_ages_raw = await self.wallet_age_collector.fetch_wallet_ages_batch(
            wallets,
            use_polygonscan=self.use_polygonscan,
        )

        # 3b. Enrich market with category/tags from the Events API.
        # ?closed=true returns them only intermittently; without this the
        # normalizer has nothing to derive the category from → "unknown".
        market = await self.markets_collector.enrich_market_with_event_data(market)

        # 4. Normalize all data
        batch = self.normalizer.normalize_batch(
            market_raw=market,
            trades_raw=trades_raw,
            wallet_ages_raw=wallet_ages_raw,
        )

        # 4b. Compute winning_outcome + mark last_trade (mirrors L1
        # _handle_market_close). The normalizer leaves these unset, so without
        # this step every backfilled market gets winning_outcome=NULL and all
        # trades last_trade=False.
        batch.metadata.winning_outcome = self.normalizer.compute_winning_outcome(
            batch.metadata.outcome_prices
        )
        self.normalizer.mark_last_trade(batch.trades)

        # 5. Publish to Redis (unless dry run)
        if not self.dry_run:
            batch_id = await publisher.publish_market_batch(batch)
            logger.info(
                "batch_published",
                batch_id=batch_id,
                trades_count=len(batch.trades),
            )
        else:
            logger.info(
                "dry_run_would_publish",
                market_id=market_id[:20],
                trades_count=len(batch.trades),
                wallets_count=len(batch.wallet_ages),
            )

        return {
            "trades_count": len(batch.trades),
            "wallets_count": len(batch.wallet_ages),
        }

    def _load_checkpoint(self) -> None:
        """Load processed markets from checkpoint file"""
        if self.checkpoint_file.exists():
            try:
                data = json.loads(self.checkpoint_file.read_text())
                self.processed_markets = set(data.get("processed_markets", []))
                logger.info(
                    "checkpoint_loaded",
                    processed_count=len(self.processed_markets),
                )
            except Exception as e:
                logger.warning("checkpoint_load_failed", error=str(e))
                self.processed_markets = set()

    def _save_checkpoint(self) -> None:
        """Save processed markets to checkpoint file"""
        try:
            data = {
                "processed_markets": list(self.processed_markets),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            self.checkpoint_file.write_text(json.dumps(data, indent=2))
        except Exception as e:
            logger.warning("checkpoint_save_failed", error=str(e))

    def clear_checkpoint(self) -> None:
        """Clear the checkpoint file"""
        if self.checkpoint_file.exists():
            self.checkpoint_file.unlink()
            logger.info("checkpoint_cleared")
        self.processed_markets = set()


async def main():
    """CLI entry point"""
    parser = argparse.ArgumentParser(
        description="Backfill historical Polymarket data"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=None,
        help="Number of days to look back (default: from config)",
    )
    parser.add_argument(
        "--min-volume",
        type=float,
        default=None,
        help="Minimum market volume in USDC (default: from config)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of markets to process",
    )
    parser.add_argument(
        "--no-polygonscan",
        action="store_true",
        help="Skip Polygonscan API calls for wallet ages",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Process data but don't publish to Redis",
    )
    parser.add_argument(
        "--clear-checkpoint",
        action="store_true",
        help="Clear checkpoint and start fresh",
    )
    parser.add_argument(
        "--max-wallets",
        type=int,
        default=None,
        help="Maximum wallets per market (for testing)",
    )
    parser.add_argument(
        "--max-trades",
        type=int,
        default=None,
        help="Maximum trades per market (for testing)",
    )

    args = parser.parse_args()

    runner = BackfillRunner(
        days_back=args.days,
        min_volume=args.min_volume,
        use_polygonscan=not args.no_polygonscan,
        dry_run=args.dry_run,
        max_wallets_per_market=args.max_wallets,
        max_trades_per_market=args.max_trades,
    )

    if args.clear_checkpoint:
        runner.clear_checkpoint()

    stats = await runner.run(limit=args.limit)

    print("\n" + "=" * 60)
    print("Backfill Summary")
    print("=" * 60)
    print(f"  Markets processed: {stats['markets_processed']}")
    print(f"  Markets skipped:   {stats['markets_skipped']}")
    print(f"  Total trades:      {stats['total_trades']}")
    print(f"  Total wallets:     {stats['total_wallets']}")
    print(f"  Errors:            {stats['errors']}")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
