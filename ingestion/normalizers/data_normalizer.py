# Data Normalizer - Convert raw API data to contract models

import json
import structlog
from datetime import datetime, timezone
from typing import Any

from contracts.raw_market_data import (
    RawTrade,
    RawMarketMetadata,
    RawWalletAge,
    MarketDataBatch,
)

logger = structlog.get_logger()


class DataNormalizer:
    """
    Converts raw API responses to Pydantic contract models.

    Handles:
    - Gamma API market data → RawMarketMetadata
    - Data API trades → RawTrade
    - Wallet age data → RawWalletAge
    """

    def normalize_market(self, raw: dict[str, Any]) -> RawMarketMetadata:
        """
        Convert raw Gamma API market response to RawMarketMetadata.

        Args:
            raw: Raw market dict from Gamma API

        Returns:
            RawMarketMetadata instance
        """
        # Parse end_date
        end_date = self._parse_datetime(
            raw.get("endDate") or raw.get("endDateIso")
        )

        # Parse outcome_prices (comes as JSON string like '["1","0"]')
        outcome_prices = self._parse_outcome_prices(raw.get("outcomePrices"))

        # Parse volume (may be string or number)
        volume = float(raw.get("volume") or 0)

        return RawMarketMetadata(
            market_id=raw["conditionId"],
            question=raw.get("question", ""),
            end_date=end_date,
            closed=bool(raw.get("closed", False)),
            outcome_prices=outcome_prices,
            volume_usdc=volume,
        )

    def normalize_trade(self, raw: dict[str, Any], market_id: str) -> RawTrade:
        """
        Convert raw Data API trade response to RawTrade.

        Args:
            raw: Raw trade dict from Data API /trades
            market_id: The market's conditionId (for validation)

        Returns:
            RawTrade instance
        """
        # Get transaction hash
        tx_hash = (
            raw.get("transactionHash") or
            raw.get("txHash") or
            raw.get("hash") or
            ""
        )

        # Get wallet address
        wallet = (
            raw.get("proxyWallet") or
            raw.get("user") or
            raw.get("maker") or
            ""
        ).lower()

        # Parse timestamp
        timestamp = self._parse_datetime(raw.get("timestamp"))

        # Get price and size
        price = float(raw.get("price") or 0)
        size_tokens = float(raw.get("size") or 0)
        size_usdc = size_tokens * price  # Calculate USDC value

        # Get side (BUY/SELL)
        side = raw.get("side", "BUY").upper()
        if side not in ("BUY", "SELL"):
            side = "BUY"

        # Get outcome (Yes/No)
        outcome = raw.get("outcome", "Yes")
        if outcome not in ("Yes", "No"):
            # Try to normalize common variations
            outcome = "Yes" if outcome.lower() in ("yes", "true", "1") else "No"

        return RawTrade(
            tx_hash=tx_hash,
            wallet=wallet,
            market_id=raw.get("conditionId", market_id),
            timestamp=timestamp,
            side=side,
            outcome=outcome,
            price=price,
            size_usdc=size_usdc,
            size_tokens=size_tokens,
        )

    def normalize_wallet_age(self, raw: dict[str, Any]) -> RawWalletAge:
        """
        Convert wallet age dict to RawWalletAge.

        Args:
            raw: Wallet age dict from WalletAgeCollector

        Returns:
            RawWalletAge instance
        """
        return RawWalletAge(
            wallet=raw["wallet"].lower(),
            first_tx_polymarket=raw.get("first_tx_polymarket"),
            first_tx_polygon=raw.get("first_tx_polygon"),
        )

    def normalize_batch(
        self,
        market_raw: dict[str, Any],
        trades_raw: list[dict[str, Any]],
        wallet_ages_raw: dict[str, dict[str, Any]],
    ) -> MarketDataBatch:
        """
        Create a complete MarketDataBatch from raw data.

        Args:
            market_raw: Raw market data from Gamma API
            trades_raw: List of raw trades from Data API
            wallet_ages_raw: Dict of wallet ages from WalletAgeCollector

        Returns:
            MarketDataBatch ready for publishing to queue
        """
        market_id = market_raw["conditionId"]

        # Normalize market metadata
        metadata = self.normalize_market(market_raw)

        # Normalize all trades
        trades = []
        for raw_trade in trades_raw:
            try:
                trade = self.normalize_trade(raw_trade, market_id)
                trades.append(trade)
            except Exception as e:
                logger.warning(
                    "trade_normalization_failed",
                    tx_hash=raw_trade.get("transactionHash", "unknown"),
                    error=str(e),
                )

        # Normalize wallet ages
        wallet_ages = {}
        for wallet, age_data in wallet_ages_raw.items():
            try:
                wallet_age = self.normalize_wallet_age(age_data)
                wallet_ages[wallet.lower()] = wallet_age
            except Exception as e:
                logger.warning(
                    "wallet_age_normalization_failed",
                    wallet=wallet[:10] + "...",
                    error=str(e),
                )

        logger.info(
            "batch_normalized",
            market_id=market_id[:20] + "...",
            trades_count=len(trades),
            wallets_count=len(wallet_ages),
        )

        return MarketDataBatch(
            metadata=metadata,
            trades=trades,
            wallet_ages=wallet_ages,
        )

    def _parse_datetime(self, value: Any) -> datetime:
        """Parse datetime from various formats"""
        if value is None:
            return datetime.now(timezone.utc)

        if isinstance(value, datetime):
            return value

        if isinstance(value, (int, float)):
            # Unix timestamp (seconds or milliseconds)
            if value > 1e12:
                value = value / 1000
            return datetime.utcfromtimestamp(value)

        if isinstance(value, str):
            # ISO format string
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
            except ValueError:
                pass

            # Try parsing as unix timestamp string
            try:
                ts = float(value)
                if ts > 1e12:
                    ts = ts / 1000
                return datetime.utcfromtimestamp(ts)
            except ValueError:
                pass

        logger.warning("datetime_parse_failed", value=str(value)[:50])
        return datetime.now(timezone.utc)

    def _parse_outcome_prices(self, value: Any) -> list[float] | None:
        """Parse outcome prices from JSON string or list"""
        if value is None:
            return None

        if isinstance(value, list):
            try:
                return [float(p) for p in value]
            except (ValueError, TypeError):
                return None

        if isinstance(value, str):
            try:
                parsed = json.loads(value)
                if isinstance(parsed, list):
                    return [float(p) for p in parsed]
            except (json.JSONDecodeError, ValueError, TypeError):
                pass

        return None
