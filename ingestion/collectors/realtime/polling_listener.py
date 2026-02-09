# Polling listener for OrderFilled events using Etherscan API V2
#
# Used as fallback when WebSocket is unavailable.
# Also used for catch-up of missed events during WebSocket reconnections.

import asyncio
from typing import Callable, Awaitable
import structlog

from ingestion.config import get_settings
from ingestion.http_client import HTTPClient, APIType
from .constants import (
    ORDER_FILLED_TOPIC,
    CTF_EXCHANGE_ADDRESS,
    NEGRISK_CTF_EXCHANGE_ADDRESS,
)
from .event_decoder import EventDecoder, DecodedOrderFilled

logger = structlog.get_logger()


class PollingListener:
    """
    Event listener via polling Etherscan API V2.

    Features:
    - Uses getLogs endpoint with topic filtering
    - Respects rate limits (3 req/sec free tier)
    - Tracks last processed block
    - Supports catch-up from specific block

    Usage:
        async def handle_event(event: DecodedOrderFilled):
            print(f"Trade: {event.tx_hash}")

        listener = PollingListener(on_event=handle_event)
        await listener.start()  # Polls indefinitely
    """

    def __init__(
        self,
        on_event: Callable[[DecodedOrderFilled], Awaitable[None]],
        http_client: HTTPClient | None = None,
    ):
        """
        Args:
            on_event: Async callback for each decoded event
            http_client: HTTP client (uses singleton if not provided)
        """
        self.settings = get_settings()
        self.on_event = on_event
        self.http_client = http_client or HTTPClient()
        self.decoder = EventDecoder()
        self.logger = logger.bind(component="polling_listener")

        self._last_block: int | None = None
        self._running = False

    @property
    def last_block(self) -> int | None:
        """Last processed block number"""
        return self._last_block

    async def start(self, from_block: int | None = None) -> None:
        """
        Start the polling loop.

        Args:
            from_block: Block to start from (None = latest block)
        """
        self._running = True
        self.logger.info("polling_listener_starting", from_block=from_block)

        if from_block:
            self._last_block = from_block
        else:
            self._last_block = await self._get_latest_block()

        self.logger.info("polling_from_block", block=self._last_block)

        while self._running:
            try:
                await self._poll_once()
            except Exception as e:
                self.logger.error("polling_error", error=str(e))

            await asyncio.sleep(self.settings.polling_interval_seconds)

    async def stop(self) -> None:
        """Stop the polling loop"""
        self.logger.info("polling_listener_stopping")
        self._running = False

    async def catch_up(self, from_block: int, to_block: int | None = None) -> int:
        """
        Catch up events between two blocks.

        Useful when WebSocket reconnects and needs to recover
        events missed during disconnection.

        Args:
            from_block: Starting block
            to_block: Ending block (None = latest)

        Returns:
            Number of events processed
        """
        self.logger.info(
            "catch_up_starting",
            from_block=from_block,
            to_block=to_block or "latest",
        )

        events_processed = 0
        current_block = from_block

        # Get target block
        target_block = to_block or await self._get_latest_block()

        # Process in batches
        blocks_per_batch = self.settings.polling_blocks_per_request

        while current_block <= target_block:
            end_block = min(current_block + blocks_per_batch - 1, target_block)

            # Get logs for both contracts
            for contract in [CTF_EXCHANGE_ADDRESS, NEGRISK_CTF_EXCHANGE_ADDRESS]:
                logs = await self._get_logs(contract, current_block, end_block)

                for log in logs:
                    decoded = self.decoder.decode_order_filled(log)
                    if decoded:
                        await self.on_event(decoded)
                        events_processed += 1

            current_block = end_block + 1

            # Small delay to respect rate limits
            await asyncio.sleep(0.5)

        self._last_block = target_block

        self.logger.info(
            "catch_up_completed",
            events_processed=events_processed,
            final_block=target_block,
        )

        return events_processed

    async def _poll_once(self) -> None:
        """Execute one polling iteration"""
        latest_block = await self._get_latest_block()

        if self._last_block and self._last_block >= latest_block:
            # No new blocks
            self.logger.debug("no_new_blocks", current=self._last_block)
            return

        from_block = (self._last_block + 1) if self._last_block else latest_block

        self.logger.debug(
            "polling_blocks",
            from_block=from_block,
            to_block=latest_block,
        )

        events_found = 0

        # Get logs for both contracts
        for contract in [CTF_EXCHANGE_ADDRESS, NEGRISK_CTF_EXCHANGE_ADDRESS]:
            logs = await self._get_logs(contract, from_block, latest_block)

            for log in logs:
                decoded = self.decoder.decode_order_filled(log)
                if decoded:
                    await self.on_event(decoded)
                    events_found += 1

        if events_found > 0:
            self.logger.info(
                "events_found",
                count=events_found,
                from_block=from_block,
                to_block=latest_block,
            )

        self._last_block = latest_block

    async def _get_logs(
        self,
        contract: str,
        from_block: int,
        to_block: int | str = "latest",
    ) -> list[dict]:
        """
        Get OrderFilled logs via Etherscan API V2.

        Args:
            contract: Contract address to query
            from_block: Starting block
            to_block: Ending block (int or "latest")

        Returns:
            List of log entries
        """
        params = {
            "chainid": self.settings.polygon_chain_id,
            "module": "logs",
            "action": "getLogs",
            "address": contract,
            "topic0": ORDER_FILLED_TOPIC,
            "fromBlock": from_block,
            "toBlock": to_block,
            "apikey": self.settings.etherscan_api_key,
        }

        try:
            response = await self.http_client.get(
                self.settings.etherscan_api_url,
                params=params,
                api_type=APIType.POLYGONSCAN,
            )

            status = response.get("status")
            message = response.get("message", "")

            if status == "1":
                return response.get("result", [])

            # "No records found" is not an error
            if message == "No records found" or "No records" in str(
                response.get("result", "")
            ):
                return []

            self.logger.warning(
                "etherscan_api_warning",
                message=message,
                result=str(response.get("result", ""))[:100],
            )
            return []

        except Exception as e:
            self.logger.error(
                "get_logs_failed",
                contract=contract[:20] + "...",
                error=str(e),
            )
            return []

    async def _get_latest_block(self) -> int:
        """Get the latest Polygon block number"""
        params = {
            "chainid": self.settings.polygon_chain_id,
            "module": "proxy",
            "action": "eth_blockNumber",
            "apikey": self.settings.etherscan_api_key,
        }

        try:
            response = await self.http_client.get(
                self.settings.etherscan_api_url,
                params=params,
                api_type=APIType.POLYGONSCAN,
            )

            result = response.get("result", "0x0")
            return int(result, 16)

        except Exception as e:
            self.logger.error("get_latest_block_failed", error=str(e))
            # Return a reasonable fallback (current approximate block)
            return 65_000_000  # Approximate Polygon block as of early 2026
