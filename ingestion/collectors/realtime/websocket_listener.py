# WebSocket listener for OrderFilled events on Polygon
#
# Uses eth_subscribe to receive real-time logs from Polymarket contracts.
# Features:
# - Automatic reconnection with exponential backoff
# - Fallback between multiple RPC endpoints
# - Callback-based event handling

import asyncio
import json
from enum import Enum
from typing import Callable, Awaitable
import structlog

try:
    from websockets.client import connect, WebSocketClientProtocol
    from websockets.exceptions import ConnectionClosed
except ImportError:
    from websockets import connect
    from websockets.client import WebSocketClientProtocol
    from websockets.exceptions import ConnectionClosed

from ingestion.config import get_settings
from .constants import ORDER_FILLED_TOPIC, MONITORED_CONTRACTS
from .event_decoder import EventDecoder, DecodedOrderFilled

logger = structlog.get_logger()


class ConnectionState(Enum):
    """WebSocket connection state"""

    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    SUBSCRIBING = "subscribing"
    SUBSCRIBED = "subscribed"
    RECONNECTING = "reconnecting"
    FAILED = "failed"


class WebSocketListener:
    """
    Real-time event listener via WebSocket to Polygon RPC.

    Features:
    - Automatic reconnection with exponential backoff
    - Fallback between multiple endpoints
    - Callback for each decoded event

    Usage:
        async def handle_event(event: DecodedOrderFilled):
            print(f"Trade: {event.tx_hash}")

        listener = WebSocketListener(on_event=handle_event)
        await listener.start()  # Runs indefinitely
    """

    def __init__(
        self,
        on_event: Callable[[DecodedOrderFilled], Awaitable[None]],
        on_connection_lost: Callable[[], Awaitable[None]] | None = None,
    ):
        """
        Args:
            on_event: Async callback for each decoded event
            on_connection_lost: Async callback when connection is lost permanently
        """
        self.settings = get_settings()
        self.on_event = on_event
        self.on_connection_lost = on_connection_lost

        self.decoder = EventDecoder()
        self.logger = logger.bind(component="websocket_listener")

        self._state = ConnectionState.DISCONNECTED
        self._ws: WebSocketClientProtocol | None = None
        self._subscription_id: str | None = None
        self._current_endpoint_index = 0
        self._reconnect_attempts = 0
        self._running = False
        self._last_block_seen: int = 0

    @property
    def state(self) -> ConnectionState:
        """Current connection state"""
        return self._state

    @property
    def last_block_seen(self) -> int:
        """Last block number seen (useful for catch-up after reconnection)"""
        return self._last_block_seen

    async def start(self) -> None:
        """
        Start the WebSocket listener.

        Runs indefinitely until stop() is called or max reconnection
        attempts are reached.
        """
        self._running = True
        self.logger.info("websocket_listener_starting")

        while self._running:
            try:
                await self._connect_and_listen()
            except Exception as e:
                self.logger.error("listener_error", error=str(e))

                if not self._running:
                    break

                # Try to reconnect
                if not await self._handle_reconnect():
                    # Max attempts reached
                    self.logger.error("max_reconnect_attempts_reached")
                    self._state = ConnectionState.FAILED
                    if self.on_connection_lost:
                        await self.on_connection_lost()
                    break

    async def stop(self) -> None:
        """Stop the listener gracefully"""
        self.logger.info("websocket_listener_stopping")
        self._running = False

        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

        self._state = ConnectionState.DISCONNECTED

    async def _connect_and_listen(self) -> None:
        """Connect to WebSocket and listen for events"""
        endpoint = self._get_current_endpoint()
        self._state = ConnectionState.CONNECTING

        self.logger.info("connecting_to_endpoint", endpoint=endpoint[:50] + "...")

        try:
            async with connect(
                endpoint,
                ping_interval=30,
                ping_timeout=10,
                close_timeout=5,
            ) as ws:
                self._ws = ws
                self._state = ConnectionState.CONNECTED
                self._reconnect_attempts = 0  # Reset on successful connection

                # Subscribe to logs
                await self._subscribe(ws)

                # Message loop
                async for message in ws:
                    if not self._running:
                        break
                    await self._handle_message(message)

        except ConnectionClosed as e:
            self.logger.warning(
                "websocket_closed",
                code=e.code if hasattr(e, "code") else "unknown",
                reason=str(e),
            )
            raise
        except Exception as e:
            self.logger.error("websocket_error", error=str(e))
            raise

    async def _subscribe(self, ws: WebSocketClientProtocol) -> None:
        """Send eth_subscribe request for OrderFilled logs"""
        self._state = ConnectionState.SUBSCRIBING

        subscription_request = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_subscribe",
            "params": [
                "logs",
                {"address": MONITORED_CONTRACTS, "topics": [ORDER_FILLED_TOPIC]},
            ],
        }

        await ws.send(json.dumps(subscription_request))

        # Wait for subscription confirmation
        response = await asyncio.wait_for(ws.recv(), timeout=10)
        response_data = json.loads(response)

        if "result" in response_data:
            self._subscription_id = response_data["result"]
            self._state = ConnectionState.SUBSCRIBED
            self.logger.info(
                "subscribed_to_logs",
                subscription_id=self._subscription_id,
                contracts_count=len(MONITORED_CONTRACTS),
            )
        else:
            error = response_data.get("error", {})
            error_msg = error.get("message", str(error))
            raise Exception(f"Subscription failed: {error_msg}")

    async def _handle_message(self, message: str) -> None:
        """Process a WebSocket message"""
        try:
            data = json.loads(message)

            # Check if it's a subscription notification
            if data.get("method") != "eth_subscription":
                return

            params = data.get("params", {})
            if params.get("subscription") != self._subscription_id:
                return

            log = params.get("result", {})

            # Update last block seen
            block_number = log.get("blockNumber")
            if block_number:
                block_int = (
                    int(block_number, 16)
                    if isinstance(block_number, str)
                    else block_number
                )
                if block_int > self._last_block_seen:
                    self._last_block_seen = block_int

            # Decode and process event
            decoded = self.decoder.decode_order_filled(log)

            if decoded:
                self.logger.debug(
                    "event_received",
                    tx_hash=decoded.tx_hash[:16] + "...",
                    maker=decoded.maker[:10] + "...",
                    side=decoded.side,
                    usdc=(
                        decoded.maker_amount
                        if decoded.is_maker_usdc
                        else decoded.taker_amount
                    ),
                )
                await self.on_event(decoded)

        except json.JSONDecodeError:
            self.logger.warning("invalid_json_message")
        except Exception as e:
            self.logger.error("message_handling_error", error=str(e))

    async def _handle_reconnect(self) -> bool:
        """
        Handle reconnection logic.

        Returns:
            True if should continue trying, False if max attempts reached
        """
        self._reconnect_attempts += 1

        if self._reconnect_attempts > self.settings.ws_reconnect_max_attempts:
            return False

        self._state = ConnectionState.RECONNECTING

        # Calculate exponential backoff delay
        delay = min(
            self.settings.ws_reconnect_delay_base
            * (2 ** (self._reconnect_attempts - 1)),
            self.settings.ws_reconnect_delay_max,
        )

        self.logger.warning(
            "reconnecting",
            attempt=self._reconnect_attempts,
            max_attempts=self.settings.ws_reconnect_max_attempts,
            delay_seconds=delay,
        )

        await asyncio.sleep(delay)

        # Rotate to next endpoint every 3 attempts
        if self._reconnect_attempts % 3 == 0:
            self._rotate_endpoint()

        return True

    def _get_current_endpoint(self) -> str:
        """Get current WebSocket endpoint"""
        endpoints = self.settings.polygon_wss_endpoints
        return endpoints[self._current_endpoint_index % len(endpoints)]

    def _rotate_endpoint(self) -> None:
        """Rotate to next available endpoint"""
        endpoints = self.settings.polygon_wss_endpoints
        self._current_endpoint_index = (self._current_endpoint_index + 1) % len(
            endpoints
        )
        self.logger.info(
            "rotating_endpoint",
            new_endpoint=self._get_current_endpoint()[:50] + "...",
        )
