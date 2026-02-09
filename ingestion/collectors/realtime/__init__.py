# Realtime listener package for on-chain Polymarket events
#
# This package provides:
# - WebSocketListener: Primary real-time event streaming via Polygon RPC
# - PollingListener: Fallback using Etherscan API V2
# - RealtimeOrchestrator: Hybrid system coordinating both approaches
#
# Usage:
#     from ingestion.collectors.realtime import RealtimeOrchestrator
#
#     async with RealtimeOrchestrator() as orchestrator:
#         await orchestrator.run()

from .constants import (
    CTF_EXCHANGE_ADDRESS,
    NEGRISK_CTF_EXCHANGE_ADDRESS,
    ORDER_FILLED_TOPIC,
    ORDER_FILLED_SIGNATURE,
    USDC_DECIMALS,
    MONITORED_CONTRACTS,
)
from .event_decoder import EventDecoder, DecodedOrderFilled
from .websocket_listener import WebSocketListener, ConnectionState
from .polling_listener import PollingListener
from .orchestrator import RealtimeOrchestrator, LRUCache

__all__ = [
    # Constants
    "CTF_EXCHANGE_ADDRESS",
    "NEGRISK_CTF_EXCHANGE_ADDRESS",
    "ORDER_FILLED_TOPIC",
    "ORDER_FILLED_SIGNATURE",
    "USDC_DECIMALS",
    "MONITORED_CONTRACTS",
    # Event decoder
    "EventDecoder",
    "DecodedOrderFilled",
    # WebSocket listener
    "WebSocketListener",
    "ConnectionState",
    # Polling listener
    "PollingListener",
    # Orchestrator
    "RealtimeOrchestrator",
    "LRUCache",
]
