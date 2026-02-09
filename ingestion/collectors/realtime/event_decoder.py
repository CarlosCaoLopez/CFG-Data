# Event decoder for Polymarket OrderFilled events
#
# Decodes raw Polygon logs into structured DecodedOrderFilled objects.
# Handles both WebSocket and Etherscan log formats.

from dataclasses import dataclass
from eth_abi import decode
import structlog

from .constants import ORDER_FILLED_DATA_TYPES, USDC_DECIMALS

logger = structlog.get_logger()


@dataclass
class DecodedOrderFilled:
    """Decoded OrderFilled event from Polymarket contracts"""

    # Transaction info
    tx_hash: str
    block_number: int
    log_index: int
    contract_address: str

    # Indexed parameters (from topics)
    order_hash: str
    maker: str  # Wallet that created the order
    taker: str  # Wallet that filled the order

    # Non-indexed parameters (from data)
    maker_asset_id: int  # 0 = USDC, else = token ID
    taker_asset_id: int  # 0 = USDC, else = token ID
    maker_amount: float  # Amount given by maker (converted from 6 decimals)
    taker_amount: float  # Amount received by maker (converted from 6 decimals)
    fee: float  # Fee paid (converted from 6 decimals)

    # Derived fields
    side: str  # "BUY" or "SELL"
    is_maker_usdc: bool  # True if maker is giving USDC (buying tokens)


class EventDecoder:
    """
    Decodes OrderFilled event logs from Polygon.

    The OrderFilled event has:
    - 3 indexed parameters (in topics): orderHash, maker, taker
    - 5 non-indexed parameters (in data): makerAssetId, takerAssetId,
      makerAmountFilled, takerAmountFilled, fee

    Log format from WebSocket:
    {
        "transactionHash": "0x...",
        "blockNumber": "0x...",  # hex string
        "logIndex": "0x...",     # hex string
        "address": "0x...",
        "topics": ["0x...", "0x...", "0x...", "0x..."],
        "data": "0x..."
    }

    Log format from Etherscan:
    {
        "transactionHash": "0x...",
        "blockNumber": "12345678",  # decimal string
        "logIndex": "5",            # decimal string
        "address": "0x...",
        "topics": ["0x...", "0x...", "0x...", "0x..."],
        "data": "0x..."
    }
    """

    def __init__(self):
        self.logger = logger.bind(component="event_decoder")

    def decode_order_filled(self, log: dict) -> DecodedOrderFilled | None:
        """
        Decode an OrderFilled event log.

        Args:
            log: Raw log from WebSocket or Etherscan API

        Returns:
            DecodedOrderFilled or None if decoding fails
        """
        try:
            # Extract basic transaction info
            tx_hash = self._normalize_hex(log.get("transactionHash", ""))
            block_number = self._to_int(log.get("blockNumber", 0))
            log_index = self._to_int(log.get("logIndex", 0))
            contract_address = log.get("address", "").lower()

            topics = log.get("topics", [])
            data = log.get("data", "0x")

            # Validate topics count
            if len(topics) < 4:
                self.logger.warning(
                    "invalid_topics_count",
                    tx_hash=tx_hash[:20] if tx_hash else "unknown",
                    topics_count=len(topics),
                )
                return None

            # Decode indexed parameters from topics
            # topics[0] = event signature hash (OrderFilled)
            # topics[1] = orderHash (bytes32)
            # topics[2] = maker (address, left-padded to 32 bytes)
            # topics[3] = taker (address, left-padded to 32 bytes)
            order_hash = self._normalize_hex(topics[1])
            maker = self._extract_address_from_topic(topics[2])
            taker = self._extract_address_from_topic(topics[3])

            # Decode non-indexed parameters from data
            data_hex = data[2:] if data.startswith("0x") else data
            if not data_hex:
                self.logger.warning("empty_data_field", tx_hash=tx_hash[:20])
                return None

            data_bytes = bytes.fromhex(data_hex)
            decoded_data = decode(ORDER_FILLED_DATA_TYPES, data_bytes)

            maker_asset_id = decoded_data[0]
            taker_asset_id = decoded_data[1]
            maker_amount_raw = decoded_data[2]
            taker_amount_raw = decoded_data[3]
            fee_raw = decoded_data[4]

            # Convert amounts from 6 decimals (USDC)
            maker_amount = maker_amount_raw / (10**USDC_DECIMALS)
            taker_amount = taker_amount_raw / (10**USDC_DECIMALS)
            fee = fee_raw / (10**USDC_DECIMALS)

            # Determine trade side
            # makerAssetId = 0 means maker is giving USDC (BUY order)
            # makerAssetId != 0 means maker is giving tokens (SELL order)
            is_maker_usdc = maker_asset_id == 0
            side = "BUY" if is_maker_usdc else "SELL"

            return DecodedOrderFilled(
                tx_hash=tx_hash,
                block_number=block_number,
                log_index=log_index,
                contract_address=contract_address,
                order_hash=order_hash,
                maker=maker,
                taker=taker,
                maker_asset_id=maker_asset_id,
                taker_asset_id=taker_asset_id,
                maker_amount=maker_amount,
                taker_amount=taker_amount,
                fee=fee,
                side=side,
                is_maker_usdc=is_maker_usdc,
            )

        except Exception as e:
            self.logger.error(
                "decode_failed",
                error=str(e),
                tx_hash=log.get("transactionHash", "unknown")[:20],
            )
            return None

    def _normalize_hex(self, value: str | bytes) -> str:
        """Normalize a hex value to lowercase string with 0x prefix"""
        if isinstance(value, bytes):
            value = value.hex()
        if not value:
            return ""
        if not value.startswith("0x"):
            value = "0x" + value
        return value.lower()

    def _extract_address_from_topic(self, topic: str | bytes) -> str:
        """
        Extract an address from a 32-byte topic.
        Addresses are left-padded with zeros to 32 bytes.
        """
        topic_hex = self._normalize_hex(topic)
        # Remove 0x prefix, take last 40 chars (20 bytes = address)
        address = "0x" + topic_hex[-40:]
        return address.lower()

    def _to_int(self, value: str | int) -> int:
        """Convert hex string or int to int"""
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            value = value.strip()
            if not value:
                return 0
            if value.startswith("0x"):
                return int(value, 16)
            return int(value)
        return 0
