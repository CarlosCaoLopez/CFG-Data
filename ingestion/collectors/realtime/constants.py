# Constants for on-chain Polymarket event listening
#
# Contracts:
# - CTF Exchange: Binary YES/NO markets
# - NegRisk CTF Exchange: Multi-outcome markets
#
# Event monitored: OrderFilled

from eth_hash.auto import keccak

# ===================
# Contract Addresses (Polygon Mainnet)
# ===================

# Polymarket CTF Exchange (binary YES/NO markets)
# https://polygonscan.com/address/0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e
CTF_EXCHANGE_ADDRESS = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"

# Polymarket NegRisk CTF Exchange (multi-outcome markets)
# https://polygonscan.com/address/0xC5d563A36AE78145C45a50134d48A1215220f80a
NEGRISK_CTF_EXCHANGE_ADDRESS = "0xC5d563A36AE78145C45a50134d48A1215220f80a"

# USDC on Polygon (6 decimals)
USDC_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
USDC_DECIMALS = 6

# ===================
# Event Signatures
# ===================

# OrderFilled event signature:
# OrderFilled(
#     bytes32 indexed orderHash,
#     address indexed maker,
#     address indexed taker,
#     uint256 makerAssetId,
#     uint256 takerAssetId,
#     uint256 makerAmountFilled,
#     uint256 takerAmountFilled,
#     uint256 fee
# )
ORDER_FILLED_SIGNATURE = (
    "OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)"
)
ORDER_FILLED_TOPIC = "0x" + keccak(ORDER_FILLED_SIGNATURE.encode()).hex()

# ===================
# ABI Types for Decoding
# ===================

# Non-indexed parameters of OrderFilled event (decoded from 'data' field)
# Indexed parameters (orderHash, maker, taker) are in 'topics'
ORDER_FILLED_DATA_TYPES = [
    "uint256",  # makerAssetId (0 = USDC, else = token ID)
    "uint256",  # takerAssetId (0 = USDC, else = token ID)
    "uint256",  # makerAmountFilled (6 decimals for USDC)
    "uint256",  # takerAmountFilled (6 decimals for USDC)
    "uint256",  # fee (6 decimals)
]

# ===================
# Monitored Addresses (lowercase for comparison)
# ===================

MONITORED_CONTRACTS = [
    CTF_EXCHANGE_ADDRESS.lower(),
    NEGRISK_CTF_EXCHANGE_ADDRESS.lower(),
]
