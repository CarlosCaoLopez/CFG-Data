import logging
import os
from fastapi import FastAPI, Depends, Header, HTTPException
from pydantic import BaseModel, Field
from typing import Literal
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from database.connection import get_session
from database.manager import DatabaseManager

app = FastAPI(title="Receptor de Resultados R")
logger = logging.getLogger(__name__)

_API_KEY = os.getenv("API_KEY", "")


async def require_api_key(x_api_key: str | None = Header(default=None)):
    if not _API_KEY:
        return  # no key configured → open (dev mode)
    if x_api_key != _API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


# --- Request / Response schemas ---

class InsiderWalletResult(BaseModel):
    """A single wallet detected as insider in a market."""
    wallet: str = Field(..., description="Wallet address (lowercase, 0x-prefixed)")
    categories_insider: list[str] = Field(
        ..., description="Categories where insider was detected"
    )
    command_type: Literal["ADD_WALLET", "UPDATE_CATEGORIES", "REMOVE_WALLET"] = Field(
        default="ADD_WALLET"
    )


class InsiderDetectionBatch(BaseModel):
    """Batch of insider detection results for a single market."""
    market_id: str = Field(..., description="Market condition ID that was analyzed")
    wallets: list[InsiderWalletResult] = Field(
        ..., min_length=1, description="Wallets detected as insiders"
    )


class InsiderDetectionResponse(BaseModel):
    status: Literal["accepted", "partial_failure", "error"]
    market_id: str
    wallets_updated: int
    tasks_dispatched: int
    errors: list[str] | None = None


# --- Endpoints ---

@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/insider-detection", response_model=InsiderDetectionResponse, dependencies=[Depends(require_api_key)])
async def receive_insider_detection(
    batch: InsiderDetectionBatch,
    session: AsyncSession = Depends(get_session),
):
    """
    Receives insider detection results from the calculation engine.

    For each wallet in the batch:
    1. Updates wallets table: insider=True, categories_insider, marketids_insider
    2. Dispatches Celery task T3 to publish InsiderTradeCommand to Redis stream
    """
    db = DatabaseManager(session)
    wallets_updated = 0
    tasks_dispatched = 0
    errors: list[str] = []

    for wallet_result in batch.wallets:
        # 1. Update DB
        try:
            await db.upsert_wallet({
                "address": wallet_result.wallet,
                "insider": True,
                "categories_insider": wallet_result.categories_insider,
                "marketids_insider": [batch.market_id],
            })
            wallets_updated += 1
        except Exception as e:
            msg = f"DB update failed for {wallet_result.wallet}: {e}"
            logger.error(msg, exc_info=True)
            errors.append(msg)
            continue

        # 2. Dispatch T3
        try:
            from workers.tasks import publish_insider_command
            publish_insider_command.delay({
                "wallet": wallet_result.wallet,
                "categories_insider": wallet_result.categories_insider,
                "market_ids_insider": [batch.market_id],
                "command_type": wallet_result.command_type,
            })
            tasks_dispatched += 1
        except Exception as e:
            msg = f"T3 dispatch failed for {wallet_result.wallet}: {e}"
            logger.error(msg, exc_info=True)
            errors.append(msg)

    # 3. Delete closed_market_variables for this market (architecture: E cleans CMV after D)
    try:
        await session.execute(
            text("DELETE FROM closed_market_variables WHERE market_id = :mid"),
            {"mid": batch.market_id},
        )
        await session.commit()
        logger.info(f"CMV deleted for market_id={batch.market_id}")
    except Exception as e:
        logger.error(f"CMV cleanup failed for {batch.market_id}: {e}", exc_info=True)

    # 4. Dispatch T4: delete trades for this processed market
    try:
        from workers.tasks import cleanup_market
        cleanup_market.delay(batch.market_id)
        logger.info(f"T4 dispatched for market_id={batch.market_id}")
    except Exception as e:
        logger.error(f"T4 dispatch failed for {batch.market_id}: {e}", exc_info=True)

    if wallets_updated == 0:
        status = "error"
    elif errors:
        status = "partial_failure"
    else:
        status = "accepted"

    return InsiderDetectionResponse(
        status=status,
        market_id=batch.market_id,
        wallets_updated=wallets_updated,
        tasks_dispatched=tasks_dispatched,
        errors=errors if errors else None,
    )
