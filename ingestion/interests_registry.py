"""
Runtime source of truth for CATEGORIAS_DE_INTERES (the category filter).

Shared by every ingestion consumer that filters by category (L1, L2, ...).
Responsibilities:
  - Persist the filter to a durable JSON file (CategoriesStore) so edits
    survive redeploys. NOT Redis: Redis runs in-memory only here (--save ""),
    so a `docker compose down` wiped it and reset the filter every redeploy.
  - Notify all subscribers on change, so a single edit propagates LIVE to
    every consumer (L1 pruning + normalizer, L2 normalizer, ...).

The .env value (CATEGORIAS_DE_INTERES) is only the initial *seed*, used the
first time the store has no persisted value. Once seeded, the store is
authoritative.

Design: in-process observer. Subscribers register a `callback(list[str])` that
fires (a) immediately on subscribe with the current value, and (b) on every
subsequent change. This is what makes "a change in L1 forces a change in L2":
there is no code path that updates one consumer without notifying the rest.
"""

import structlog

from ingestion.config_store import CategoriesStore

logger = structlog.get_logger()


class InterestsRegistry:
    def __init__(self, store: CategoriesStore, seed: list[str]):
        self._store = store
        self._seed = list(seed)
        self._categories: list[str] = list(seed)
        self._subscribers: list = []
        self.logger = logger.bind(component="interests_registry")

    @property
    def categories(self) -> list[str]:
        """Current filter (copy, original casing)."""
        return list(self._categories)

    def subscribe(self, callback) -> None:
        """
        Register a callback(list[str]) fired on every change.

        Fires once immediately with the current value so the subscriber starts
        in sync without extra wiring.
        """
        self._subscribers.append(callback)
        self._safe_call(callback, self.categories)

    async def load(self) -> None:
        """
        Seed the registry from the durable store if present, else persist the
        .env seed so future restarts have a value to read. Notifies subscribers.
        """
        try:
            persisted = await self._store.get()
        except Exception as e:
            self.logger.warning("interests_load_failed", error=str(e))
            persisted = None

        if persisted:
            self._categories = persisted
            self.logger.info("interests_loaded_from_store", categories=persisted)
        else:
            try:
                await self._store.set(self._seed)
                self.logger.info("interests_seeded_from_env", categories=self._seed)
            except Exception as e:
                self.logger.warning("interests_seed_failed", error=str(e))
        self._notify()

    async def set(self, categories: list[str]) -> None:
        """Replace the filter: persist to the store + notify subscribers live."""
        self._categories = list(categories)
        try:
            await self._store.set(self._categories)
        except Exception as e:
            self.logger.warning("interests_persist_failed", error=str(e))
        self._notify()
        self.logger.info("interests_updated", categories=self._categories)

    def _notify(self) -> None:
        for cb in self._subscribers:
            self._safe_call(cb, self.categories)

    def _safe_call(self, callback, categories: list[str]) -> None:
        try:
            callback(categories)
        except Exception as e:
            self.logger.warning("interests_subscriber_failed", error=str(e))
