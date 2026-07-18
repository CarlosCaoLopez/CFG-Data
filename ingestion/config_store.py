"""
Durable file-backed store for the category filter (CATEGORIAS_DE_INTERES).

Why not Redis: Redis in this stack runs with `--save "" --appendonly no`
(see docker-compose.yml) — purely in-memory, so a `docker compose down` wipes
it. That's fine for the streams (transient, MAXLEN-capped, replayable from
Postgres) but it silently reset the category filter on every redeploy. Config
is durable state, not a transient queue, so it lives in its own JSON file on a
durable named volume, independent of Redis' lifecycle.

Single writer: only the ingestion container's InterestsRegistry touches this
file. The dashboard reads/writes the filter over HTTP (stats server), never the
file directly, so there is no cross-process file contention.
"""

import asyncio
import json
import os
import tempfile

import structlog

logger = structlog.get_logger()


class CategoriesStore:
    """Reads/writes the category filter as a JSON list at `path`."""

    def __init__(self, path: str):
        self._path = path
        self.logger = logger.bind(component="categories_store", path=path)

    async def get(self) -> list[str] | None:
        """Return the persisted list, or None if the file doesn't exist yet."""
        return await asyncio.to_thread(self._read)

    async def set(self, categories: list[str]) -> None:
        """Persist the list atomically (temp file + os.replace)."""
        await asyncio.to_thread(self._write, list(categories))

    def _read(self) -> list[str] | None:
        try:
            with open(self._path, encoding="utf-8") as f:
                data = json.load(f)
        except FileNotFoundError:
            return None
        except (json.JSONDecodeError, OSError) as e:
            self.logger.warning("categories_store_read_failed", error=str(e))
            return None
        if isinstance(data, list):
            return [str(c) for c in data]
        self.logger.warning("categories_store_bad_shape", raw=data)
        return None

    def _write(self, categories: list[str]) -> None:
        # Atomic write: dump to a temp file in the same dir, then rename over
        # the target so a crash mid-write can never leave a truncated file.
        directory = os.path.dirname(self._path) or "."
        os.makedirs(directory, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=directory, suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(categories, f)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self._path)
        except OSError:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
