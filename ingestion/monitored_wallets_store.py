"""
Durable file-backed persistence for L2's monitored-insider-wallets state.
Mirrors InsiderTradesListener's in-memory `_monitored_wallets` dict.

Why not Redis: same reasoning as ingestion/config_store.py and
ingestion/tracked_markets_store.py — Redis in this stack runs purely
in-memory (`--save "" --appendonly no` in docker-compose.yml), so a
`docker compose down` wiped it and silently stopped L2 from tracking every
insider already detected until the next re-detection. Persistence here is
best-effort and non-blocking: a failure just logs a warning and L2 keeps
operating purely in memory.

monitored_wallets.json (dict wallet -> {categories_insider,
market_ids_insider, last_seen_ts}) is small (bounded by currently-monitored
insiders) and rewritten in full on every mutation — cheap at this size.
Callers batch their writes (one save per command / per poll sweep) instead of
one per wallet to limit SD card wear on the Pi, since `last_seen_ts` updates
on almost every 30s poll cycle for every monitored wallet.

scripts/seed_monitored_wallets.py (a separate, sync, one-shot script run from
the orchestrator container) reads the same file directly with plain `json`
to dedupe against already-monitored wallets before re-seeding — see that
file's `_already_monitored` helper.
"""

import asyncio
import json
import os
import tempfile

import structlog

logger = structlog.get_logger()


class MonitoredWalletsStore:
    def __init__(self, path: str):
        self._path = path
        self.logger = logger.bind(component="monitored_wallets_store")

    async def load(self) -> dict[str, dict]:
        return await asyncio.to_thread(self._read)

    async def save(self, monitored: dict[str, dict]) -> None:
        """Overwrite the file with the full current dict (best-effort)."""
        try:
            await asyncio.to_thread(self._write, dict(monitored))
        except OSError as e:
            self.logger.warning("save_monitored_failed", error=str(e))

    def _read(self) -> dict[str, dict]:
        try:
            with open(self._path, encoding="utf-8") as f:
                data = json.load(f)
        except FileNotFoundError:
            return {}
        except (json.JSONDecodeError, OSError) as e:
            self.logger.warning("monitored_read_failed", error=str(e))
            return {}
        return data if isinstance(data, dict) else {}

    def _write(self, monitored: dict[str, dict]) -> None:
        directory = os.path.dirname(self._path) or "."
        os.makedirs(directory, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=directory, suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(monitored, f)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self._path)
        except OSError:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
