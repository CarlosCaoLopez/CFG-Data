"""
Durable file-backed persistence for L1's market-tracking state: the markets
currently being watched for closure, and the markets already processed
(closed + published). Mirrors MarketCloseListener's in-memory
`_tracked_markets` dict and `_processed_markets` set.

Why not Redis: same reasoning as ingestion/config_store.py — Redis in this
stack runs purely in-memory (`--save "" --appendonly no` in
docker-compose.yml), so a `docker compose down` wiped both sets and forced L1
to rediscover every open market from scratch on every redeploy (this stalled
ingestion 3 days on the Pi once, see CLAUDE.md). Persistence here is
best-effort and non-blocking, same contract the old Redis mirror had: a
failure just logs a warning and L1 keeps operating purely in memory.

Minimal footprint: only what pruning-on-filter-change (_categories_match) and
the dashboard's tracked-markets widget need is persisted per tracked market —
question, category, tags — not the full raw Gamma API blob (tokens, prices,
images, description, ...) that the old Redis hash stored in full.

tracked_markets.json is small (bounded by currently-open tracked markets) and
rewritten in full on every add/remove — cheap at this size, and callers batch
their writes (one save per discovery sweep / prune pass) instead of one per
market to limit SD card wear on the Pi. processed_markets.log only ever
grows, so it's append-only (one condition_id per line) to avoid an O(n)
rewrite every time a market closes.
"""

import asyncio
import json
import os
import tempfile

import structlog

logger = structlog.get_logger()


class TrackedMarketsStore:
    def __init__(self, tracked_path: str, processed_path: str):
        self._tracked_path = tracked_path
        self._processed_path = processed_path
        self.logger = logger.bind(component="tracked_markets_store")

    # ================================================================
    # Tracked markets: condition_id -> {question, category, tags}
    # ================================================================

    async def load_tracked(self) -> dict[str, dict]:
        return await asyncio.to_thread(self._read_tracked)

    async def save_tracked(self, tracked: dict[str, dict]) -> None:
        """Overwrite the file with the full current dict (best-effort)."""
        try:
            await asyncio.to_thread(self._write_tracked, dict(tracked))
        except OSError as e:
            self.logger.warning("save_tracked_failed", error=str(e))

    def _read_tracked(self) -> dict[str, dict]:
        try:
            with open(self._tracked_path, encoding="utf-8") as f:
                data = json.load(f)
        except FileNotFoundError:
            return {}
        except (json.JSONDecodeError, OSError) as e:
            self.logger.warning("tracked_read_failed", error=str(e))
            return {}
        return data if isinstance(data, dict) else {}

    def _write_tracked(self, tracked: dict[str, dict]) -> None:
        directory = os.path.dirname(self._tracked_path) or "."
        os.makedirs(directory, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=directory, suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(tracked, f)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self._tracked_path)
        except OSError:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    # ================================================================
    # Processed markets: append-only set of condition_ids
    # ================================================================

    async def load_processed(self) -> set[str]:
        return await asyncio.to_thread(self._read_processed)

    async def add_processed(self, condition_id: str) -> None:
        """Append-only (best-effort): O(1) write, no full-file rewrite as the
        set grows without bound over the ingestion's lifetime."""
        try:
            await asyncio.to_thread(self._append_processed, condition_id)
        except OSError as e:
            self.logger.warning(
                "add_processed_failed", condition_id=condition_id, error=str(e)
            )

    def _read_processed(self) -> set[str]:
        try:
            with open(self._processed_path, encoding="utf-8") as f:
                return {line.strip() for line in f if line.strip()}
        except FileNotFoundError:
            return set()
        except OSError as e:
            self.logger.warning("processed_read_failed", error=str(e))
            return set()

    def _append_processed(self, condition_id: str) -> None:
        directory = os.path.dirname(self._processed_path) or "."
        os.makedirs(directory, exist_ok=True)
        with open(self._processed_path, "a", encoding="utf-8") as f:
            f.write(condition_id + "\n")
            f.flush()
            os.fsync(f.fileno())
