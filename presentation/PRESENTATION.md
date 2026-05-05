# CFG Data — Insider Detection in Prediction Markets
### 5-minute talk outline · Follow the data pipeline

---

## 0. Hook (30s)

- Polymarket: a decentralized prediction market platform where users bet on real-world events
- All trades are public on-chain — wallets, amounts, timing, outcomes
- **The question:** can we automatically detect wallets that consistently trade with non-public information (insiders)?
- This system does exactly that — end to end, in near real-time

---

## 1. Ingestion Layer — Layer A (60s)

### L1: Market Close Listener
- Three concurrent async loops running at all times:
  - **WebSocket loop**: listens to on-chain `OrderFilled` events from Polygon RPC — this is the only way to get wallet addresses in real-time (Polymarket's public WS is anonymous)
  - **Gamma loop** (every 15 min): catch-all query to the Gamma API for markets with low activity the WebSocket would miss
  - **Close detection loop** (every 60s): polls each tracked market; when a market closes → triggers the full data collection
- On close: fetches all trades, all wallet ages (from Polymarket Data API + Etherscan V2 on Polygon), normalizes everything into a single `MarketDataBatch`, and pushes it to Redis

### L2: Insider Trades Listener
- Activated only after a wallet is flagged as insider
- Receives `ADD_WALLET` / `UPDATE_CATEGORIES` / `REMOVE_WALLET` commands from the pipeline via Redis
- Polls new trades every 30s per monitored wallet, filters by the categories where that wallet is known to be insider, and publishes them for storage

### Design decision: why two discovery strategies?
- WebSocket catches markets the moment they have on-chain activity → low latency
- Gamma API polling is the safety net for quiet markets → no gaps

---

## 2. Transport Layer — Redis Streams, Layer B (20s)

- 4 independent streams: `closed_markets`, `raw_data` (backfill), `insider_trades`, `insider_commands`
- Consumer groups with XREADGROUP → at-least-once delivery, explicit ACK
- Decouples ingestion from processing entirely — L1/L2 and the Orchestrator can crash independently

---

## 3. Persistence — Orchestrator Q + Database C (40s)

- The **Orchestrator** (plain async service, not Celery) reads from Redis streams and persists to PostgreSQL
- Strict FK insertion order: wallets → markets → trades (no orphan rows)
- `ON CONFLICT DO NOTHING` for trades — immutable once written
- 5 tables: `wallets`, `markets`, `trades`, `insider_trades`, and a **temporary table** `closed_market_variables`
- Key wallet fields: `first_seen_at`, `insider` (bool), `categories_insider`, `marketids_insider`
- After persisting a market batch, the Orchestrator dispatches **Task T2** to Celery

---

## 4. Statistical Engine — Tasks T2 + Motor D (60s)

### T2: Compute Variables (SQL, Celery)
- Runs a SQL script that populates `closed_market_variables` with 5 statistical features per wallet per market:
  - **Δt** — hours between the wallet's last trade and market close (how close to the wire?)
  - **Age** — days since the wallet's first on-chain transaction (is it a burner?)
  - **Winning volume** — net USDC placed on the correct outcome (did they bet big on the winner?)
  - **α_slippage** — wallet's share of total market volume (did they move the market?)
  - **Entropy H** — portfolio diversity across categories (currently disabled, needs more data)

### Motor D: Inference (R + Plumber)
- Receives only `market_id` via HTTP → reads `closed_market_variables` directly from the DB
- Computes **empirical percentile thresholds** for each variable across the full dataset
- **Insider criterion**: a wallet must simultaneously be in the critical region of all 4 active variables (Δt ≤ P1, Age ≤ P1, Winning_volume ≥ P99, α_slippage ≥ P99)
- Design decision: **empirical percentiles instead of parametric distributions** — robust to Polymarket's fat-tailed, non-normal data with no distribution assumptions needed

---

## 5. Alerting and Feedback Loop — Layer E + Tasks T3/T4 (40s)

- Motor D calls **Layer E (FastAPI REST API)** only if insiders are found
- E updates the wallet record: sets `insider = true`, stores `categories_insider` and `marketids_insider`
- E immediately cleans up `closed_market_variables` for that market (inline, synchronous)
- E dispatches **T3**: publishes an `ADD_WALLET` command to Redis → L2 starts monitoring that wallet's future trades
- E dispatches **T4 reactive**: deletes all trades from the `trades` table for that market (no longer needed)
- **T4 periodic** (Celery Beat, every hour): safety net — cleans up any trades that were never processed (pipeline gaps, crashes)

### The feedback loop
- Once flagged, insider wallets are monitored continuously by L2
- Their future trades land in `insider_trades` (separate table, kept long-term)
- This closes the loop: detection → monitoring → evidence accumulation

---

## 6. Dashboard — Layer F (20s)

- FastAPI + HTML templates, port 8002
- Shows: active markets, flagged insider wallets, computed variables, pipeline health
- SSE (Server-Sent Events) for live updates without polling

---

## 7. Key Architectural Decisions (30s)

| Decision | Why |
|---|---|
| WebSocket on-chain (Polygon RPC) | Only way to get wallet addresses in real-time |
| Redis Streams with consumer groups | Reliable async decoupling, survives restarts |
| R for inference | Mature statistical libraries, fast percentile computation |
| Empirical percentiles (not parametric) | No distribution assumptions, works with small N |
| Temporary `closed_market_variables` | Trades are ephemeral data; only the verdict matters long-term |
| Dual discovery (WS + Gamma) | Zero-gap coverage across all market activity levels |

---

## Summary

```
Polygon WebSocket → L1 discovers + collects on market close
                          ↓ Redis closed_markets
                    Orchestrator persists (wallets → markets → trades)
                          ↓ Celery T2
                    SQL computes 5 variables per wallet
                          ↓ HTTP POST /analyze
                    R Motor: empirical percentiles → insider criterion
                          ↓ HTTP POST /insider-detection (if insiders found)
                    FastAPI E: flags wallets, cleans up, triggers monitoring
                          ↓ Redis insider_commands
                    L2 monitors flagged wallets' future trades continuously
```
