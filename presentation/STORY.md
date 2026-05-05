# CFG Data — The Insider Hunter
### Story-driven talk · 5 minutes

---

## ACT I: The Scene (45s)

- It's February 9th, 2025. Super Bowl halftime show
- Rumors are swirling: Bad Bunny might perform. Nothing confirmed. Pure speculation.
- On Polymarket, a market opens: *"Will Bad Bunny perform at the Super Bowl halftime show?"*
- The odds sit at 30%. Plausible, but far from certain.
- Then, 48 hours before the show, a wallet appears
  - Places a $35,000 bet. All-in. At 31 cents.
  - No hedging. Maximum conviction.
- Bad Bunny performs. The wallet collects $112,000. A clean $77K profit.
- **Was it luck? Maybe. But what if this wallet did it 9 times in a row?**

- That's the problem: Polymarket is fully public — every trade, every wallet, every timestamp is on-chain
- And that transparency cuts both ways: we can see the suspicious patterns too
- **CFG Data is the system that finds them automatically**

---

## ACT II: The Challenge (30s)

- Every day, hundreds of markets close on Polymarket — sports, politics, crypto, pop culture
- Each market has dozens to hundreds of wallets trading in it
- The signals are subtle: not just "did they win?" but *when* did they bet, *how much*, *how young is the wallet*, *how focused is their portfolio*
- You can't review this manually. You need a pipeline that:
  - Watches every market, 24/7
  - Collects all trades the moment a market closes
  - Runs statistical analysis automatically
  - And keeps watching the flagged wallets forever after

- **That's exactly what we built**

---

## ACT III: Following the Data (3 min)

### Step 1 — Spotting the market in real-time

- We have two eyes on the market at all times
- **Eye #1 — the blockchain**: we listen to raw `OrderFilled` events directly from the Polygon network via WebSocket
  - This is the only way to get wallet addresses — Polymarket's own WebSocket is anonymous
  - Every on-chain trade tells us: *"this market is active, and here's who's trading in it"*
- **Eye #2 — the API**: every 15 minutes, a sweep of the Gamma API catches quiet markets that haven't traded recently and would slip through the WebSocket
- Every 60 seconds, we check: did any tracked market just close?

### Step 2 — The market closes. Go.

- The moment a market is marked as closed, a collection pipeline fires:
  - Fetch **every single trade** ever placed on that market
  - For every wallet that traded: look up its **age** — not just on Polymarket, but on the Polygon blockchain itself (via Etherscan V2)
    - Why? Because a wallet created three days ago for a single large bet is a red flag. A five-year-old wallet is not.
  - Normalize everything into a clean data contract and push it to **Redis**

### Step 3 — Redis: the backbone that keeps everything decoupled

- Four independent streams: closed markets, historical backfill, insider trades, insider commands
- Consumer groups with explicit ACK — if the processor crashes, nothing is lost
- This is the seam between "collecting data" and "doing something with it"

### Step 4 — Persisting to the database

- The **Orchestrator** reads from Redis and writes to PostgreSQL
- Strict insertion order: wallets first, then markets, then trades — foreign key constraints respected every time
- Trades are immutable once written: `ON CONFLICT DO NOTHING`
- After writing, the Orchestrator fires one Celery task: **T2**

### Step 5 — Five numbers that tell a story

- T2 runs a SQL script that computes five variables per wallet, per market:
  - **Δt** — how many hours before close did they make their last trade? (the closer, the more suspicious)
  - **Age** — how old is the wallet on-chain? (in days — burner wallets are young)
  - **Winning volume** — how much net USDC did they bet on the correct outcome? (weighted by direction: BUY/SELL, winner/loser)
  - **α_slippage** — what fraction of the total market volume is theirs? (did they move the market?)
  - **Entropy** — how diversified is their betting portfolio across categories? (a focused wallet = more suspicious)

- These five numbers go into a temporary table. They live there just long enough to be judged.

### Step 6 — The judge (written in R)

- Our statistical engine is built in R with Plumber — battle-tested statistical tooling
- It receives only one thing: the `market_id`
- It reads the variables directly from the database and applies **empirical percentile thresholds**:
  - No parametric distribution assumptions — Polymarket data is fat-tailed and weird
  - Δt ≤ 1st percentile (traded extremely close to close)
  - Age ≤ 1st percentile (wallet is extremely young)
  - Winning volume ≥ 99th percentile (bet extremely large on the winner)
  - α_slippage ≥ 99th percentile (moved the market significantly)
- **The verdict**: if a wallet is simultaneously in all four critical regions → insider
- A wallet that just got lucky will hit one or two. An insider hits all four.

### Step 7 — The alert fires

- If insiders are found, the R engine calls our FastAPI endpoint with the results
- The API does four things immediately:
  - Marks the wallet as `insider = true` in the database
  - Records which categories and which markets they were flagged in
  - Cleans up the temporary variable table (no longer needed)
  - Dispatches a command to start **watching that wallet forever**

### Step 8 — The feedback loop closes

- A Redis command goes out: *"Start monitoring this wallet"*
- **L2**, our insider watcher, picks it up and adds the wallet to its surveillance list
- Every 30 seconds, it checks: did this wallet make any new trades?
- If yes, and if those trades are in the categories where they're flagged — the trades are stored permanently in `insider_trades`
- The wallet is now under continuous observation
- **And if they do it again, we see it in real-time**

---

## ACT IV: The Full Picture (30s)

```
On-chain event fires → L1 spots the market
Market closes → all trades + wallet ages collected → Redis
Orchestrator persists to PostgreSQL
T2 computes 5 statistical variables per wallet (SQL)
R engine applies percentile criterion → insider verdict
FastAPI flags wallet, cleans up, triggers monitoring
L2 watches flagged wallets' future trades in real-time
                         ↕
               Dashboard shows everything live
```

- Every layer is independently restartable — Redis absorbs the gaps
- The system runs 24/7 without human intervention
- Historical markets can be backfilled: same pipeline, same logic, different entry point

---

## ACT V: Back to our wallet (15s)

- Remember the wallet that bet $35K on Bad Bunny at the Super Bowl?
- With CFG Data running, the moment that market closed:
  - Its Δt, age, winning volume, and slippage were computed automatically
  - The R engine flagged it as an outlier on all four dimensions
  - It was marked as insider. It went under surveillance.
  - **Its next trade was already being watched before Bad Bunny left the stage**

---

*The data was always there. We just built the machine to read it.*
