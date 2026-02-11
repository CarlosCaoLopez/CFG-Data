# CLAUDE.md - Polymarket Insider Detection System

---

## 📐 Fuente de Verdad: Arquitectura

La arquitectura de referencia es **`ARQUITECTURA SERVILLETA.md`**.
Este documento define las 6 capas del sistema (A, B, C, D, E, T) y es la fuente de verdad para el diseño.

### Ejecución de Tests
**Los scripts de test (`scripts/test_*.py`) SIEMPRE los ejecuta el usuario.**
NO ejecutes comandos `poetry run python -m scripts.test_*` directamente.

---

## Visión General

Sistema de detección de insider trading en Polymarket. Se organiza en **6 capas** (A, B, C, D, E, T). Al cierre de un market, se calculan 5 variables estadísticas por wallet/trade. El **Motor de Cálculo (D)** ajusta distribuciones, obtiene regiones críticas y aplica el **criterio de insider**: una wallet que esté en las 5 regiones críticas simultáneamente se marca como insider.

---

## Variables del Criterio de Insider

Al cierre de un market, la **Task T2** calcula estas variables via SQL (tabla temporal `Closed_Market_Variables`):

| Variable | Fórmula | Unidad |
|----------|---------|--------|
| **Δt** | `Market.EndDate - Trade.Timestamp` | Horas |
| **Age** | `Max(1, Market.EndDate - Wallet.First_seen_at)` | Días |
| **Winning_volume** | `Σ(Trade.Size_usdc × Trade.Prize × ±1)` donde ±1 = +1 si `Trade.Outcome == Market.WinningOutcome`, -1 si no | USDC |
| **α_slippage** | `(Prize × (1 + Size_usdc / Market.Volume_usdc) - Prize) / Prize` (0 si Prize=0) | % (0-1) |
| **H (entropía)** | `-Σ(P × log(P))` donde P = volumen wallet en categoría / volumen total wallet | - |

### Criterio de Insider (Motor D)

El Motor de Cálculo (implementado en **R** con Plumber) recibe `{IDWallet, IDMarket, Δt, Age, Winning_volume, α_slippage, H}` y:

1. Ajusta bondad de distribución para cada variable (periódicamente)
2. Obtiene **región crítica** para cada variable
3. Aplica criterio: wallet en las **5 regiones críticas** simultáneamente → **insider**
4. Devuelve `{IDMarket, IDWallet, Categories_insider, MarketIDs_insider}` via HTTP a la REST API (E)

---

## APIs de Polymarket

### Endpoints Base

| API       | URL                                              | Propósito             |
| --------- | ------------------------------------------------ | --------------------- |
| Gamma     | `https://gamma-api.polymarket.com`               | Metadatos de mercados |
| CLOB      | `https://clob.polymarket.com`                    | Order book, precios   |
| Data      | `https://data-api.polymarket.com`                | Actividad de usuarios |
| WebSocket | `wss://ws-subscriptions-clob.polymarket.com/ws/` | Tiempo real           |

### Mapeo Variable → Endpoint

| Variable                 | Endpoint                                                | Campo                        |
| ------------------------ | ------------------------------------------------------- | ---------------------------- |
| V (volumen)              | `GET /activity?user={wallet}`                           | `usdcSize`                   |
| P_mkt (probabilidad)     | `GET /midpoint?token_id={id}`                           | `mid`                        |
| S (ganó/perdió)          | `GET /positions?user={wallet}&redeemable=true`          | existe = ganó                |
| Age (edad wallet)        | `GET /activity?user={wallet}&sortDirection=ASC&limit=1` | primer `timestamp`           |
| Δt_res (tiempo a cierre) | `GET /markets?condition_ids={id}`                       | `endDate`                    |
| H_portfolio (entropía)   | `GET /positions?user={wallet}`                          | calcular de `currentValue[]` |
| **category**             | `GET /events/{event_id}`                                | `category` (solo cerrados)   |
| **tags**                 | `GET /events/{event_id}`                                | `tags[].label` (siempre)     |

### ⚠️ Quirk de la Gamma API: category y tags

La API retorna **schemas diferentes** según el tipo de query:

| Query | `category` | `tags` |
|-------|-----------|--------|
| `?closed=true` | ✅ En market Y event | ✅ En event (a veces) |
| `?condition_ids=X` | ❌ No incluido | ❌ No incluido |
| `?clob_token_ids=X` | ❌ No incluido | ❌ No incluido |
| `GET /events/{id}` | ✅ Solo cerrados | ✅ **Siempre** (activos y cerrados) |

**Solución implementada (realtime):**
1. `GET /markets?clob_token_ids=X` → obtener `conditionId` + `events[0].id`
2. `GET /events/{event_id}` → obtener `tags[]` (siempre) + `category` (si cerrado)
3. Merge tags/category al dict del market antes de normalizar

**Para backfill:** `?closed=true` ya devuelve category y no necesita la segunda llamada.

### Consultar Mercados Históricos

```bash
# Mercados cerrados con volumen > $50K
GET /markets?closed=true&volume_num_min=50000&order=closedTime&ascending=false

# Trades de un mercado específico (abierto o cerrado)
GET /activity?market={conditionId}&type=TRADE

# Ganadores de un mercado
GET /positions?market={conditionId}&redeemable=true
```

---

## Etherscan API V2 (Wallet Age)

### ⚠️ Migración de Polygonscan a Etherscan V2

La API de Polygonscan (V1) está **deprecada**. Ahora usamos **Etherscan API V2** que unifica todas las chains bajo un solo endpoint.

| Versión | URL | Estado |
|---------|-----|--------|
| V1 (deprecated) | `https://api.polygonscan.com/api` | ❌ Ya no funciona |
| **V2 (actual)** | `https://api.etherscan.io/v2/api` | ✅ En uso |

### Configuración

```bash
# .env
ETHERSCAN_API_KEY=tu_api_key_aqui

# Rate limit (free tier = 3 calls/sec)
POLYGONSCAN_REQUESTS_PER_SECOND=3
```

### Chain IDs

El parámetro `chainid` especifica qué blockchain consultar:

| Chain | Chain ID | Uso |
|-------|----------|-----|
| Ethereum Mainnet | 1 | - |
| **Polygon PoS** | **137** | ✅ Polymarket |
| Polygon zkEVM | 1101 | - |
| Arbitrum | 42161 | - |
| Base | 8453 | - |

### Endpoint: Primera transacción de una wallet

```
GET https://api.etherscan.io/v2/api
    ?chainid=137                    # Polygon
    &module=account
    &action=txlist                  # o txlistinternal
    &address=0x...
    &startblock=0
    &endblock=99999999
    &sort=asc                       # Orden ascendente (más antigua primero)
    &page=1
    &offset=1                       # Solo 1 resultado
    &apikey=TU_API_KEY
```

### Tipos de transacciones

| Action | Qué detecta |
|--------|-------------|
| `txlist` | Transacciones normales (EOA → EOA) |
| `txlistinternal` | Transacciones internas (via Smart Contracts, bridges) |

**Importante:** Una wallet puede ser fondeada via bridge (ej: Polygon Bridge) y su primera actividad será una transacción **interna**, no normal. Por eso consultamos **ambos** endpoints y tomamos la fecha más antigua.

### Formato de fechas

Todas las fechas se manejan en formato **ISO 8601 UTC**:

```
YYYY-MM-DDTHH:MM:SS
```

Ejemplos:
- `2025-07-30T20:14:59` - Primera tx en Polygon
- `2025-07-31T11:46:36` - Primera tx en Polymarket

**Regla de coherencia:** `first_tx_polygon` ≤ `first_tx_polymarket` siempre, porque la wallet debe existir en Polygon antes de poder interactuar con Polymarket.

### WalletAgeCollector - Implementación

```python
# ingestion/collectors/wallet_age_collector.py

class WalletAgeCollector:
    """
    Obtiene edad de wallets desde dos fuentes:
    1. Polymarket Data API - Primera actividad en Polymarket
    2. Etherscan API V2 - Primera transacción en Polygon blockchain
    """

    async def fetch_wallet_age(self, wallet: str) -> dict:
        """
        Retorna:
        {
            "wallet": "0x...",
            "first_tx_polymarket": datetime | None,
            "first_tx_polygon": datetime | None,
        }
        """

    async def fetch_wallet_ages_batch(self, wallets: set[str]) -> dict[str, dict]:
        """Procesa múltiples wallets con rate limiting"""
```

### Manejo de Rate Limits

El código incluye **retry automático** cuando Etherscan devuelve error de rate limit:

```python
# Si recibe "Max calls per sec rate limit reached (3/sec)":
# 1. Espera 1 segundo
# 2. Reintenta (máximo 3 intentos)
```

### Ejemplo de respuesta

```json
{
  "wallet_ages": {
    "0xabc123...": {
      "wallet": "0xabc123...",
      "first_tx_polymarket": "2025-07-31T11:46:36",
      "first_tx_polygon": "2025-07-30T20:14:59"
    }
  }
}
```

En este ejemplo:
- La wallet existía en Polygon desde el 30 de julio 2025
- Empezó a usar Polymarket 1 día después (31 de julio)
- **Conclusión:** No es una "cuenta fantasma" creada para un trade específico

### Test del WalletAgeCollector

```bash
poetry run python -m scripts.test_wallet_age
```

Output esperado:
```
Etherscan API key configured: True

--- Wallet: 0x5117658505d0d... ---
  First Polymarket tx: 2026-02-03T18:10:36
  First Polygon tx: 2025-06-18T23:22:51    # 8 meses antes = wallet vieja ✅
```

---

## Arquitectura: Implementación Concreta

| Capa | Nombre | Tecnología | Archivos principales |
|------|--------|------------|----------------------|
| **(A)** | Ingesta de Datos | Python async | `ingestion/listeners/`, `ingestion/collectors/`, `ingestion/publisher.py` |
| **(B)** | Redis + Orchestrator | Redis Streams + Python async | `queue_system/orchestrator.py`, `queue_system/streams/` |
| **(C)** | Base de Datos | PostgreSQL (TimescaleDB) | `database/models/`, `database/manager.py` |
| **(D)** | Motor de Cálculo | R (Plumber) | `inference/market_inference.r` |
| **(E)** | REST API | FastAPI | `api/main.py` |
| **(T)** | Tasks | Python (Celery) | `workers/tasks.py` |

### Flujo completo de punta a punta

#### 1. L1: Descubrimiento y cierre de markets

`MarketCloseListener` (`ingestion/listeners/market_close_listener.py`) corre 3 loops concurrentes:

```
_ws_discovery_loop()  ← WebSocket on-chain Polygon (event_decoder.py: OrderFilled)
  → MarketsCollector.fetch_market_by_token_id()
  → MarketsCollector.fetch_event_by_id()         # siempre devuelve tags
  → si category/tags ∈ CATEGORIAS_DE_INTERES → _tracked_markets

_gamma_discovery_loop()  [cada 15 min — catch-all para markets sin trades recientes]
  → MarketsCollector.fetch_open_markets()
  → MarketsCollector.fetch_event_by_id()
  → si category/tags ∈ CATEGORIAS_DE_INTERES → _tracked_markets

_close_detection_loop()  [cada 60s]
  → MarketsCollector.fetch_market_by_id() por cada tracked market
  → si closed=True → _handle_market_close()
        → TradesCollector.fetch_trades_for_market()
        → WalletAgeCollector.fetch_wallet_ages_batch()
        → DataNormalizer.normalize_batch()  → MarketDataBatch
        → compute_winning_outcome() + mark_last_trade()
        → RedisPublisher.publish_closed_market_batch()
              → XADD closed_markets {payload, market_id, ...}
```

> **¿Por qué WebSocket on-chain?** El CLOB de Polymarket es anónimo — el WebSocket público no revela wallets. Se usa el WebSocket de Polygon (nodo RPC) para detectar eventos `OrderFilled` del contrato CTFExchange y así descubrir qué markets están activos.

#### 2. T1: Persistencia a BD (inline en el orchestrator)

`QueueOrchestrator` (`queue_system/orchestrator.py`) corre 2 loops concurrentes con `asyncio.gather`:

```
_run_market_loop()
  XREADGROUP closed_markets + raw_data  (group: scheduler)
  → _process_batch(message_id, MarketDataBatch, source_stream)
        → DatabaseManager.upsert_wallet()    # por cada wallet_age (FK primero)
        → DatabaseManager.upsert_market()
        → DatabaseManager.upsert_trade_batch()   # INSERT ON CONFLICT DO NOTHING
        → celery.send_task('workers.tasks.compute_variables', [market_id])  ← T2
        → source_stream.ack(group, message_id)

_run_insider_trades_loop()
  XREADGROUP insider_trades  (group: tasks)
  → _process_insider_trade(message_id, InsiderTradeData)
        → DatabaseManager.upsert_insider_trade_batch([_trade_to_dict(t) ...])
        → insider_trades_stream.ack(group, message_id)
```

`_trade_to_dict()` mapea `RawTrade.price` → columna `prize`, incluye `last_trade`.

#### 3. T2: Cálculo de variables (Celery)

```
compute_variables(market_id)  [workers/tasks.py]
  → ejecuta scripts/sql/compute_variables.sql
  → puebla closed_market_variables con Δt, Age, Winning_volume, α_slippage, H
  → run_market_inference.delay(market_id)  ← dispara Motor D
```

#### 4. Motor D: R Plumber (inference)

```
run_market_inference(market_id)  [workers/tasks.py → HTTP]
  → POST http://inference:8000/run?market_id=...
  → inference/market_inference.r:
        SELECT * FROM closed_market_variables WHERE market_id = ?
        ajusta distribuciones (Gamma, T-student, Beta, ...)
        aplica criterio insider: wallet en las 5 regiones críticas
        si insiders_found > 0:
          POST http://api:8000/insider-detection  ← llama a E automáticamente (httr)
  → devuelve {status, insiders_found}
```

#### 5. E: REST API + limpieza (FastAPI)

```
receive_insider_detection(batch)  [api/main.py]
  ← llamado por Motor D cuando detecta insiders
  por cada wallet_result:
    → DatabaseManager.upsert_wallet({insider=True, categories_insider, marketids_insider})
    → publish_insider_command.delay({wallet, categories, command_type})  ← T3
  → DELETE FROM closed_market_variables WHERE market_id = batch.market_id  ← limpieza CMV
  → cleanup_market.delay(batch.market_id)  ← T4 reactivo
```

#### 6. T3: Publicar comando a L2 (Celery)

```
publish_insider_command(command_dict)  [workers/tasks.py]
  → XADD insider_commands {payload: InsiderTradeCommand JSON}
    campos: wallet, categories_insider, market_ids_insider, command_type
```

#### 7. L2: Monitor de wallets insider

`InsiderTradesListener` (`ingestion/listeners/insider_trades_listener.py`) corre 2 loops:

```
_command_listener_loop()
  XREADGROUP insider_commands  (group: ingestion_l2)
  ADD_WALLET      → _monitored_wallets[wallet] = MonitoredWallet(categories, last_seen_ts)
  UPDATE_CATEGORIES → actualiza wallet.categories_insider
  REMOVE_WALLET   → elimina wallet de _monitored_wallets
  → ack

_trade_polling_loop()  [cada 30s]
  por cada wallet en _monitored_wallets:
    → TradesCollector.fetch_trades_for_wallet(wallet, limit=100)
    → filtra timestamp > last_seen_ts             (evita duplicados)
    → filtra market.category ∩ wallet.categories_insider
    → RedisPublisher.publish_insider_trades(wallet, trades, categories)
          → XADD insider_trades {payload: InsiderTradeData JSON}
    → actualiza last_seen_ts
```

#### 8. T4: Limpieza de trades

```
cleanup_market(market_id)  [workers/tasks.py — disparado por E]
  → DELETE FROM closed_market_variables WHERE market_id = :mid  (no-op si E ya borró)
  → DELETE FROM trades WHERE market_id = :mid
  FK order: CMV primero (cmv.trade_id → trades.id), luego trades

cleanup_stale_trades()  [workers/tasks.py — Celery Beat, cada hora]
  → ejecuta scripts/sql/cleanup_stale_trades.sql
  → borra trades con updated_at < NOW() - 1h + sus CMV asociados
```

### Redis Streams

| Stream | Productor | Contrato | Consumer Group | Consumidor |
|--------|-----------|----------|----------------|------------|
| `closed_markets` | L1 (`RedisPublisher`) | `MarketDataBatch` | `scheduler` | `QueueOrchestrator._run_market_loop()` |
| `raw_data` | Backfill (`RedisPublisher`) | `MarketDataBatch` | `scheduler` | `QueueOrchestrator._run_market_loop()` |
| `insider_trades` | L2 (`RedisPublisher`) | `InsiderTradeData` | `tasks` | `QueueOrchestrator._run_insider_trades_loop()` |
| `insider_commands` | T3 (`publish_insider_command`) | `InsiderTradeCommand` | `ingestion_l2` | `InsiderTradesListener._command_listener_loop()` |

#### Inspección Redis (CLI)

```bash
# Longitud de streams
docker exec cfg-data-redis-1 redis-cli XLEN closed_markets
docker exec cfg-data-redis-1 redis-cli XLEN insider_trades
docker exec cfg-data-redis-1 redis-cli XLEN insider_commands

# Mensajes pendientes por consumer group
docker exec cfg-data-redis-1 redis-cli XPENDING closed_markets scheduler
docker exec cfg-data-redis-1 redis-cli XPENDING insider_trades tasks
docker exec cfg-data-redis-1 redis-cli XPENDING insider_commands ingestion_l2

# Últimos mensajes
docker exec cfg-data-redis-1 redis-cli XRANGE closed_markets - + COUNT 3
```

### Tasks (workers/tasks.py)

| Función Celery | Tarea | Disparado por | Qué hace |
|----------------|-------|---------------|----------|
| `compute_variables(market_id)` | T2 | `QueueOrchestrator` (tras T1) | Ejecuta `compute_variables.sql` → `closed_market_variables`; dispara `run_market_inference` |
| `run_market_inference(market_id)` | → D | T2 | HTTP POST al Motor R; D llama a E si hay insiders |
| `publish_insider_command(cmd)` | T3 | E (`api/main.py`) | XADD `insider_commands` con `InsiderTradeCommand` |
| `cleanup_market(market_id)` | T4 reactivo | E (`api/main.py`) | DELETE trades + CMV del market procesado |
| `cleanup_stale_trades()` | T4 periódico | Celery Beat (cada hora) | DELETE trades con `updated_at < NOW()-1h` + sus CMV |

---

## Estructura del Repositorio

```
polymarket-insider-detection/
│
├── CLAUDE.md                     # Este archivo
├── README.md                     # Setup e instrucciones
├── docker-compose.yml            # Redis + PostgreSQL + Workers
├── pyproject.toml                # Dependencias (Poetry)
│
├── contracts/                    # 🔒 INTERFACES COMPARTIDAS
│   ├── __init__.py
│   ├── raw_market_data.py        # A → B
│   ├── processing_job.py         # B → C
│   └── score_output.py           # C → DB
│
├── ingestion/                    # 👤 PERSONA A
│   ├── __init__.py
│   ├── main.py                       # Entry point: arranca L1 + L2
│   ├── config.py                     # API keys, rate limits, CATEGORIAS_DE_INTERES
│   ├── http_client.py                # Cliente HTTP async con rate limiting
│   ├── publisher.py                  # Enviar a Redis (3 streams)
│   ├── backfill_runner.py            # Backfill histórico (CLI)
│   ├── collectors/
│   │   ├── __init__.py
│   │   ├── markets_collector.py      # Gamma API + Events API
│   │   ├── trades_collector.py       # Data API - trades
│   │   ├── wallet_age_collector.py   # Data API + Etherscan V2
│   │   └── realtime/                 # On-chain monitoring (Polygon)
│   │       ├── __init__.py
│   │       ├── websocket_listener.py # WebSocket RPC Polygon
│   │       ├── polling_listener.py   # Fallback polling
│   │       └── event_decoder.py      # Decode OrderFilled events
│   ├── listeners/
│   │   ├── __init__.py
│   │   ├── market_close_listener.py  # L1: descubrimiento + cierre
│   │   └── insider_trades_listener.py # L2: comandos T3 + polling
│   └── normalizers/
│       ├── __init__.py
│       └── data_normalizer.py        # Raw → Contratos Pydantic
│
├── queue/                        # 👤 PERSONA B
│   ├── __init__.py
│   ├── streams/
│   │   ├── __init__.py
│   │   ├── raw_data_stream.py        # Stream entrada (A→B)
│   │   ├── jobs_stream.py            # Stream jobs (B→C)
│   │   └── results_stream.py         # Stream resultados (C→B)
│   ├── scheduler/
│   │   ├── __init__.py
│   │   ├── job_creator.py            # MarketDataBatch → ProcessingJob
│   │   ├── priority_queue.py         # Ordenar por prioridad
│   │   └── rate_limiter.py           # No saturar workers
│   ├── reliability/
│   │   ├── __init__.py
│   │   ├── dead_letter.py            # Jobs fallidos
│   │   └── retry_handler.py          # Reintentos con backoff
│   ├── interfaces/
│   │   ├── __init__.py
│   │   ├── producer.py               # Interfaz para A
│   │   └── consumer.py               # Interfaz para C
│   └── config.py                     # Redis connection
│
├── workers/                      # 👤 PERSONA C
│   ├── __init__.py
│   ├── calculator/
│   │   ├── __init__.py
│   │   ├── iscore_calculator.py      # Orquestador principal
│   │   ├── component_a.py            # Impacto del éxito
│   │   ├── component_b.py            # Factor fantasma
│   │   └── component_c.py            # Agresividad focalizada
│   ├── percentiles/
│   │   ├── __init__.py
│   │   └── percentile_calculator.py
│   ├── alerts/
│   │   ├── __init__.py
│   │   └── alert_generator.py
│   ├── worker.py                     # Loop principal
│   └── config.py                     # Parámetros θ₁, θ₂, θ₃, K
│
├── database/                     # 👤 PERSONA C
│   ├── __init__.py
│   ├── models/
│   │   ├── __init__.py
│   │   ├── wallet.py
│   │   ├── trade.py
│   │   ├── market.py
│   │   ├── score.py
│   │   └── alert.py
│   ├── migrations/
│   │   ├── 001_initial_schema.sql
│   │   └── 002_indexes.sql
│   ├── repositories/
│   │   ├── __init__.py
│   │   ├── wallets_repo.py
│   │   ├── trades_repo.py
│   │   ├── scores_repo.py
│   │   └── alerts_repo.py
│   └── connection.py
│
├── tests/
│   ├── unit/
│   │   ├── test_component_a.py
│   │   ├── test_component_b.py
│   │   ├── test_component_c.py
│   │   └── test_iscore.py
│   ├── integration/
│   │   ├── test_ingestion_to_queue.py
│   │   └── test_queue_to_worker.py
│   └── fixtures/
│       ├── sample_trades.json
│       └── sample_markets.json
│
└── scripts/
    ├── run_backfill.py               # Ejecutar backfill histórico
    ├── calibrate_thetas.py           # Calibrar parámetros
    └── analyze_market.py             # Analizar un mercado específico
```

---

## Contratos (Interfaces entre capas)

### A → B: `MarketDataBatch` + Insider contracts

```python
# contracts/raw_market_data.py
from pydantic import BaseModel, Field
from datetime import datetime, UTC
from typing import Literal

class RawTrade(BaseModel):
    tx_hash: str
    wallet: str                           # lowercase, 0x-prefixed
    market_id: str                        # conditionId
    timestamp: datetime
    side: Literal["BUY", "SELL"]
    outcome: Literal["Yes", "No"]
    price: float                          # 0.0 - 1.0
    size_usdc: float
    size_tokens: float
    last_trade: bool = False              # True = último trade antes del cierre

class RawMarketMetadata(BaseModel):
    market_id: str
    question: str
    end_date: datetime
    closed: bool
    outcome_prices: list[float] | None    # [1.0, 0.0] si resuelto
    volume_usdc: float
    winning_outcome: str | None = None    # "Yes"/"No", derivado de outcome_prices
    category: str | None                  # "Sports", "US-current-affairs", "Crypto", etc.
    tags: list[str] | None                # ["NBA", "Basketball"] - extraídos de events

class RawWalletAge(BaseModel):
    wallet: str
    first_tx_polymarket: datetime | None
    first_tx_polygon: datetime | None     # De Etherscan V2 (chainid=137)

class MarketDataBatch(BaseModel):
    metadata: RawMarketMetadata
    trades: list[RawTrade]
    wallet_ages: dict[str, RawWalletAge]
    ingested_at: datetime

# --- Insider Trades (L2 <-> T3) ---

class InsiderTradeCommand(BaseModel):
    """Comando T3 → L2 via Redis stream 'insider_commands'"""
    wallet: str
    categories_insider: list[str]
    market_ids_insider: list[str] | None = None
    command_type: Literal["ADD_WALLET", "UPDATE_CATEGORIES", "REMOVE_WALLET"] = "ADD_WALLET"

class InsiderTradeData(BaseModel):
    """Dato L2 → Redis stream 'insider_trades'"""
    wallet: str
    trades: list[RawTrade]
    categories_insider: list[str]
    published_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
```

### B → C: `ProcessingJob`

```python
# contracts/processing_job.py
from pydantic import BaseModel
from datetime import datetime
from enum import Enum

class JobType(str, Enum):
    CALCULATE_SCORES = "calculate_scores"
    RECALCULATE_WALLET = "recalculate_wallet"
    UPDATE_PERCENTILES = "update_percentiles"

class JobPriority(int, Enum):
    LOW = 1         # Backfill histórico
    NORMAL = 5      # Mercados recién cerrados
    HIGH = 10       # Tiempo real

class ProcessingJob(BaseModel):
    job_id: str
    job_type: JobType
    priority: JobPriority
    created_at: datetime
    payload: dict                         # Contenido según job_type

class JobResult(BaseModel):
    job_id: str
    success: bool
    error_message: str | None
    scores_calculated: int
    alerts_generated: int
    processing_time_ms: int
```

### C → DB: `WalletScore`

```python
# contracts/score_output.py
from pydantic import BaseModel
from datetime import datetime
from typing import Literal

class WalletScore(BaseModel):
    wallet: str
    market_id: str
    trade_tx_hash: str

    # Categorización (para análisis y filtrado)
    category: str | None                  # "Sports", "US-current-affairs", etc.
    tags: list[str] | None                # ["NBA", "Basketball"]

    i_score: float
    percentile: float | None

    component_a: float
    component_b: float
    component_c: float

    # Variables intermedias
    delta_elo: float
    volume_usdc: float
    won: bool
    price_at_trade: float
    age_days: float
    hours_to_resolution: float
    slippage: float
    portfolio_entropy: float

    calculated_at: datetime
    parameters_version: str               # "v1.0"

class Alert(BaseModel):
    wallet: str
    market_id: str
    category: str | None                  # Para filtrar alertas por dominio
    i_score: float
    percentile: float
    alert_type: Literal["P95", "P99", "EXTREME"]
    created_at: datetime
```

---

## Interfaces de Comunicación

### Producer (A usa esto para publicar)

```python
# queue/interfaces/producer.py
from abc import ABC, abstractmethod
from contracts.raw_market_data import MarketDataBatch

class QueueProducer(ABC):
    @abstractmethod
    async def publish_market_batch(self, batch: MarketDataBatch) -> str:
        """Publica batch completo de mercado (backfill). Retorna batch_id."""
        pass

    @abstractmethod
    async def health_check(self) -> bool:
        """Verifica conexión con Redis."""
        pass
```

**Nota:** Además del `publish_market_batch` (backfill → `raw_data` stream), el `RedisPublisher` de ingestion implementa directamente:
- `publish_closed_market_batch()` → `closed_markets` stream (L1)
- `publish_insider_trades()` → `insider_trades` stream (L2)

### Consumer (C usa esto para consumir)

```python
# queue/interfaces/consumer.py
from abc import ABC, abstractmethod
from contracts.processing_job import ProcessingJob, JobResult

class QueueConsumer(ABC):
    @abstractmethod
    async def get_next_job(self, worker_id: str, timeout_ms: int = 5000) -> ProcessingJob | None:
        """Obtiene siguiente job. Bloquea hasta timeout."""
        pass

    @abstractmethod
    async def ack_job(self, job_id: str, result: JobResult) -> None:
        """Confirma job procesado exitosamente."""
        pass

    @abstractmethod
    async def nack_job(self, job_id: str, error: str, retry: bool = True) -> None:
        """Rechaza job. Si retry=True, vuelve a la cola."""
        pass

    @abstractmethod
    async def get_pending_count(self) -> int:
        """Número de jobs pendientes."""
        pass
```

---

## Mensajes del Publisher a Redis

### `publish_closed_market_batch()` → `stream:closed_markets` (L1)

```json
{
  "payload": "{...MarketDataBatch JSON completo...}",
  "market_id": "0x1234abcd5678efgh9012ijkl3456mnop",
  "trades_count": "47",
  "wallets_count": "23",
  "category": "Sports",
  "winning_outcome": "Yes",
  "ingested_at": "2026-02-09T15:30:00.123456"
}
```

### `publish_insider_trades()` → `stream:insider_trades` (L2)

```json
{
  "payload": "{...InsiderTradeData JSON...}",
  "wallet": "0xabc123...",
  "trades_count": "3",
  "categories": "[\"Sports\",\"Crypto\"]",
  "published_at": "2026-02-09T15:30:05.789012"
}
```

### `publish_market_batch()` → `stream:raw_data` (Backfill)

```json
{
  "payload": "{...MarketDataBatch JSON completo...}"
}
```

### Resumen Visual

```
┌─────────────────────────────────────────────────────────────┐
│              L1: MARKET CLOSE (batch completo)              │
│  stream:closed_markets                                      │
│  ┌───────────────────────────────────────────────────────┐  │
│  │ MarketDataBatch + winning_outcome + wallet_ages       │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│              L2: INSIDER TRADES (por wallet)                │
│  stream:insider_trades                                      │
│  ┌───────────────────────────────────────────────────────┐  │
│  │ InsiderTradeData (wallet + trades + categories)       │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│              BACKFILL (batch histórico)                     │
│  stream:raw_data                                            │
│  ┌───────────────────────────────────────────────────────┐  │
│  │ MarketDataBatch (via RawDataStream)                   │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### Cómo Levantar el Publisher

El publisher se ejecuta como parte del servicio `ingestion` en docker-compose:

```bash
# Levantar solo Redis (requerido)
docker-compose up -d redis

# Ejecutar listeners L1+L2 (automático, entry point principal)
poetry run python -m ingestion.main

# Ejecutar backfill histórico (herramienta CLI separada)
poetry run python -m ingestion.backfill_runner --days=30 --min-volume=50000

# O levantar todo con Docker
docker-compose up -d redis ingestion
```

El publisher NO es un servicio independiente. Se usa internamente desde:
- `ingestion/main.py` → L1 (`publish_closed_market_batch`) y L2 (`publish_insider_trades`)
- `backfill_runner.py` → `publish_market_batch` para batches históricos

### Arquitectura de Clases (Ingestion)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    ingestion/backfill_runner.py                             │
│                         BackfillRunner                                      │
│                    (Orquestador principal - CLI)                            │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ usa
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        ingestion/collectors/                                │
│  ┌───────────────────┐ ┌───────────────────┐ ┌───────────────────────────┐  │
│  │  MarketsCollector │ │  TradesCollector  │ │   WalletAgeCollector      │  │
│  │  (Gamma API)      │ │  (Data API)       │ │   (Data API + Etherscan)  │  │
│  │                   │ │                   │ │                           │  │
│  │ fetch_closed_     │ │ fetch_trades_     │ │ fetch_wallet_ages_batch() │  │
│  │ markets()         │ │ for_market()      │ │                           │  │
│  └─────────┬─────────┘ └─────────┬─────────┘ └─────────────┬─────────────┘  │
│            │                     │                         │                │
│            │ dict                │ list[dict]              │ dict           │
│            ▼                     ▼                         ▼                │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ raw data (dicts)
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                   ingestion/normalizers/data_normalizer.py                  │
│                           DataNormalizer                                    │
│                                                                             │
│  normalize_batch(market_raw, trades_raw, wallet_ages_raw)                   │
│                         │                                                   │
│                         ▼                                                   │
│              ┌─────────────────────┐                                        │
│              │   MarketDataBatch   │  (contracts/raw_market_data.py)        │
│              │  • metadata         │                                        │
│              │  • trades[]         │                                        │
│              │  • wallet_ages{}    │                                        │
│              │  • ingested_at      │                                        │
│              └─────────────────────┘                                        │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ MarketDataBatch
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                       ingestion/publisher.py                                │
│                         RedisPublisher                                      │
│                   (implements QueueProducer)                                │
│                                                                             │
│  publish_market_batch(batch) ──────► RawDataStream.add()                    │
│                                              │                              │
│                                              ▼                              │
│                                      Redis Stream                           │
│                                      "raw_data"                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Flujo de datos:**
1. `BackfillRunner` solicita mercados cerrados a `MarketsCollector`
2. Para cada mercado, obtiene trades con `TradesCollector`
3. Extrae wallets de los trades y consulta edades con `WalletAgeCollector`
4. `DataNormalizer` convierte los dicts raw → `MarketDataBatch`
5. `RedisPublisher` (ingestion/publisher.py) envía el batch al stream `raw_data`

---

## Listeners (L1 + L2) — Ingestion en Tiempo Real

Al levantar la capa de ingestion (`python -m ingestion.main`), se arrancan **dos listeners concurrentes** que funcionan de forma autónoma:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        ingestion/main.py                               │
│                   asyncio.gather(l1.run(), l2.run())                   │
└────────────────────────────┬────────────────────────────────────────────┘
                             │
              ┌──────────────┴──────────────┐
              ▼                              ▼
┌──────────────────────────┐   ┌──────────────────────────────┐
│  L1: MarketCloseListener │   │  L2: InsiderTradesListener   │
│                          │   │                              │
│  Descubrimiento híbrido  │   │  Comandos desde T3 (Redis)   │
│  + detección de cierre   │   │  + polling de trades         │
└────────────┬─────────────┘   └──────────────┬───────────────┘
             │                                 │
             ▼                                 ▼
      closed_markets                    insider_trades
      (Redis Stream)                    (Redis Stream)
```

### L1: MarketCloseListener

**Archivo:** `ingestion/listeners/market_close_listener.py`

**Propósito:** Monitorea mercados activos que coincidan con `CATEGORIAS_DE_INTERES`. Cuando un mercado **cierra**, recolecta TODOS los trades + wallet ages, normaliza, y publica un batch completo.

**Tres loops concurrentes:**

| Loop | Intervalo | Qué hace |
|------|-----------|----------|
| `_ws_discovery_loop()` | Continuo | Escucha eventos OrderFilled on-chain via WebSocket de Polygon. Cada trade descubierto → enriquece market metadata via Events API → si tags/category coinciden con `CATEGORIAS_DE_INTERES` → añade a `_tracked_markets` |
| `_gamma_discovery_loop()` | Cada 15 min | `GET /markets?closed=false&volume_num_min=X` + Events API para tags/category. Catch-all para mercados sin trades recientes que el WebSocket no vería |
| `_close_detection_loop()` | Cada 60s | Re-consulta cada tracked market via `fetch_market_by_id()`. Si `closed=true` → ejecuta `_handle_market_close()` |

**Flujo de cierre (`_handle_market_close`):**
1. `TradesCollector.fetch_trades_for_market()` → obtiene TODOS los trades
2. `TradesCollector.extract_wallets_from_trades()` → extrae wallets únicas
3. `WalletAgeCollector.fetch_wallet_ages_batch()` → obtiene edades
4. `DataNormalizer.normalize_batch()` → convierte a `MarketDataBatch`
5. `compute_winning_outcome()` → determina ganador ("Yes"/"No")
6. `mark_last_trade()` → marca el trade con timestamp más reciente
7. `RedisPublisher.publish_closed_market_batch()` → XADD a `closed_markets`

**Filtrado por categorías:**
- Compara `category` del market + `tags[]` de events contra `CATEGORIAS_DE_INTERES`
- Case-insensitive
- Tags pueden venir como `{label, slug}` dicts o como strings

**Descubrimiento híbrido (por qué):**
- **WebSocket** detecta mercados instantáneamente cuando tienen actividad on-chain
- **Gamma API** (cada 15 min) captura mercados sin trades recientes que el WebSocket no ve
- Ambos se complementan para no perder ningún mercado relevante

### L2: InsiderTradesListener

**Archivo:** `ingestion/listeners/insider_trades_listener.py`

**Propósito:** Monitorea wallets flaggeadas como insiders por T3. Cuando se detectan trades nuevos de esas wallets en categorías relevantes, los publica para análisis.

**Dos loops concurrentes:**

| Loop | Mecanismo | Qué hace |
|------|-----------|----------|
| `_command_listener_loop()` | XREADGROUP blocking (5s timeout) | Espera comandos de T3 en `insider_commands` stream. Soporta: `ADD_WALLET`, `UPDATE_CATEGORIES`, `REMOVE_WALLET` |
| `_trade_polling_loop()` | Cada 30s | Para cada wallet monitoreada: fetch trades recientes → filtra por timestamp > last_seen → filtra por categorías del market vs `categories_insider` de la wallet → publica matches |

**Flujo de comandos:**
```
T3 (Celery Task) ─── XADD insider_commands ───► L2 _command_listener_loop()
                                                       │
                                                       ▼
                                                _monitored_wallets dict
                                                  wallet → MonitoredWallet{
                                                    categories_insider,
                                                    last_seen_ts,
                                                    trades_published
                                                  }
```

**Formato de comando (`InsiderTradeCommand`):**
```python
{
    "wallet": "0xabc...",
    "categories_insider": ["Sports", "Crypto"],
    "market_ids_insider": ["0xmarket1", ...],  # opcional
    "command_type": "ADD_WALLET"  # ADD_WALLET | UPDATE_CATEGORIES | REMOVE_WALLET
}
```

**Flujo de polling de trades:**
1. Para cada wallet en `_monitored_wallets`:
2. `TradesCollector.fetch_trades_for_wallet(wallet, limit=100)` → trades recientes
3. Filtra trades con `timestamp > last_seen_ts` (evita duplicados)
4. Para cada trade, consulta metadata del market (con cache) y enriquece via Events API
5. Si el market tiene tags/category que intersectan con `categories_insider` de la wallet → normaliza el trade
6. `RedisPublisher.publish_insider_trades(wallet, trades, categories)` → XADD a `insider_trades`
7. Actualiza `last_seen_ts` al timestamp más reciente

### Redis Streams (A → B)

| Stream | Productor | Contenido | Consumer Group | Consumidor |
|--------|-----------|-----------|----------------|------------|
| `raw_data` | Backfill | `MarketDataBatch` | `scheduler` | QueueOrchestrator → `jobs_stream` |
| `closed_markets` | L1 | `MarketDataBatch` (market + trades + wallet_ages + winning_outcome) | `scheduler` | QueueOrchestrator → `jobs_stream` |
| `insider_trades` | L2 | `InsiderTradeData` (wallet + trades filtrados + categories) | `tasks` | T1 → BD |
| `insider_commands` | T3 | `InsiderTradeCommand` (wallet + categories + command_type) | `ingestion_l2` | L2 |

> `raw_data` y `closed_markets` son leídos por el QueueOrchestrator en paralelo y convertidos a `ProcessingJob`.
> `insider_trades` es leído directamente por T1 (no pasa por el orchestrator).

### Configuración (`ingestion/config.py`)

```bash
# .env
CATEGORIAS_DE_INTERES="Sports,Crypto,US-current-affairs,Pop Culture"
```

| Setting | Default | Descripción |
|---------|---------|-------------|
| `categorias_de_interes` | `["Sports", "Crypto", "US-current-affairs", "Pop Culture"]` | Categorías/tags a monitorear (configurable via .env) |
| `l1_market_poll_interval_seconds` | `60` | Frecuencia de chequeo de cierre de mercados |
| `l1_gamma_discovery_interval_seconds` | `900` | Frecuencia de descubrimiento via Gamma API (15 min) |
| `l1_min_volume_usdc` | `10000.0` | Volumen mínimo para trackear un mercado |
| `l2_poll_interval_seconds` | `30` | Frecuencia de polling de trades de wallets insider |
| `l2_commands_stream` | `"insider_commands"` | Stream Redis para comandos T3 → L2 |
| `l2_commands_consumer_group` | `"ingestion_l2"` | Consumer group para XREADGROUP |
| `closed_markets_stream` | `"closed_markets"` | Stream Redis para batches de mercados cerrados |
| `insider_trades_stream` | `"insider_trades"` | Stream Redis para trades de insiders |

### Cómo Levantar

```bash
# Requisito: Redis
docker-compose up -d redis

# Localmente (L1 + L2 arrancan automáticamente)
poetry run python -m ingestion.main

# Con Docker
docker-compose up -d ingestion

# Backfill histórico (herramienta separada, perfil tools)
docker-compose --profile tools run backfill
```

### Test Scripts de Listeners

| Script | Descripción | Dependencias |
|--------|-------------|--------------|
| `test_market_close_listener.py` | Category filtering, winning_outcome, last_trade, discovery live | Gamma API (test 7) |
| `test_insider_trades_listener.py` | Command processing, modelos, Redis injection | Redis (test 6) |

```bash
poetry run python -m scripts.test_market_close_listener
poetry run python -m scripts.test_insider_trades_listener
```

### Verificar Streams de Listeners

```bash
# Ver mensajes de mercados cerrados
docker exec -it cfg-data-redis-1 redis-cli XLEN closed_markets
docker exec -it cfg-data-redis-1 redis-cli XRANGE closed_markets - + COUNT 3

# Ver mensajes de insider trades
docker exec -it cfg-data-redis-1 redis-cli XLEN insider_trades

# Inyectar comando de insider para testing
docker exec -it cfg-data-redis-1 redis-cli XADD insider_commands '*' \
  wallet '0xTestWallet123' \
  categories_insider '["Sports","Crypto"]' \
  command_type 'ADD_WALLET'

# Ver comandos pendientes
docker exec -it cfg-data-redis-1 redis-cli XRANGE insider_commands - + COUNT 5
```

---

## Schema de Base de Datos

```sql
-- database/migrations/001_initial_schema.sql

-- Wallets analizadas
CREATE TABLE wallets (
    address TEXT PRIMARY KEY,
    first_seen_polymarket TIMESTAMPTZ,
    first_seen_polygon TIMESTAMPTZ,
    total_volume_usdc DECIMAL DEFAULT 0,
    total_markets INT DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Mercados procesados
CREATE TABLE markets (
    market_id TEXT PRIMARY KEY,
    question TEXT,
    end_date TIMESTAMPTZ,
    closed BOOLEAN DEFAULT FALSE,
    winning_outcome TEXT,                 -- 'Yes' o 'No'
    volume_usdc DECIMAL,
    processed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Trades indexados
CREATE TABLE trades (
    id SERIAL PRIMARY KEY,
    tx_hash TEXT UNIQUE NOT NULL,
    wallet TEXT REFERENCES wallets(address),
    market_id TEXT REFERENCES markets(market_id),
    timestamp TIMESTAMPTZ NOT NULL,
    side TEXT NOT NULL,                   -- BUY/SELL
    outcome TEXT NOT NULL,                -- Yes/No
    price DECIMAL NOT NULL,
    size_usdc DECIMAL NOT NULL,
    size_tokens DECIMAL NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Scores calculados
CREATE TABLE scores (
    id SERIAL PRIMARY KEY,
    wallet TEXT REFERENCES wallets(address),
    market_id TEXT REFERENCES markets(market_id),
    trade_id INT REFERENCES trades(id),

    i_score DECIMAL NOT NULL,
    percentile DECIMAL,

    component_a DECIMAL NOT NULL,
    component_b DECIMAL NOT NULL,
    component_c DECIMAL NOT NULL,

    delta_elo DECIMAL,
    volume_usdc DECIMAL,
    won BOOLEAN,
    price_at_trade DECIMAL,
    age_days DECIMAL,
    hours_to_resolution DECIMAL,
    slippage DECIMAL,
    portfolio_entropy DECIMAL,

    parameters_version TEXT DEFAULT 'v1.0',
    calculated_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE(wallet, market_id, trade_id)
);

-- Alertas
CREATE TABLE alerts (
    id SERIAL PRIMARY KEY,
    wallet TEXT NOT NULL,
    market_id TEXT NOT NULL,
    score_id INT REFERENCES scores(id),

    i_score DECIMAL NOT NULL,
    percentile DECIMAL NOT NULL,
    alert_type TEXT NOT NULL,             -- P95, P99, EXTREME

    status TEXT DEFAULT 'NEW',            -- NEW, REVIEWED, DISMISSED, CONFIRMED
    reviewed_by TEXT,
    reviewed_at TIMESTAMPTZ,
    notes TEXT,

    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- database/migrations/002_indexes.sql

CREATE INDEX idx_trades_wallet ON trades(wallet);
CREATE INDEX idx_trades_market ON trades(market_id);
CREATE INDEX idx_trades_timestamp ON trades(timestamp DESC);

CREATE INDEX idx_scores_wallet ON scores(wallet);
CREATE INDEX idx_scores_market ON scores(market_id);
CREATE INDEX idx_scores_iscore ON scores(i_score DESC);
CREATE INDEX idx_scores_percentile ON scores(percentile DESC);

CREATE INDEX idx_alerts_status ON alerts(status, created_at DESC);
CREATE INDEX idx_alerts_wallet ON alerts(wallet);
CREATE INDEX idx_alerts_percentile ON alerts(percentile DESC);
```

---

## Configuración

### Variables de Entorno

```bash
# .env.example

# APIs Externas
# Etherscan API V2 (unified endpoint for all chains including Polygon)
# Get your key at: https://etherscan.io/myapikey
ETHERSCAN_API_KEY=your_key_here

# Filtro de categorías (L1 y L2)
# Comma-separated list of categories/tags to monitor
CATEGORIAS_DE_INTERES="Sports,Crypto,US-current-affairs,Pop Culture"

# Redis
REDIS_URL=redis://localhost:6379/0

# PostgreSQL
DATABASE_URL=postgresql://user:pass@localhost:5432/insider_detection

# Parámetros del modelo
THETA_1=1.0
THETA_2=10.0
THETA_3=10.0
THETA_4=5.0
ELO_K_FACTOR=32

# Umbrales de alerta
ALERT_PERCENTILE_P95=0.95
ALERT_PERCENTILE_P99=0.99

# Rate limits
POLYMARKET_REQUESTS_PER_MINUTE=100
POLYGONSCAN_REQUESTS_PER_SECOND=3  # Etherscan V2 free tier limit

# Workers
NUM_WORKERS=4
```

### Docker Compose

```yaml
# docker-compose.yml
version: "3.8"

services:
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data

  postgres:
    image: timescale/timescaledb:latest-pg15
    environment:
      POSTGRES_USER: insider
      POSTGRES_PASSWORD: insider_pass
      POSTGRES_DB: insider_detection
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data

  ingestion:
    build:
      context: .
      dockerfile: Dockerfile
    command: python -m ingestion.main      # Arranca L1 + L2
    env_file: .env
    environment:
      - REDIS_URL=redis://redis:6379/0    # Override para Docker networking
    depends_on:
      - redis

  backfill:
    build:
      context: .
      dockerfile: Dockerfile
    command: python -m ingestion.backfill_runner
    env_file: .env
    environment:
      - REDIS_URL=redis://redis:6379/0
    depends_on:
      - redis
    profiles:
      - tools                              # Solo se ejecuta con --profile tools

  worker:
    build:
      context: .
      dockerfile: Dockerfile
    command: python -m workers.worker
    env_file: .env
    depends_on:
      - redis
      - postgres
    deploy:
      replicas: 4

volumes:
  redis_data:
  postgres_data:
```

**Nota:** Dentro de Docker, los servicios se comunican por nombre de servicio (`redis://redis:6379/0`), no por `localhost`. El `environment` override en `ingestion` y `backfill` sobreescribe el `REDIS_URL` del `.env` sin afectar el desarrollo local.

---

## Dependencias

```toml
# pyproject.toml (resumen)
[tool.poetry.dependencies]
python = "^3.11"

# APIs y HTTP
aiohttp = "^3.9"
httpx = "^0.27"
websockets = "^12.0"

# Blockchain (for on-chain event decoding)
web3 = "^6.0"
eth-abi = "^5.0"

# Polymarket client
py-clob-client = "^0.1"

# Queue
redis = "^5.0"

# Database
asyncpg = "^0.29"
sqlalchemy = "^2.0"
alembic = "^1.13"

# Data validation
pydantic = "^2.5"
pydantic-settings = "^2.1"

# Cálculos
numpy = "^1.26"

# Utils
python-dotenv = "^1.0"
structlog = "^24.1"
setuptools = "^80.10.2"

[tool.poetry.group.dev.dependencies]
pytest = "^8.0"
pytest-asyncio = "^0.23"
pytest-cov = "^4.1"
pytest-mock = "^3.12"
fakeredis = "^2.21"
freezegun = "^1.4"
ruff = "^0.2"
mypy = "^1.8"
```

---

## Comandos Útiles

```bash
# Setup inicial
poetry install
docker-compose up -d redis postgres
alembic upgrade head

# Ejecutar listeners L1 + L2 (entry point principal)
poetry run python -m ingestion.main

# Ejecutar backfill histórico (herramienta CLI separada)
poetry run python -m ingestion.backfill_runner --days=30 --min-volume=50000

# Levantar con Docker (listeners L1 + L2)
docker-compose up -d redis ingestion

# Ejecutar backfill via Docker (perfil tools)
docker-compose --profile tools run backfill

# Ejecutar workers
python -m workers.worker --id=worker-1

# Tests unitarios (pytest)
pytest tests/unit -v
pytest tests/integration -v --cov=.
```

---

## Ejecutar Tests de Scripts

El proyecto incluye un **test runner dinámico** que ejecuta todos los scripts `test_*.py` en la carpeta `scripts/`.

### Comandos Básicos

```bash
# Ejecutar TODOS los tests
poetry run python -m scripts.run_all_tests

# Ver lista de tests disponibles (sin ejecutar)
poetry run python -m scripts.run_all_tests --list

# Ejecutar con output detallado
poetry run python -m scripts.run_all_tests --verbose
```

### Opciones Avanzadas

```bash
# Filtrar tests por nombre (ej: solo los de categorías)
poetry run python -m scripts.run_all_tests --filter categories

# Filtrar tests de mercados
poetry run python -m scripts.run_all_tests --filter market

# Detenerse en el primer fallo
poetry run python -m scripts.run_all_tests --stop-on-fail

# Ejecutar en paralelo (experimental)
poetry run python -m scripts.run_all_tests --parallel

# Combinaciones
poetry run python -m scripts.run_all_tests --filter normalizer --verbose
```

### Opciones del Test Runner

| Opción | Descripción |
|--------|-------------|
| `--list` | Lista los tests disponibles sin ejecutarlos |
| `-v, --verbose` | Muestra el output completo de cada test |
| `--filter PATTERN` | Filtra tests cuyo nombre contenga el patrón |
| `--stop-on-fail` | Detiene la ejecución en el primer fallo |
| `--parallel` | Ejecuta tests en paralelo (4 workers) |

### Tests Disponibles

| Script | Descripción | Dependencias |
|--------|-------------|--------------|
| `test_markets.py` | Valida collector de mercados | Gamma API |
| `test_trades.py` | Valida collector de trades | Data API |
| `test_normalizer.py` | Valida normalización de datos | Ninguna |
| `test_categories.py` | Valida extracción de categorías | Ninguna |
| `test_wallet_age.py` | Valida edad de wallets | Etherscan API V2 |
| `test_publisher.py` | Valida publicación a Redis | Redis |
| `test_backfill.py` | Valida backfill runner | APIs + Redis |
| `test_ingestion_to_queue.py` | Test de integración A→B | Redis |
| `test_market_close_listener.py` | L1: filtrado categorías, winning outcome, discovery | Gamma API |
| `test_insider_trades_listener.py` | L2: comandos, modelos, inyección Redis | Redis |

---

## Verificar Conexión a Redis

### 1. Levantar Redis

```bash
docker-compose up -d redis
```

### 2. Monitorear operaciones en tiempo real (opcional)

En una terminal separada, ver TODAS las operaciones de Redis:

```bash
docker exec -it cfg-data-redis-1 redis-cli MONITOR
```

### 3. Ejecutar tests de conexión

```bash
# Test de integración con datos de ejemplo (rápido)
poetry run python -m scripts.test_ingestion_to_queue

# Test con datos reales de Polymarket
poetry run python -m scripts.test_publisher
```

### 4. Inspeccionar el stream directamente

```bash
# Conectar al CLI de Redis
docker exec -it cfg-data-redis-1 redis-cli

# Ver longitud del stream
XLEN raw_data

# Ver los últimos mensajes
XRANGE raw_data - + COUNT 5

# Ver info del stream
XINFO STREAM raw_data

# Ver consumer groups
XINFO GROUPS raw_data
```

---

## Notas Importantes

1. **Rate Limits:** Polymarket ~100 req/min, Etherscan V2 ~3 req/seg (free tier). `http_client.py` implementa retry con backoff exponencial.

2. **Proxy Wallets:** Los usuarios de Polymarket.com usan proxy wallets. La dirección visible puede no ser la EOA original.

3. **WebSocket on-chain vs CLOB:** El WebSocket público de Polymarket no revela wallets (anónimo). L1 usa WebSocket de Polygon RPC para detectar eventos `OrderFilled` del contrato CTFExchange.

4. **FK order en inserts:** wallets → markets → trades (siempre en este orden para respetar constraints).

5. **ON CONFLICT DO NOTHING:** Trades e insider_trades usan esta estrategia — son inmutables una vez escritos.

6. **Edad de wallet:** La Data API solo muestra actividad en Polymarket. Para edad real en blockchain, usar Etherscan API V2 con `chainid=137` (Polygon).

---

## 🧪 Test de Pipeline Completo (`scripts/test_full_pipeline.py`)

### Qué verifica (22 checks)

| Fase | Componente | Qué se comprueba |
|------|-----------|------------------|
| 1 | L2 | ACK de comando ADD_WALLET en `insider_commands` (pending → 0) |
| 2 | Orchestrator | ACK de `closed_markets` batch (pending → 0) |
| 3 | T1 | Wallet, market y trade escritos en BD por el orchestrator |
| 4 | T2 | `compute_variables` Celery task completa sin error; `closed_market_variables` tiene filas |
| 5 | Motor D | `run_market_inference` completa sin excepción; status válido devuelto |
| 5b | D→E auto | D llama automáticamente a `/insider-detection` → wallet marcada `insider=true` en BD |
| 5b | E | `closed_market_variables` vaciada tras la llamada D→E |
| 6 | E | HTTP 200 de `/insider-detection`; `wallet.insider=true`; `categories_insider` poblado |
| 6b | T3 | Mensaje publicado en `insider_commands` tras la llamada a E |
| 6c | L2 | ACK del comando T3 en `ingestion_l2` group (pending → 0) |
| 6d | T1 | ACK de `InsiderTradeData` en `insider_trades`; fila insertada en `insiders_trades` |
| 7 | T4 | `cleanup_stale_trades` completa sin excepción |

### Qué NO verifica (gaps conocidos)

#### Bypasses estructurales
- **L1 real** no se ejercita: el batch se inyecta directamente en `closed_markets` sin pasar por Gamma API, WebSocket de Polygon ni la lógica de detección de cierre de `MarketCloseListener`.
- **L2 polling orgánico** no se ejercita: Phase 6d inyecta directamente en `insider_trades`. El loop `_trade_polling_loop` de `InsiderTradesListener` (que llama a la Data API y filtra por categorías) no es ejecutado.

#### Corrección de datos
- **Valores de CMV no verificados**: `compute_variables` se comprueba que corre y que produce filas, pero los valores (`delta_t`, `age`, `winning_volume`, `alpha_slippage`, `entropy`) no se contrastan contra las fórmulas de la arquitectura.
- **`mark_last_trade()` no ejercitada**: el batch inyectado tiene `last_trade=True` hard-coded en el único trade. El normalizer no se invoca en el pipeline test.
- **`markets.categories` como array**: no se verifica que la columna `categories` se haya guardado como `["Sports"]` (array) y no como string `"Sports"`.

#### Campos de BD no comprobados
- **`wallet.marketids_insider`**: E actualiza también este campo, pero el test solo verifica `insider` y `categories_insider`.
- **`wallet.total_volume_usdc` y `markets_traded`**: el upsert de T1 debería acumular estos valores; no se comprueba.
- **`insiders_trades.last_trade`**: el test verifica que la fila existe, pero no que el valor de `last_trade` es el correcto.

#### Contenido de mensajes Redis
- **Contenido del comando T3**: solo se verifica que `xlen("insider_commands")` creció; no se lee el mensaje para comprobar `wallet`, `categories_insider` ni `command_type`.
- **Contenido del batch de `closed_markets`**: el ACK del orchestrator se considera suficiente, pero no se verifica que el orchestrator parseó correctamente todos los campos.

#### Flujos no cubiertos
- **Stream `raw_data` (backfill)**: el orchestrator consume tanto `closed_markets` como `raw_data`; solo el primero se prueba.
- **Comandos `UPDATE_CATEGORIES` y `REMOVE_WALLET` de L2**: solo `ADD_WALLET` se ejercita.
- **Múltiples trades por batch**: el test usa exactamente 1 trade. El comportamiento con N trades (incluido el orden FK y la unicidad) no se prueba.
- **T4 scope real**: `cleanup_stale_trades` borra datos stale globalmente; el test no verifica que los trades del `TEST_MARKET_ID` fueron específicamente eliminados.
- **Paths de error de D**: cuando D devuelve `no_data` o `distribution_fit_failed`, el test marca los checks posteriores como PASS directamente (N/A) sin verificar que E no fue llamada.

#### Gaps ya resueltos (no son falsos positivos)
- **"E: deleted closed_market_variables after D→E auto-call"**: `api/main.py` ahora ejecuta `DELETE FROM closed_market_variables WHERE market_id = ?` de forma síncrona antes de responder. ✅
- **"T4: cleanup de trades del market procesado"**: `api/main.py` ahora dispara `cleanup_market.delay(market_id)` (nuevo task en `workers/tasks.py`) que borra CMV + trades para ese market_id específico. ✅

### Cómo ejecutar

```bash
# Requiere todos los servicios activos
docker-compose up -d redis postgres orchestrator worker inference api ingestion

# Lanzar el test (lo ejecuta siempre el usuario)
poetry run python -m scripts.test_full_pipeline
```