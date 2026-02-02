# CLAUDE.md - Polymarket Insider Detection System

---

## ⚠️ RESTRICCIÓN DE ROL: PERSONA A (INGESTION)

**Eres la Persona A.** Tu responsabilidad es ÚNICAMENTE la capa de **Ingestion**.

### ✅ PUEDES trabajar en:
- `ingestion/` - Collectors, normalizers, publisher, backfill
- `contracts/raw_market_data.py` - Contratos A → B
- Tests relacionados con ingestion

### ❌ NO PUEDES trabajar en:
- `queue/` - Persona B (Queue + Orchestration)
- `workers/` - Persona C (Workers + Cálculo I_score)
- `database/` - Persona C (PostgreSQL + TimescaleDB)
- `contracts/processing_job.py` - Contrato B → C
- `contracts/score_output.py` - Contrato C → DB

### 📋 Tu interfaz con Persona B:
Usas `queue/interfaces/producer.py` (QueueProducer) para publicar datos.
NO implementas esa interface, solo la consumes.

---

## Visión General

Sistema de detección de insider trading en Polymarket mediante análisis de patrones de comportamiento. Calcula un **I_score** para cada trade que combina tres señales: impacto financiero, firma temporal sospechosa, y agresividad focalizada.

---

## La Fórmula Maestra

```
I_sc = θ₁·(ΔELO) +        θ₂·(1/Δt_res) + θ_3(1/age) + θ_4(slippage·e^(-H_portfolio))
      \_________/         \________________________/   \_________________________/
      A. Impacto del           B. Factor              C. Agresividad
        Éxito                  Fantasma               Focalizada
```

### Componentes

| Componente | Qué detecta                   | Variables                                                  |
| ---------- | ----------------------------- | ---------------------------------------------------------- |
| **A**      | Ganó mucho contra las odds    | ΔELO = K_base×ln(1+V)×(S-Prob_mkt), V = volumen USDC       |
| **B**      | Cuenta nueva + apuesta tardía | Age = días desde primera tx, Δt_res = horas hasta cierre   |
| **C**      | Urgencia + concentración      | α_slippage = impacto en precio, H = entropía del portfolio |

### Interpretación

| I_score          | Perfil                            |
| ---------------- | --------------------------------- |
| Bajo             | Ruido de fondo, trader normal     |
| Medio-Alto (P95) | Posible forecaster hábil, revisar |
| Muy Alto (P99)   | Probable insider, investigar      |

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

## Arquitectura

```
┌─────────────────────────────────────────────────────────────────┐
│                                                                 │
│  [Polymarket APIs]    [Polygonscan]    [WebSocket]             │
│         │                   │               │                   │
│         ▼                   ▼               ▼                   │
│  ┌─────────────────────────────────────────────────┐           │
│  │              PERSONA A: INGESTION               │           │
│  │  • Collectors (REST + WebSocket)                │           │
│  │  • Normalización a contratos                    │           │
│  │  • Rate limiting, retry, checkpointing          │           │
│  └──────────────────────┬──────────────────────────┘           │
│                         │ MarketDataBatch                       │
│                         ▼                                       │
│  ┌─────────────────────────────────────────────────┐           │
│  │           PERSONA B: QUEUE + ORCHESTRATION      │           │
│  │  • Redis Streams                                │           │
│  │  • Job creation, priorización                   │           │
│  │  • Retry logic, dead letter queue               │           │
│  └──────────────────────┬──────────────────────────┘           │
│                         │ ProcessingJob                         │
│                         ▼                                       │
│  ┌─────────────────────────────────────────────────┐           │
│  │            PERSONA C: WORKERS + DB              │           │
│  │  • Cálculo de I_score                           │           │
│  │  • PostgreSQL + TimescaleDB                     │           │
│  │  • Generación de alertas                        │           │
│  └─────────────────────────────────────────────────┘           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

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
│   ├── collectors/
│   │   ├── __init__.py
│   │   ├── markets_collector.py      # Gamma API - mercados
│   │   ├── trades_collector.py       # Data API - trades
│   │   ├── wallet_age_collector.py   # Data API + Polygonscan
│   │   └── realtime_listener.py      # WebSocket
│   ├── normalizers/
│   │   ├── __init__.py
│   │   └── data_normalizer.py        # Raw → Contratos
│   ├── publisher.py                  # Enviar a Redis
│   ├── backfill_runner.py            # Backfill histórico
│   └── config.py                     # API keys, rate limits
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

### A → B: `MarketDataBatch`

```python
# contracts/raw_market_data.py
from pydantic import BaseModel
from datetime import datetime
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

class RawMarketMetadata(BaseModel):
    market_id: str
    question: str
    end_date: datetime
    closed: bool
    outcome_prices: list[float] | None    # [1.0, 0.0] si resuelto
    volume_usdc: float

class RawWalletAge(BaseModel):
    wallet: str
    first_tx_polymarket: datetime | None
    first_tx_polygon: datetime | None     # De Polygonscan

class MarketDataBatch(BaseModel):
    metadata: RawMarketMetadata
    trades: list[RawTrade]
    wallet_ages: dict[str, RawWalletAge]
    ingested_at: datetime
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
from contracts.raw_market_data import MarketDataBatch, RawTrade, RawMarketMetadata

class QueueProducer(ABC):
    @abstractmethod
    async def publish_market_batch(self, batch: MarketDataBatch) -> str:
        """Publica batch completo de mercado histórico. Retorna batch_id."""
        pass

    @abstractmethod
    async def publish_realtime_trade(
        self,
        trade: RawTrade,
        market: RawMarketMetadata
    ) -> str:
        """Publica trade en tiempo real (alta prioridad). Retorna job_id."""
        pass

    @abstractmethod
    async def health_check(self) -> bool:
        """Verifica conexión con Redis."""
        pass
```

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

## Ejemplos de Mensajes del Publisher

### `publish_market_batch()` → `stream:raw_market_data`

Mensaje enviado a Redis:

```json
{
  "batch_id": "batch_0x1234abcd5678_a1b2c3d4",
  "market_id": "0x1234abcd5678efgh9012ijkl3456mnop",
  "type": "market_batch",
  "priority": "normal",
  "trades_count": "47",
  "wallets_count": "23",
  "published_at": "2026-01-30T15:30:00.123456",
  "data": "{...MarketDataBatch serializado...}"
}
```

El campo `data` contiene el `MarketDataBatch` completo:

```json
{
  "metadata": {
    "market_id": "0x1234abcd5678efgh9012ijkl3456mnop",
    "question": "¿Dimitirá el CEO antes del 15 de febrero?",
    "end_date": "2026-02-15T00:00:00Z",
    "closed": true,
    "outcome_prices": [1.0, 0.0],
    "volume_usdc": 125000.50
  },
  "trades": [
    {
      "tx_hash": "0xabc123def456...",
      "wallet": "0x9876fedcba...",
      "market_id": "0x1234abcd5678efgh9012ijkl3456mnop",
      "timestamp": "2026-02-14T22:30:00Z",
      "side": "BUY",
      "outcome": "Yes",
      "price": 0.65,
      "size_usdc": 10000.0,
      "size_tokens": 15384.61
    }
  ],
  "wallet_ages": {
    "0x9876fedcba...": {
      "wallet": "0x9876fedcba...",
      "first_tx_polymarket": "2026-02-14T20:00:00Z",
      "first_tx_polygon": "2026-02-14T19:45:00Z"
    }
  },
  "ingested_at": "2026-01-30T15:30:00.100000Z"
}
```

### `publish_realtime_trade()` → `stream:realtime_trades`

```json
{
  "job_id": "rt_0xabc123def456_f7e8d9c0",
  "type": "realtime_trade",
  "priority": "high",
  "market_id": "0x1234abcd5678efgh9012ijkl3456mnop",
  "published_at": "2026-01-30T15:30:05.789012",
  "trade": "{...RawTrade serializado...}",
  "market": "{...RawMarketMetadata serializado...}"
}
```

### Resumen Visual

```
┌─────────────────────────────────────────────────────────────┐
│                    BACKFILL (batch)                         │
│  stream:raw_market_data                                     │
│  ┌───────────────────────────────────────────────────────┐  │
│  │ batch_id + metadata + N trades + M wallet_ages        │  │
│  │ priority: normal                                      │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                   REALTIME (individual)                     │
│  stream:realtime_trades                                     │
│  ┌───────────────────────────────────────────────────────┐  │
│  │ job_id + 1 trade + market metadata                    │  │
│  │ priority: high                                        │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### Cómo Levantar el Publisher

El publisher se ejecuta como parte del servicio `ingestion` en docker-compose:

```bash
# Levantar solo Redis (requerido)
docker-compose up -d redis

# Ejecutar ingestion (incluye publisher) localmente
poetry run python -m ingestion.backfill_runner --days=30 --min-volume=50000

# O levantar todo con Docker
docker-compose up -d redis ingestion
```

El publisher NO es un servicio independiente. Se usa internamente desde:
- `backfill_runner.py` para publicar batches históricos
- `realtime_listener.py` (futuro) para trades en tiempo real

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
│  │  (Gamma API)      │ │  (Data API)       │ │   (Data API + Polygonscan)│  │
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
POLYGONSCAN_API_KEY=your_key_here

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
POLYGONSCAN_REQUESTS_PER_SECOND=5

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
    command: python -m ingestion.backfill_runner
    env_file: .env
    depends_on:
      - redis

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

---

## Dependencias

```toml
# pyproject.toml
[tool.poetry]
name = "polymarket-insider-detection"
version = "0.1.0"
python = "^3.11"

[tool.poetry.dependencies]
python = "^3.11"

# APIs y HTTP
aiohttp = "^3.9"
httpx = "^0.27"
websockets = "^12.0"

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

[tool.poetry.group.dev.dependencies]
pytest = "^8.0"
pytest-asyncio = "^0.23"
pytest-cov = "^4.1"
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

# Ejecutar backfill histórico
python -m scripts.run_backfill --days=180 --min-volume=50000

# Ejecutar workers
python -m workers.worker --id=worker-1

# Analizar un mercado específico
python -m scripts.analyze_market --market-id=0xabc123...

# Tests
pytest tests/unit -v
pytest tests/integration -v --cov=.
```

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

## Checklist de Desarrollo

### Persona A (Ingestion)

- [ ] Collector de mercados cerrados con paginación
- [ ] Collector de trades con paginación
- [ ] Collector de edad de wallets (Polymarket + Polygonscan)
- [ ] WebSocket listener para tiempo real
- [ ] Normalización a contratos `RawTrade`, `RawMarketMetadata`
- [ ] Rate limiting con backoff exponencial
- [ ] Checkpointing para backfill resumible
- [ ] Tests unitarios de normalización
- [ ] Test de integración con mock de APIs

### Persona B (Queue)

- [ ] Setup de Redis Streams
- [ ] Implementar `QueueProducer`
- [ ] Implementar `QueueConsumer`
- [ ] Job creator (MarketDataBatch → ProcessingJob)
- [ ] Cola de prioridades
- [ ] Retry con backoff exponencial
- [ ] Dead letter queue
- [ ] Métricas de queue depth
- [ ] Tests unitarios de cada componente
- [ ] Test de integración producer→consumer

### Persona C (Workers + DB)

- [ ] Schema de PostgreSQL + migraciones
- [ ] Repositorios (wallets, trades, scores, alerts)
- [ ] Calculadora componente A (ΔELO × ln(1+V))
- [ ] Calculadora componente B (1/√(Age × Δt_res))
- [ ] Calculadora componente C (α × e^(-H))
- [ ] Orquestador de I_score
- [ ] Cálculo de percentiles por mercado
- [ ] Generador de alertas
- [ ] Worker loop principal
- [ ] Tests unitarios de cada componente
- [ ] Test con datos reales de Polymarket

---

## Ejemplo: Caso de Uso Completo

```
1. Mercado "¿Dimitirá el CEO?" cierra el 15 Feb

2. INGESTION detecta que el mercado cerró
   → GET /markets?closed=true
   → GET /activity?market=0x123...
   → Publica MarketDataBatch a Redis

3. QUEUE crea ProcessingJob
   → Prioridad NORMAL (mercado recién cerrado)
   → Encola en Redis Stream

4. WORKER consume el job
   → Para cada trade, calcula I_score
   → Trade sospechoso: wallet nueva, $10K a 2h del cierre, 0 diversificación
   → I_score = 257, percentil 99.2%
   → Genera Alert tipo P99

5. Resultado en DB:
   - scores: {wallet: 0xABC, i_score: 257, percentile: 99.2, ...}
   - alerts: {wallet: 0xABC, alert_type: P99, status: NEW}
```

---

## Notas Importantes

1. **Rate Limits:** Polymarket ~100 req/min, Polygonscan ~5 req/seg. Implementar delays.

2. **Proxy Wallets:** Los usuarios de Polymarket.com usan proxy wallets. La dirección visible puede no ser la EOA original.

3. **Idempotencia:** Los workers deben ser idempotentes. Mismo job_id = mismo resultado.

4. **Calibración de θ:** Los valores iniciales (θ₁=1, θ₂=10, θ₃=10, θ₄=5) son estimaciones. Calibrar con datos reales.

5. **Edad de wallet:** La Data API solo muestra actividad en Polymarket. Para edad real en blockchain, usar Polygonscan.

---

## 📋 PLAN DE IMPLEMENTACIÓN - PERSONA A (Ingestion)

> **Estado:** ✅ Completado (excepto realtime_listener)
> **Última actualización:** 2026-01-29

### Fase 1: Fundamentos ✅

| Archivo | Estado | Descripción |
|---------|--------|-------------|
| `ingestion/config.py` | ✅ Completado | URLs de APIs, rate limits, settings desde .env |
| `ingestion/http_client.py` | ✅ Completado | Cliente HTTP async con rate limiting y retry |

### Fase 2: Collectors ✅

| Archivo | Estado | Descripción |
|---------|--------|-------------|
| `ingestion/collectors/markets_collector.py` | ✅ Completado | Gamma API - mercados cerrados con paginación |
| `ingestion/collectors/trades_collector.py` | ✅ Completado | Data API - trades por mercado (param: `market`) |
| `ingestion/collectors/wallet_age_collector.py` | ✅ Completado | Data API + Polygonscan - edad de wallets |
| `ingestion/collectors/realtime_listener.py` | ❌ **BLOQUEADO** | WebSocket para trades en tiempo real (ver nota abajo) |

### Fase 3: Normalización ✅

| Archivo | Estado | Descripción |
|---------|--------|-------------|
| `ingestion/normalizers/data_normalizer.py` | ✅ Completado | Raw JSON → Contratos Pydantic |

### Fase 4: Publisher ✅

| Archivo | Estado | Descripción |
|---------|--------|-------------|
| `ingestion/publisher.py` | ✅ Completado | Implementación de QueueProducer → Redis Streams |

### Fase 5: Orquestación ✅

| Archivo | Estado | Descripción |
|---------|--------|-------------|
| `ingestion/backfill_runner.py` | ✅ Completado | Orquestador con CLI, checkpointing, dry-run |

### Scripts de Test

| Script | Estado | Comando |
|--------|--------|---------|
| `scripts/test_markets.py` | ✅ Verificado | `poetry run python -m scripts.test_markets` |
| `scripts/test_trades.py` | ✅ Verificado | `poetry run python -m scripts.test_trades` |
| `scripts/test_normalizer.py` | ✅ Verificado | `poetry run python -m scripts.test_normalizer` |
| `scripts/test_wallet_age.py` | ⚠️ **PENDIENTE** | `poetry run python -m scripts.test_wallet_age` |
| `scripts/test_publisher.py` | ⚠️ **PENDIENTE** | `poetry run python -m scripts.test_publisher` |
| `scripts/test_backfill.py` | ✅ Verificado | `poetry run python -m scripts.test_backfill` |

> ⚠️ **BLOQUEADO:** `test_wallet_age.py` requiere `POLYGONSCAN_API_KEY` configurada en `.env`.
> ⚠️ **BLOQUEADO:** `test_publisher.py` requiere Redis (Persona B debe configurar `job_queue/`).

### ❌ Realtime Listener - Por qué está bloqueado

El `realtime_listener.py` **no se puede construir actualmente** debido a limitaciones de la API de Polymarket:

#### Opciones de WebSocket disponibles

| Datos | `/ws/user` (Privado) | `/ws/market` (Público) |
|-------|---------------------|------------------------|
| Tus órdenes | ✅ Sí | ❌ No |
| Órdenes de otros | ❌ No | ❌ No (Anónimo) |
| Trades globales | ❌ No | ✅ Sí |
| Libro de órdenes (L2) | ❌ No | ✅ Sí |

#### El problema

1. **WebSocket Público (`/ws/market`)**: Muestra trades en tiempo real, pero el **CLOB de Polymarket es anónimo**. No revela la wallet de quien hizo la operación.

2. **WebSocket Privado (`/ws/user`)**: Solo muestra **tus propias** operaciones, no las de otros usuarios.

3. **Trades públicos**: Puedes suscribirte al canal `trades` para ver ejecuciones globales, pero sin identificar wallets:
   ```json
   {
     "type": "subscribe",
     "assets_ids": ["ID_DEL_MERCADO"],
     "channels": ["trades"]
   }
   ```

#### Solución futura: Rastreo On-Chain (Polygon)

Polymarket liquida las operaciones en la red de **Polygon**. La solución será:

- Monitorear transacciones directamente en blockchain via **Polygonscan API** o webhooks (Alchemy/QuickNode)
- **Limitación:** Solo se verán operaciones cuando ya se han confirmado en la red, no órdenes límite pendientes en el CLOB

> 📅 **TODO:** Implementar rastreo on-chain de Polygon para monitoreo en tiempo real (fase futura).

### Leyenda

- ✅ Completado
- 🔄 En progreso
- ⏳ Pendiente
- ❌ Bloqueado