# Polymarket Insider Detection System

Sistema de detección de insider trading en Polymarket. Monitorea mercados de predicción en tiempo real, calcula variables estadísticas al cierre de cada mercado y aplica un criterio multivariable para identificar wallets con comportamiento anómalo.

## Arquitectura

El sistema se organiza en 6 capas:

| Capa | Nombre | Tecnología |
|------|--------|------------|
| **A** | Ingesta | Python async (`ingestion/`) |
| **B** | Cola + Orchestrator | Redis Streams + Python async (`queue_system/`) |
| **C** | Base de Datos | PostgreSQL / TimescaleDB (`database/`) |
| **D** | Motor de Cálculo | R + Plumber (`inference/`) |
| **E** | REST API | FastAPI (`api/`) |
| **T** | Tasks | Celery (`workers/`) |

El flujo completo: L1 detecta cierre de mercado → publica batch a Redis → Orchestrator persiste en BD → Celery calcula variables → Motor R aplica criterio insider → API actualiza wallets → L2 monitorea trades futuros de insiders.

Para documentación completa ver [CLAUDE.md](CLAUDE.md) y [docs/FRONTEND.md](docs/FRONTEND.md).

## Requisitos

- Python 3.11+
- Docker y Docker Compose
- Poetry
- API key de Etherscan (gratuita): https://etherscan.io/myapikey

## Instalación

```bash
# 1. Clonar e instalar dependencias
poetry install

# 2. Configurar variables de entorno
cp .env.example .env
# Editar .env con tus valores (ver sección Variables de Entorno)

# 3. Levantar todos los servicios
docker-compose up -d
```

Las migraciones de base de datos se ejecutan automáticamente al levantar el servicio `orchestrator`.

## Variables de Entorno

Copia `.env.example` a `.env` y rellena:

```env
# Obligatorio: Etherscan API V2 (Polygon chain, para edad de wallets)
ETHERSCAN_API_KEY=your_key_here

# Base de datos (local dev usa puerto 5433; Docker sobreescribe a 5432)
DATABASE_URL=postgresql://username:password@localhost:5433/dbname

# Redis
REDIS_URL=redis://localhost:6379/0

# Categorías a monitorear (tags/categorías de la Gamma API, separadas por coma)
CATEGORIAS_DE_INTERES=Sports,Crypto,Politics,Geopolitics,Elections

# Contraseña del dashboard web
DASHBOARD_PASSWORD=your_password_here
```

### Categorías disponibles

Los valores válidos para `CATEGORIAS_DE_INTERES` son los `label` de tags de la Gamma API o el campo `category` del mercado. Ejemplos:

- Deportes: `Sports`
- Crypto: `Crypto`
- Política: `Politics`, `Elections`, `Geopolitics`, `World Affairs`, `US Election`
- Finanzas: `Finance`, `Tariffs`, `Trade War`
- Tecnología: `Tech`, `AI`

La comparación es case-insensitive.

## Servicios Docker

| Servicio | Puerto | Descripción |
|----------|--------|-------------|
| `redis` | 6379 | Cola de mensajes (Redis Streams) |
| `postgres` | 5433 | Base de datos (TimescaleDB) |
| `ingestion` | — | L1 + L2: descubrimiento y monitoreo en tiempo real |
| `orchestrator` | — | Consume Redis → persiste en BD → dispara Celery |
| `worker` (x4) | — | Celery workers: T2 (variables), T3 (comandos), T4 (limpieza) |
| `beat` | — | Celery Beat: tareas periódicas (limpieza cada hora) |
| `inference` | 8000 (interno) | Motor R + Plumber: ajuste de distribuciones + criterio insider |
| `api` | **8001** | REST API FastAPI: recibe resultados del Motor R |
| `dashboard` | **8002** | Dashboard web de monitoreo |

### Comandos útiles

```bash
# Levantar todo
docker-compose up -d

# Ver logs de un servicio
docker-compose logs -f ingestion
docker-compose logs -f worker

# Solo infraestructura (Redis + Postgres), para desarrollo local
docker-compose up -d redis postgres

# Ejecutar backfill histórico (perfil tools)
docker-compose --profile tools run backfill

# Estado de los servicios
docker-compose ps
```

### Inspección de Redis

```bash
# Longitud de streams
docker exec cfg-data-redis-1 redis-cli XLEN closed_markets
docker exec cfg-data-redis-1 redis-cli XLEN insider_trades
docker exec cfg-data-redis-1 redis-cli XLEN insider_commands

# Mensajes pendientes por consumer group
docker exec cfg-data-redis-1 redis-cli XPENDING closed_markets scheduler
docker exec cfg-data-redis-1 redis-cli XPENDING insider_trades tasks

# Últimos mensajes
docker exec cfg-data-redis-1 redis-cli XRANGE closed_markets - + COUNT 3
```

## Desarrollo Local

```bash
# Infraestructura
docker-compose up -d redis postgres

# L1 + L2 (ingestion)
poetry run python -m ingestion.main

# Orchestrator
poetry run python -m queue_system

# Celery worker
poetry run celery -A workers.celery_app worker --loglevel=info

# Celery beat (tareas periódicas)
poetry run celery -A workers.celery_app beat --loglevel=info

# REST API
poetry run uvicorn api.main:app --host 0.0.0.0 --port 8001

# Dashboard
poetry run uvicorn dashboard.main:app --host 0.0.0.0 --port 8002

# Backfill histórico
poetry run python -m ingestion.backfill_runner --days=30 --min-volume=50000
```

## Dashboard

Accesible en `http://<host>:8002`. Requiere `DASHBOARD_PASSWORD` en `.env`.

Tecnologías: FastAPI + Jinja2 (SSR), Tailwind CSS, Alpine.js, Chart.js, SSE para actualizaciones en tiempo real.

## API REST

Accesible en `http://<host>:8001`.

| Endpoint | Método | Descripción |
|----------|--------|-------------|
| `/health` | GET | Estado del servicio |
| `/insider-detection` | POST | Recibe resultados del Motor R |
| `/docs` | GET | Swagger UI |

## Tests

Los scripts de test **los ejecuta siempre el usuario**, nunca de forma automática.

```bash
# Ejecutar todos los tests
poetry run python -m scripts.run_all_tests

# Ver tests disponibles
poetry run python -m scripts.run_all_tests --list

# Tests individuales
poetry run python -m scripts.test_markets           # Gamma API
poetry run python -m scripts.test_trades            # Data API
poetry run python -m scripts.test_normalizer        # Normalización (sin dependencias)
poetry run python -m scripts.test_categories        # Extracción de categorías
poetry run python -m scripts.test_wallet_age        # Etherscan API V2
poetry run python -m scripts.test_publisher         # Redis
poetry run python -m scripts.test_backfill          # Backfill runner
poetry run python -m scripts.test_market_close_listener   # L1
poetry run python -m scripts.test_insider_trades_listener # L2
poetry run python -m scripts.test_realtime_listener       # WebSocket on-chain
poetry run python -m scripts.test_ingestion_to_queue      # Integración A→B
poetry run python -m scripts.test_full_pipeline           # Pipeline completo (todos los servicios)
```

### Test del pipeline completo

Requiere todos los servicios activos:

```bash
docker-compose up -d redis postgres orchestrator worker inference api ingestion
poetry run python -m scripts.test_full_pipeline
```

Verifica 22 checks cubriendo L1→T1→T2→D→E→T3→L2→T4.
