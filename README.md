# Polymarket Insider Detection System

Sistema de detección de insider trading en Polymarket mediante análisis de patrones de comportamiento.

## Setup

### Requisitos
- Python 3.11+
- Docker y Docker Compose
- Poetry

### Instalación

```bash
# Instalar dependencias
poetry install
poetry add psycopg2-binary sqlalchemy asyncpg celery

# Levantar servicios (Redis + PostgreSQL)
docker-compose up -d redis postgres

# Ejecutar migraciones
alembic upgrade head

# Configurar variables de entorno
cp .env.example .env
# Editar .env con tus API keys
```

## Uso

### Ejecutar backfill histórico
```bash
python -m scripts.run_backfill --days=180 --min-volume=50000
```

### Ejecutar workers
```bash
poetry run celery -A workers.celery_app worker --loglevel=info
```

### Analizar un mercado específico
```bash
python -m scripts.analyze_market --market-id=0xabc123...
```

## Arquitectura

Ver [CLAUDE.md](CLAUDE.md) para documentación completa del sistema.

## Testing

```bash
# Tests unitarios
pytest tests/unit -v

# Tests de integración
pytest tests/integration -v --cov=.
```

### Scripts de prueba (carpeta `scripts/`)

Scripts para probar componentes individuales contra las APIs reales:

```bash
# Test del collector de mercados
python -m scripts.test_markets

# Test del collector de trades
python -m scripts.test_trades

# Test del normalizador de datos
python -m scripts.test_normalizer

# Test del collector de wallet age (requiere POLYGONSCAN_API_KEY)
python -m scripts.test_wallet_age

# Test del publisher a Redis (requiere Redis)
python -m scripts.test_publisher

# Test del backfill completo (dry run, sin Redis)
python -m scripts.test_backfill
```
