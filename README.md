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
python -m workers.worker --id=worker-1
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
