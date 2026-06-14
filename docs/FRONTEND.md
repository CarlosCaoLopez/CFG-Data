# Dashboard de Monitoreo — Documentación Técnica

> **Puerto:** `http://localhost:8002`
> **Servicio Docker:** `dashboard`
> **Código:** `dashboard/`
> **Stack:** FastAPI + Jinja2 + Tailwind CSS CDN + Alpine.js + Chart.js

---

## Tabla de contenidos

1. [Visión general](#1-visión-general)
2. [Arquitectura del dashboard](#2-arquitectura-del-dashboard)
3. [Cómo levantar el dashboard](#3-cómo-levantar-el-dashboard)
4. [Páginas y qué muestra cada una](#4-páginas-y-qué-muestra-cada-una)
5. [API JSON — endpoints disponibles](#5-api-json--endpoints-disponibles)
6. [Actualización en tiempo real (SSE)](#6-actualización-en-tiempo-real-sse)
7. [Stats de L1 (ingestion container)](#7-stats-de-l1-ingestion-container)
8. [Estructura de archivos](#8-estructura-de-archivos)
9. [Flujo de una petición de principio a fin](#9-flujo-de-una-petición-de-principio-a-fin)
10. [Troubleshooting](#10-troubleshooting)

---

## 1. Visión general

El dashboard es un **servicio FastAPI separado** que corre en el puerto `8002`. Es completamente **de solo lectura** — no escribe nada en la BD ni en Redis. Su único propósito es agregar datos de tres fuentes y presentarlos en una interfaz web:

| Fuente | Qué provee |
|--------|-----------|
| **PostgreSQL** (`postgres:5432`) | Wallets, markets, trades, insiders_trades, closed_market_variables |
| **Redis** (`redis:6379`) | Longitud y mensajes pendientes de los 4 streams |
| **Ingestion container** (`ingestion:9001`) | Estado en memoria de L1: markets tracked, estadísticas de descubrimiento |

El dashboard **no modifica ningún dato**. Si se cae, el pipeline de detección sigue funcionando sin interrupción.

---

## 2. Arquitectura del dashboard

```
Browser
  │
  ├── GET /                    → Jinja2 renderiza overview.html
  ├── GET /markets             → Jinja2 renderiza markets.html
  ├── ...                      → (una ruta HTML por página)
  │
  ├── GET /api/overview        → JSON: KPIs de BD
  ├── GET /api/markets         → JSON: lista paginada de markets
  ├── GET /api/insiders        → JSON: wallets insider
  ├── GET /api/pipeline        → JSON: Redis + Celery + L1 stats
  ├── GET /api/trades          → JSON: trades explorer
  ├── GET /api/variables       → JSON: closed_market_variables en vuelo
  │
  └── GET /api/sse/live        → text/event-stream (SSE, actualización cada 5s)
           │
           ├── event: overview_kpis    → actualiza contadores del sidebar
           ├── event: stream_lengths   → actualiza badge del sidebar Pipeline
           └── event: new_detection    → dispara toast notification
```

### Responsabilidades por archivo

```
dashboard/main.py          → App FastAPI, monta routers, configura Jinja2
dashboard/config.py        → URLs de BD/Redis/ingestion (lee variables de entorno)
dashboard/dependencies.py  → Pool de conexiones async a BD y Redis (singletons)
dashboard/redis_client.py  → XLEN, XPENDING, Celery inspect
dashboard/l1_stats_client.py → GET http://ingestion:9001/stats
dashboard/queries/         → SQL puro (no ORM) para cada página
dashboard/routers/         → FastAPI routers (uno por página + SSE)
dashboard/templates/       → HTML con Alpine.js inline
```

### Frontend: sin build, sin bundler

No hay `package.json`, no hay webpack, no hay React. Todo el frontend es HTML servido por Jinja2 con dependencias cargadas desde CDN:

```html
<script src="https://cdn.tailwindcss.com"></script>
<script src="https://cdn.jsdelivr.net/npm/alpinejs@3.x.x/dist/cdn.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"></script>
```

La interactividad (filtros, paginación, paneles de detalle, refresh automático) la maneja **Alpine.js**, que funciona con atributos HTML (`x-data`, `x-init`, `x-for`, `x-model`). No hay ningún archivo `.js` separado.

---

## 3. Cómo levantar el dashboard

```bash
# Primera vez (construye imagen)
docker-compose up -d --build dashboard

# Arranques posteriores
docker-compose up -d dashboard

# Ver logs en vivo
docker-compose logs -f dashboard

# Reconstruir tras cambiar código Python o templates
docker-compose up -d --build dashboard
```

Accede en: **http://localhost:8002**

> **Nota:** El servicio `ingestion` debe estar corriendo para que la página Pipeline muestre los stats de L1. Si `ingestion` no está disponible, las otras 5 páginas funcionan igualmente — Pipeline simplemente muestra "L1 unavailable".

---

## 4. Páginas y qué muestra cada una

### Overview (`/`)

La pantalla principal. Se actualiza automáticamente via SSE sin necesidad de recargar.

**KPI cards:**
- Wallets totales en BD
- Insiders detectados (total + cuántos en las últimas 24h)
- Markets procesados (cerrados / total)
- Trades insider / Trades totales
- CMV en vuelo (cuántos markets están siendo analizados ahora por Motor D)

**Gráfico de categorías:** Doughnut chart con la distribución de insiders por categoría (`categories_insider`). Se renderiza con Chart.js.

**Feed de detecciones recientes:** Las últimas 10 wallets marcadas como `insider=true`, ordenadas por `updated_at DESC`.

Los contadores del sidebar (badge rojo en Insiders, badge amarillo en Variables) se actualizan en tiempo real via SSE sin recargar la página.

---

### Markets (`/markets`)

Lista paginada de todos los markets en la BD, 50 por página.

**Filtros disponibles:**
- Estado: `Todos` / `Cerrados` / `Abiertos` (filtra por `markets.closed`)
- Categoría: campo libre, filtra por `ANY(m.categories)`

**Columnas:** Pregunta, Categorías, Volumen USDC, Nº Trades, Nº Insiders, Estado (badge verde/amarillo), Fecha de cierre.

**Comportamiento:** Hacer click en una fila actualmente no abre detalle (el campo `detail` está preparado en la URL pero no implementado como subpágina — extensión futura).

---

### Insiders (`/insiders`)

Lista de todas las wallets con `insider = true`.

**Filtro:** por categoría insider.

**Columnas:** Wallet (truncada), Categorías insider (badges rojos), Nº markets, Nº trades insider, Volumen total, Antigüedad en Polygon (`first_tx_polygon`), Fecha de detección (`updated_at`).

**Panel de detalle (click en fila):** Llama a `GET /api/insiders/{wallet}` y muestra un panel inferior con:
- KPIs del wallet (trades, volumen, markets, fecha primera tx)
- Tabla de todos sus insider_trades con market, timestamp, side, outcome, precio, tamaño, flag `last_trade`

---

### Pipeline (`/pipeline`)

Estado del sistema en tiempo real. Se refresca cada 8 segundos.

**Redis Streams:** Barra de progreso por cada stream (`closed_markets`, `raw_data`, `insider_trades`, `insider_commands`). La barra es amarilla si hay mensajes pendientes, verde si no. Muestra longitud total y mensajes pendientes (XPENDING).

**Celery Workers:** Workers online, tasks activas, tasks programadas, nombres de workers.

**L1 — Market Close Listener:**
- Metrics: tracked ahora, publicados, descubiertos, filtrados
- Lista de markets que L1 está monitoreando actualmente (question + categoría)

Si `ingestion:9001` no responde, el bloque L1 muestra un badge "Unavailable" y las demás secciones siguen funcionando.

---

### Trades (`/trades`)

Explorer de trades con filtros.

**Filtros:**
- Fuente: `Insider trades` / `Trades normales` / `Todos` (UNION SQL)
- Lado: `BUY` / `SELL`
- Wallet: campo libre (filtra por `wallet_id`)

**Columnas:** Wallet, Market (pregunta truncada), Timestamp, Side (badge), Outcome, Price, USDC, Fuente (badge rojo=insider / azul=normal), Last (✓ si `last_trade=true`).

---

### Variables (`/variables`)

Muestra el contenido actual de `closed_market_variables` — la tabla temporal que existe solo mientras Motor D está procesando un market.

**Vista principal:** Una card por market en vuelo con resumen (wallet count, row count, Δt medio, H medio). Si la tabla está vacía, muestra "No hay markets siendo procesados ahora".

**Drill-down (click en card):** Tabla con todas las filas CMV del market: wallet, Δt (horas al cierre), Age (días antigüedad), Winning Volume, α Slippage, H (entropía).

> Esta página es la más útil para diagnosticar si Motor D está procesando o se ha atascado. Si hay filas aquí desde hace más de 5 minutos, puede haber un problema en el Motor R.

---

## 5. API JSON — endpoints disponibles

Todos devuelven JSON. Útiles para hacer `curl` directo durante troubleshooting.

```bash
# Overview KPIs
curl http://localhost:8002/api/overview | python -m json.tool

# Markets (paginado)
curl "http://localhost:8002/api/markets?page=1&limit=10&closed=true"
curl "http://localhost:8002/api/markets?category=Sports"

# Market específico con CMV
curl "http://localhost:8002/api/markets/0xabc123..."

# Insiders
curl "http://localhost:8002/api/insiders"
curl "http://localhost:8002/api/insiders?category=Crypto"

# Insider específico con sus trades
curl "http://localhost:8002/api/insiders/0xwallet..."

# Pipeline completo
curl http://localhost:8002/api/pipeline | python -m json.tool

# Trades (por defecto: insider_trades)
curl "http://localhost:8002/api/trades?table=insider&limit=20"
curl "http://localhost:8002/api/trades?table=all&side=BUY"
curl "http://localhost:8002/api/trades?wallet=0xabc..."

# Variables CMV (resumen)
curl http://localhost:8002/api/variables

# Variables CMV (detalle de un market)
curl "http://localhost:8002/api/variables?market_id=0xabc..."
```

### Swagger UI

FastAPI genera documentación interactiva automáticamente en:
```
http://localhost:8002/api/docs
```

---

## 6. Actualización en tiempo real (SSE)

El dashboard usa **Server-Sent Events** (SSE) — una conexión HTTP unidireccional del servidor al browser que mantiene abierta y empuja eventos cada 5 segundos.

### Cómo funciona

```
Browser → GET /api/sse/live → conexión HTTP abierta indefinidamente
                               ↓ cada 5 segundos:
                               event: overview_kpis
                               data: {"total_wallets": 142, "total_insiders": 7, ...}

                               event: stream_lengths
                               data: {"closed_markets": 0, "raw_data": 0, ...}

                               event: new_detection   ← solo si total_insiders creció
                               data: {"count": 8}
```

### Qué actualiza cada evento

| Evento | Qué actualiza en el browser |
|--------|----------------------------|
| `overview_kpis` | Contadores del sidebar (badge Insiders, badge Variables), timestamp "Actualizado" |
| `stream_lengths` | Badge amarillo en el ítem Pipeline del sidebar |
| `new_detection` | Toast notification: "🚨 Nuevo insider detectado (total: N)" |

### Reconexión automática

`EventSource` reconecta automáticamente si la conexión se pierde. El indicador verde/rojo en la parte inferior del sidebar muestra el estado:
- 🟢 `Live` — conexión SSE activa
- 🔴 `Reconectando...` — perdida, intentando de nuevo en 5 segundos

### Limitación importante

El evento `new_detection` **no se dispara en el primer arranque** (cuando `_prev_insider_count = 0`). Esto es intencional para evitar spamear notificaciones al iniciar el dashboard si ya hay insiders en BD.

---

## 7. Stats de L1 (ingestion container)

L1 (`MarketCloseListener`) mantiene en memoria:
- `_tracked_markets` — dict de markets que está monitoreando activamente
- `_stats` — contadores de descubrimiento

Estos datos no son accesibles desde fuera del proceso sin algún mecanismo de IPC.

### Solución: stats server interno (puerto 9001)

Al arrancar `ingestion`, se levanta un servidor FastAPI mínimo en el puerto `9001` (solo accesible dentro de la red Docker `proyect_net`, no expuesto al host).

**Archivos:**
- `ingestion/stats_server.py` — define el servidor con el endpoint `/stats`
- `ingestion/main.py` — lo arranca como cuarta tarea en `asyncio.gather`

**Endpoint:**
```
GET http://ingestion:9001/stats
```

Respuesta:
```json
{
  "l1": {
    "markets_discovered_ws": 156,
    "markets_discovered_gamma": 42,
    "markets_closed_published": 12,
    "markets_filtered_out": 89,
    "tracked_markets": 7,
    "tracked_market_list": [
      {"market_id": "0xabc...", "question": "¿Ganará X?", "category": "Sports", "volume_usdc": 25000}
    ]
  },
  "l2": {}
}
```

El dashboard llama a este endpoint en `GET /api/pipeline` via `dashboard/l1_stats_client.py` con un timeout de 2 segundos. Si falla (ingestion caído, port no disponible), devuelve `null` y el frontend muestra "L1 unavailable" sin romper nada.

---

## 8. Estructura de archivos

```
dashboard/
├── __init__.py
├── main.py                   # FastAPI app: monta routers, configura Jinja2
├── config.py                 # DATABASE_URL, REDIS_URL, INGESTION_STATS_URL
├── dependencies.py           # Pool BD async + singleton Redis
├── redis_client.py           # get_stream_metrics(), get_celery_stats()
├── l1_stats_client.py        # GET http://ingestion:9001/stats
├── routers/
│   ├── overview.py           # GET /api/overview
│   ├── markets.py            # GET /api/markets, /api/markets/{id}
│   ├── insiders.py           # GET /api/insiders, /api/insiders/{wallet}
│   ├── pipeline.py           # GET /api/pipeline (Redis + Celery + L1)
│   ├── trades.py             # GET /api/trades
│   ├── variables.py          # GET /api/variables
│   └── sse.py                # GET /api/sse/live
├── queries/
│   ├── overview_queries.py   # get_kpis(), get_top_categories(), get_recent_detections()
│   ├── market_queries.py     # get_markets(), get_market_detail()
│   ├── insider_queries.py    # get_insiders(), get_insider_detail()
│   ├── trade_queries.py      # get_trades()
│   └── variable_queries.py   # get_variables_summary(), get_variables_detail()
└── templates/
    ├── base.html             # Layout con sidebar, SSE JS, toast, Alpine.js
    ├── overview.html         # KPIs + gráfico + feed
    ├── markets.html          # Tabla paginada con filtros
    ├── insiders.html         # Lista wallets + panel de detalle
    ├── pipeline.html         # Redis bars + Celery + L1 stats
    ├── trades.html           # Trade explorer
    └── variables.html        # CMV en vuelo

ingestion/stats_server.py     # Mini FastAPI interna (puerto 9001)
```

---

## 9. Flujo de una petición de principio a fin

### Ejemplo: usuario abre la página Pipeline

```
1. Browser → GET /pipeline
2. dashboard/main.py → templates.TemplateResponse("pipeline.html", ...)
3. Jinja2 renderiza pipeline.html extendiendo base.html
4. Browser recibe HTML + inicia SSE via EventSource('/api/sse/live')
5. Alpine.js ejecuta x-init="load(); setInterval(() => load(), 8000)"

6. load() → fetch('/api/pipeline')
7. dashboard/routers/pipeline.py → asyncio.gather(3 coroutines en paralelo):
   a. get_stream_metrics(redis)  → XLEN + XPENDING para 4 streams
   b. get_celery_stats(REDIS_URL) → Celery inspect en thread pool (sync)
   c. get_l1_stats()             → httpx GET http://ingestion:9001/stats timeout=2s
8. Responde JSON con {"redis": {...}, "celery": {...}, "l1_stats": {...}}

9. Alpine.js actualiza DOM: barras de Redis, counters Celery, lista tracked markets

10. Cada 8 segundos → vuelve al paso 6

11. Paralelamente, SSE emite cada 5s → actualiza badge del sidebar Pipeline si
    hay streams con pending > 0
```

### Ejemplo: SSE detecta nuevo insider

```
1. sse.py: get_kpis(session) → total_insiders = 8
2. total_insiders (8) > _prev_insider_count (7) Y _prev_insider_count > 0
3. yield "event: new_detection\ndata: {\"count\": 8}\n\n"
4. Browser EventSource recibe evento
5. base.html: showToast("🚨 Nuevo insider detectado (total: 8)")
6. Toast aparece 5 segundos, desaparece solo
7. Sidebar badge Insiders actualizado en el mismo ciclo via overview_kpis
```

---

## 10. Troubleshooting

### El dashboard no arranca

```bash
docker-compose logs dashboard
```

**Error: `ModuleNotFoundError: No module named 'jinja2'`**
```bash
docker-compose up -d --build dashboard
# El Dockerfile instala dependencias de pyproject.toml. Jinja2 debe estar incluida.
```

**Error: `Address already in use` (puerto 8002)**
```bash
# Otro proceso usa el puerto
docker ps | grep 8002
# Cambiar el puerto en docker-compose.yml si es necesario
```

---

### El sidebar muestra "Reconectando..." (SSE caído)

La conexión SSE se pierde si el servidor se reinicia o hay un error en `/api/sse/live`. Alpine.js reconecta automáticamente en 5 segundos.

Si persiste, verificar:
```bash
# ¿El container sigue corriendo?
docker-compose ps dashboard

# ¿Hay error en el loop SSE?
docker-compose logs dashboard | grep -i "sse\|error"

# Probar el endpoint directamente (debe mantenerse abierto y emitir líneas)
curl -N http://localhost:8002/api/sse/live
```

---

### La página Pipeline muestra "L1 unavailable"

El dashboard no puede conectar con `ingestion:9001`. Causas posibles:

1. **El container `ingestion` no está corriendo:**
   ```bash
   docker-compose ps ingestion
   docker-compose up -d ingestion
   ```

2. **El stats server (puerto 9001) no arrancó dentro de ingestion:**
   ```bash
   docker-compose logs ingestion | grep -i "stats\|9001\|uvicorn"
   ```
   Debe verse algo como `Uvicorn running on http://0.0.0.0:9001`.

3. **Verificar que el endpoint responde desde dentro de la red Docker:**
   ```bash
   # Desde el container del dashboard
   docker exec cfg-data-dashboard-1 curl http://ingestion:9001/stats
   # Desde el container de ingestion
   docker exec cfg-data-ingestion-1 curl http://localhost:9001/stats
   ```

4. **Problema de red Docker:** Verificar que ambos servicios están en `proyect_net`:
   ```bash
   docker network inspect cfg-data_proyect_net | grep -A2 "ingestion\|dashboard"
   ```

---

### Los datos no se actualizan (página estancada)

Cada página tiene un `setInterval` diferente:

| Página | Intervalo fetch |
|--------|----------------|
| Overview | 15 segundos |
| Pipeline | 8 segundos |
| Markets, Insiders, Trades, Variables | Manual (botón ↻) |

Los KPIs del **sidebar** se actualizan cada 5 segundos via SSE independientemente de la página. Si el sidebar se actualiza pero la tabla no, el botón "↻ Actualizar" fuerza un refresh manual.

---

### Datos incorrectos o desfasados

El dashboard lee directamente de PostgreSQL y Redis. Si los datos parecen incorrectos:

```bash
# Verificar en BD directamente
docker exec cfg-data-postgres-1 psql -U insider -d insider_detection \
  -c "SELECT COUNT(*) FROM wallets WHERE insider=true;"

# Comparar con lo que devuelve el dashboard
curl http://localhost:8002/api/overview | python -m json.tool | grep insider
```

Si hay discrepancia entre BD y dashboard, es un bug en las queries SQL de `dashboard/queries/`.

---

### Redis streams muestran pending alto

Si `XPENDING` de `closed_markets` o `insider_trades` crece continuamente, el orchestrator o los workers no están consumiendo:

```bash
# Ver estado del orchestrator
docker-compose logs orchestrator | tail -20

# Ver workers
docker-compose ps worker

# Ver pending directamente en Redis
docker exec cfg-data-redis-1 redis-cli XPENDING closed_markets scheduler
```

---

### Celery muestra 0 workers

```bash
# ¿Los workers están corriendo?
docker-compose ps worker

# ¿Están conectados a Redis?
docker-compose logs worker | grep "Connected to"

# El inspect de Celery tiene timeout de 2s. Si hay latencia alta puede fallar.
# Aumentar timeout en dashboard/redis_client.py:
#   app.control.inspect(timeout=5.0)
```

---

### Gráfico de categorías no aparece (Overview)

Chart.js solo renderiza si `top_categories` tiene al menos 1 elemento. Si no hay wallets con `insider=true`, el gráfico no se dibuja. Verificar:

```bash
curl http://localhost:8002/api/overview | python -m json.tool | grep -A10 top_categories
```

---

### Error en consola: `TypeError: this.$nextTick is not a function`

Síntoma (visible en la consola del browser, normalmente en Overview):
```
(índice):298 TypeError: this.$nextTick is not a function
    at load ((índice):297:14)
```

**Causa:** pasar la referencia **desnuda** de un método del componente a `setInterval`:
```html
<!-- ❌ MAL -->
<div x-data="overviewPage()" x-init="load(); setInterval(load, 15000)">
```
La primera llamada `load()` corre con el `this` ligado al proxy reactivo de Alpine, pero `setInterval(load, ...)` guarda la función **sin** ese binding. Cuando el intervalo dispara, `this` ya no es el componente Alpine, así que cualquier *magic* (`this.$nextTick`, `this.$refs`, …) o propiedad (`this.d`, `this.ts`) revienta.

**Solución:** envolver la llamada en una arrow function para preservar el contexto:
```html
<!-- ✅ BIEN -->
<div x-data="overviewPage()" x-init="load(); setInterval(() => load(), 15000)">
```

Aplica a cualquier template que use `setInterval` con auto-refresh (`overview.html`, `pipeline.html`). Regla general: **nunca pasar `metodoDelComponente` directamente a `setInterval`/`setTimeout`; siempre `() => metodoDelComponente()`.**

---

### Añadir una nueva página al dashboard

1. Crear la query en `dashboard/queries/nueva_queries.py`
2. Crear el router en `dashboard/routers/nueva.py`
3. Registrar el router en `dashboard/main.py`: `app.include_router(nueva.router)`
4. Añadir la ruta de página HTML en `dashboard/main.py`
5. Crear `dashboard/templates/nueva.html` extendiendo `base.html`
6. Añadir el ítem al sidebar en `dashboard/templates/base.html`
7. Reconstruir: `docker-compose up -d --build dashboard`

---

### Variables de entorno del dashboard

El dashboard lee estas variables (configuradas en `docker-compose.yml`):

| Variable | Por defecto (local) | Valor en Docker |
|----------|--------------------|----|
| `DATABASE_URL` | `postgresql://insider:insider_pass@localhost:5433/insider_detection` | `postgresql://insider:insider_pass@postgres:5432/insider_detection` |
| `REDIS_URL` | `redis://localhost:6379/0` | `redis://redis:6379/0` |
| `INGESTION_STATS_URL` | `http://localhost:9001` | `http://ingestion:9001` |

Para ejecutar el dashboard localmente sin Docker (útil para desarrollo de templates):
```bash
# Requiere que postgres y redis estén accesibles en localhost
DATABASE_URL=postgresql://insider:insider_pass@localhost:5433/insider_detection \
REDIS_URL=redis://localhost:6379/0 \
INGESTION_STATS_URL=http://localhost:9001 \
poetry run uvicorn dashboard.main:app --host 0.0.0.0 --port 8002 --reload
```

El flag `--reload` recarga automáticamente cuando se edita cualquier archivo Python o template.
