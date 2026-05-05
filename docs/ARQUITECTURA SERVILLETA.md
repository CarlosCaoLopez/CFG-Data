# Arquitectura del algoritmo de detección de insiders en mercados predictivos de CFG Data

## Distribución de capas

Vamos a construir un sistema organizado en 6 capas + 1 capa de orquestación:

- Capa (A) Ingesta de Datos: Esta capa se encargará de leer los datos de las API de Polymarket y Polygon, según los criterios especificados, y enviarlos a la siguiente capa. Implementada en Python.
- Capa (B) Colas REDIS: Esta capa actuará como un intermediario que recibirá los datos de la capa (A) Ingesta de Datos y los expondrá a la capa (Q) Orchestrator. Implementada con Redis Streams.
- Capa (Q) Orchestrator: Esta capa lee de los streams de Redis, persiste los datos en la base de datos (función T1), y despacha la tarea T2 a la capa (T) Tasks. Es un servicio independiente (no Celery).
- Capa (C) Base de Datos: Esta capa almacenará los datos recibidos en una base de datos estructurada con varias tablas. Implementada en PostgreSQL.
- Capa (D) Motor de Cálculo: Esta capa se encargará de procesar los datos almacenados en la base de datos y aplicar el criterio de insider para identificar posibles insiders. Implementada en R con Plumber.
- Capa (E) REST API: Esta capa se encargará de recibir las peticiones HTTP del motor de cálculo y actualizar la base de datos en consecuencia. Implementada en Python (FastAPI).
- Capa (T) Tasks: Esta capa se encargará de ejecutar tareas específicas para trasladar información entre las otras capas, como calcular variables al cierre de un market, publicar comandos a Redis, y limpiar datos temporales. Implementada en Python con Celery.
- Capa (F) Dashboard: Panel de control web para visualizar markets, wallets insider, variables y el estado del pipeline. Implementada en Python (FastAPI) con templates HTML. Puerto 8002.

Vamos a definir globalmente un array de strings de CATEGORIAS_DE_INTERES.

## A. Ingesta de datos

Vamos a tener en primer lugar dos listeners en la capa (A) Ingesta de Datos.

1. **L1 — Listener Markets (MarketCloseListener):** está leyendo siempre de las API de Polymarket y Polygon, según los criterios especificados, y solo manda los datos al cierre de un market. Tiene 3 loops concurrentes:
   - `_ws_discovery_loop`: escucha eventos `OrderFilled` on-chain via WebSocket de Polygon RPC. Descubre markets activos y los enriquece con tags/category via Events API.
   - `_gamma_discovery_loop`: cada 15 min, llama a la Gamma API para descubrir markets abiertos (catch-all para markets sin trades recientes que el WebSocket no vería).
   - `_close_detection_loop`: cada 60s, comprueba si algún market tracked ha cerrado. Si cierra, recolecta todos los trades + wallet ages, normaliza y publica el batch completo al stream `closed_markets`.

2. **L2 — Listener Insiders (InsiderTradesListener):** solo va a leer de las APIs la información de todos los trades de las wallets que reciban como entrada. La entrada la recibe via stream Redis `insider_commands` (publicado por T3). La entrada va acompañada de las categories en las que la wallet es insider, por lo que solo mandará los trades de esa wallet en esas categories. Tiene 2 loops:
   - `_command_listener_loop`: lee comandos de `insider_commands` (ADD_WALLET, UPDATE_CATEGORIES, REMOVE_WALLET) via XREADGROUP.
   - `_trade_polling_loop`: cada 30s, por cada wallet monitorizada, consulta trades nuevos y publica al stream `insider_trades`.

Existe también un **BackfillRunner** (herramienta CLI separada) que procesa mercados históricos cerrados y publica batches al stream `raw_data`.

## B. Colas REDIS

Redis actúa como intermediario con 4 streams:

| Stream | Productor | Consumidor |
|--------|-----------|------------|
| `closed_markets` | L1 (market cerrado) | Orchestrator (Q) |
| `raw_data` | BackfillRunner | Orchestrator (Q) |
| `insider_trades` | L2 (trades de insider) | Orchestrator (Q) |
| `insider_commands` | T3 (nuevo insider) | L2 |

## Q. Orchestrator

El `QueueOrchestrator` es un **servicio independiente** (no Celery) que:

1. Lee de los streams `closed_markets`, `raw_data` e `insider_trades` mediante XREADGROUP.
2. Ejecuta la función **T1 inline** (persistencia a BD):
   - Para batches de mercado: `upsert_wallet` → `upsert_market` → `upsert_trade_batch` (orden FK).
   - Para insider trades: `upsert_insider_trade_batch`.
3. Tras persistir un batch de mercado, despacha la tarea Celery **T2** (`compute_variables`).
4. Hace ACK de los mensajes procesados.

## C. Base de datos

Va a tener 5 tablas más 1 tabla temporal:

1. **Tabla de Markets:** guarda lo que se recibe a través del Orchestrator del listener de markets cerrados.
   - `market_id` (pk): hexadecimal (conditionId)
   - `question`: string
   - `end_date`: timestamp
   - `closed`: boolean
   - `outcome_prices`: array de floats
   - `volume_usdc`: float
   - `categories`: array de strings
   - `tags`: array de strings
   - `winning_outcome`: string ('Yes' / 'No')
   - `updated_at`: timestamp

2. **Tabla de Trades:** guarda lo que se recibe a través del Orchestrator del listener de markets cerrados.
   - `id` (pk): hexadecimal (tx_hash)
   - `wallet_id`: hexadecimal (fk → wallets.address)
   - `market_id`: hexadecimal (fk → markets.market_id)
   - `side`: string (BUY/SELL)
   - `outcome`: string (Yes/No)
   - `prize`: float (precio 0.0–1.0)
   - `size_usdc`: float
   - `size_tokens`: float
   - `timestamp`: timestamp
   - `last_trade`: boolean — True si es el último trade de ese market antes del cierre
   - `updated_at`: timestamp

3. **Tabla de Wallets:** guarda lo que se recibe a través del Orchestrator del listener de markets cerrados.
   - `address` (pk): hexadecimal
   - `first_seen_at`: timestamp — Min(first_tx_polymarket, first_tx_polygon)
   - `first_tx_polymarket`: timestamp
   - `first_tx_polygon`: timestamp
   - `total_volume_usdc`: float
   - `markets_traded`: integer
   - `insider`: boolean (sí/no)
   - `categories_insider`: array de strings (categorías en las que es insider)
   - `marketids_insider`: array de hexadecimales (markets en los que es insider)
   - `updated_at`: timestamp

   En la tabla de wallets habrá un index de insiders (`idx_wallets_insider`) que devuelve las filas en las que `insider = true`, ordenadas por `updated_at` de forma descendente, para acceder rápidamente a los insiders más recientes.

4. **Tabla de Insiders_Trades:** guardará lo que se recibe a través del Orchestrator del listener de insiders.
   - `id` (pk): hexadecimal
   - `wallet_id`: hexadecimal (fk → wallets.address)
   - `market_id`: hexadecimal (fk → markets.market_id)
   - `side`: string (BUY/SELL)
   - `outcome`: string (Yes/No)
   - `prize`: float
   - `size_usdc`: float
   - `size_tokens`: float
   - `timestamp`: timestamp
   - `last_trade`: boolean (default FALSE)
   - `updated_at`: timestamp

5. **Tabla temporal de Closed_Market_Variables:** esta tabla se calcula por la Task T2 al cierre de un market mediante un script SQL. Se elimina una vez el Motor D ha procesado el market.
   - `wallet_id` (pk, fk → wallets.address): hexadecimal
   - `market_id` (pk, fk → markets.market_id): hexadecimal
   - **PK compuesta: (wallet_id, market_id). No hay columna IDTrade.**
   - `delta_t`: float — tiempo en horas
   - `age`: float — edad en días
   - `winning_volume`: float
   - `alpha_slippage`: float
   - `entropy`: float

Las columnas `updated_at` de Markets y Wallets se actualizan cada vez que se modifica un registro.

## D. Motor de cálculo

Se encarga de procesar los datos de la capa (C) Base de Datos. La tarea T2 dispara una petición HTTP `POST /analyze` al motor (endpoint Plumber en R) con únicamente `{"market_id": "0x..."}`. El motor **lee directamente de la BD** la tabla `closed_market_variables` filtrada por ese `market_id`; no recibe las variables como parámetros HTTP.

El motor de cálculo hace:

1. **Percentiles empíricos** para obtener umbrales (no ajuste paramétrico de distribuciones):
   - `delta_t`: percentil 1% (trades más cercanos al cierre)
   - `age`: percentil 1% (wallets más jóvenes)
   - `winning_volume`: percentil 99% (mayor volumen ganador)
   - `alpha_slippage`: percentil 99% (mayor slippage)
   - `entropy`: percentil 1% — **actualmente DESHABILITADO** (`ENTROPY_ENABLED = FALSE`). Se activará cuando haya datos de múltiples categorías suficientes.

2. **Criterio de insider:** de momento, una wallet está en las **4 regiones críticas** simultáneamente (delta_t, age, winning_volume, alpha_slippage). Cuando se habilite la entropía serán 5.

3. **Resultado:** si hay insiders, hace `POST /insider-detection` a la capa (E) REST API con el formato:
   ```json
   {
     "market_id": "0x...",
     "wallets": [
       {"wallet": "0x...", "categories_insider": [...], "command_type": "ADD_WALLET"}
     ]
   }
   ```
   Si no hay insiders, el endpoint E no es llamado; la limpieza de la tabla CMV la realizará T4 periódico.

## E. REST API

La función de este módulo es recibir las peticiones HTTP del motor de cálculo y:

1. Actualizar la tabla de Wallets: `insider = true`, actualizar `categories_insider` y `marketids_insider`.
2. Disparar la tarea **T3** por cada wallet insider detectada.
3. Borrar las filas de `closed_market_variables` para ese `market_id` (limpieza inline, síncrona).
4. Disparar la tarea **T4 reactivo** (`cleanup_market`) para eliminar los trades de ese `market_id`.

## T. Tasks

Diversas Tasks de Celery que ejecutan las tareas de trasladar información entre los componentes anteriores:

1. **T1 — inline en Orchestrator (Q):** no es una tarea Celery. El Orchestrator lo ejecuta directamente al leer de Redis: persiste wallets, markets, trades e insider_trades en la BD (en ese orden de FK). Tras persistir un batch de mercado, despacha T2.

2. **Task T2 (`compute_variables`):** al cierre de un market, llama al script SQL `compute_variables.sql` que calcula las variables y las inserta en `closed_market_variables`. Luego despacha la tarea D (`run_market_inference`). Solo trabaja en (C) Base de Datos. Las fórmulas son:

   - **Δt** = `MIN(Market.EndDate - Trade.Timestamp)` para cada wallet — se toma el trade más cercano al cierre. Se mide en **horas**.

   - **Age** = `MAX(1, Market.EndDate - Wallet.first_seen_at)`. Se mide en **días**.

   - **Winning_volume** = `SUM(Trade.Size_usdc × Trade.Prize × side_sign × outcome_sign)` donde:
     - `side_sign` = +1 si BUY, -1 si SELL
     - `outcome_sign` = +1 si `Trade.Outcome == Market.WinningOutcome`, -1 si no
     - Se mide en **USDC**.

   - **α_slippage** = `SUM(size_usdc WHERE prize > 0) / Market.Volume_usdc` — simplificación algebraica de `(Prize × (1 + Size_usdc / Volume) - Prize) / Prize`. 0 si `Market.Volume_usdc = 0`. Se mide entre 0 y 1.

   - **H (entropía)** = `-SUM(P × ln(P))` donde `P = volumen de la wallet en una categoría / volumen total de la wallet`, para cada categoría en la que ha apostado. Calculado sobre todos los trades históricos de la wallet (no solo los del market actual).

3. **Task T3 (`publish_insider_command`):** cada vez que la capa (E) detecta un insider, publica el comando necesario al stream `insider_commands` para que L2 empiece a monitorizar esa wallet. Mueve información de (E) a la capa (A) Ingesta de Datos.

4. **Task T4 (`cleanup_market`) — reactivo:** cuando (E) recibe la petición HTTP de (D), despacha esta tarea para eliminar de (C) todos los trades de la tabla `trades` asociados al `market_id` procesado (y sus filas CMV si quedan). Orden de borrado: CMV primero (FK), luego trades.

5. **Task T4 (`cleanup_stale_trades`) — periódico:** tarea Celery Beat que se ejecuta **cada hora**. Elimina de la tabla `trades` todos los registros con `updated_at < NOW() - 1h` y sus `closed_market_variables` asociadas. Para mercados que no llegaron a ser procesados por D o cuyo T4 reactivo no fue disparado.

---

## Diagrama de Arquitectura

```
┌─────────────────┐     ┌─────────────────┐
│   L1 Markets    │     │  L2 Insiders    │◄──────────────────────┐
│ (3 loops: WS,   │     │ (2 loops: cmds  │                       │
│  Gamma, close)  │     │  + polling)     │                       │
└────────┬────────┘     └────────┬────────┘                       │
         │ closed_markets        │ insider_trades                  │
         │ raw_data (backfill)   │                                 │
         └───────────┬───────────┘                                 │
                     ▼                                             │
              ┌─────────────┐   4 streams:                         │
              │    REDIS    │   closed_markets                     │
              │    (B)      │   raw_data                           │
              │             │   insider_trades                     │
              │             │   insider_commands ─────────────────►│
              └──────┬──────┘                                       │
                     │                                             │
                     ▼                                             │
              ┌─────────────┐                                       │
              │ Orchestrator│  T1 inline                           │
              │    (Q)      │  (upsert wallet/market/trades)       │
              └──────┬──────┘                                       │
                     │ dispatch T2                                 │
                     ▼                                             │
              ┌─────────────┐                                       │
┌─────────────│     BD      │◄──────┐                              │
│             │     (C)     │       │                              │
│             └──────┬──────┘       │                              │
│                    │              │                              │
│        ┌───────────┘              │                              │
│        ▼                          │                              │
│  ┌─────────┐                      │                              │
│  │   T2    │ compute_variables    │                              │
│  │(Celery) │ SQL + dispatch D     │                              │
└──►         │                      │                              │
   └────┬────┘                      │                              │
        │ POST /analyze             │                              │
        ▼                           │                              │
  ┌───────────────┐                 │                              │
  │ Motor Cálculo │                 │                              │
  │    (D / R)    │                 │                              │
  │ Percentiles   │                 │                              │
  │ empíricos     │                 │                              │
  └───────┬───────┘                 │                              │
          │ POST /insider-detection │                              │
          ▼                         │                              │
  ┌───────────────┐                 │                              │
  │   REST API    │─────────────────┘                              │
  │     (E)       │ upsert wallets, delete CMV,                    │
  │               │ dispatch T3 ──────────────────────────────────►│
  └───────┬───────┘ dispatch T4 reactivo                           │
          │                                                        │
          ▼
  ┌───────────────┐
  │  Dashboard    │
  │    (F)        │
  │  puerto 8002  │
  └───────────────┘

T4 periódico (Celery Beat, cada hora):
  cleanup_stale_trades() → DELETE trades + CMV con updated_at < NOW()-1h
```
