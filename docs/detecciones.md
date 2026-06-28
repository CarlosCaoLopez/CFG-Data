# Detecciones y fixes — Incidencia "3 días sin guardar mercados"

Resumen de la incidencia del 28-06-2026 y los cambios aplicados. La ingesta en
tiempo real (L1) llevaba **3 días sin guardar mercados en BD** pese a tener
todos los contenedores `Up`.

---

## Diagnóstico

No era memoria: 0 reinicios, `OOMKilled=false`, sin OOM-killer en `dmesg`, disco
al 36%. El pipeline estaba vivo pero **cortado lógicamente**.

Bisección del flujo `L1 → closed_markets → orchestrator → BD`:

- `XINFO STREAM closed_markets` → **`entries-added = 0`**: L1 nunca publicó un
  cierre en 3 días.
- `/stats` de L1 → 668 mercados trackeados, `markets_closed_published = 0`.
- Logs → `market_close_detected = 0` y el síntoma clave:
  `market_not_found` para mercados ya resueltos.

**Causa raíz:** la detección de cierre usaba `fetch_market_by_id()`
(`?condition_ids=X`), que por defecto filtra `closed=false`. Cuando un mercado
resuelve, esa query devuelve `[]` o una fila vieja con `closed=false`, así que
`market.get("closed")` nunca era `True` → el cierre **no se detectaba jamás**.
Lo que había llenado la BD era el `backfill` (one-shot del 24-06), no el tiempo
real.

### Comandos de diagnóstico útiles

```bash
# Estado de contenedores / OOM / disco
docker inspect --format '{{.Name}} restarts={{.RestartCount}} oom={{.State.OOMKilled}}' $(docker ps -aq)
dmesg -T | grep -i oom ; df -h ; df -i

# Bisección del pipeline
docker exec ...-redis-1 redis-cli XINFO STREAM closed_markets   # entries-added
docker exec ...-ingestion-1 python -c "import urllib.request;print(urllib.request.urlopen('http://localhost:9001/stats').read().decode())"
docker compose logs --since 72h ingestion | grep -E 'market_close_detected|market_close_published'
docker compose logs --since 10m orchestrator | grep -c t1_persisted
```

---

## Fixes aplicados

### 1. Detección de cierre con `closed=true` (causa raíz)

`MarketCloseListener._check_for_closures` ahora detecta el cierre con la query
*closed-aware* (`?condition_ids=X&closed=true`). Presencia de fila = cerrado;
`None` = sigue abierto. Es la misma trampa documentada del `closedTime`, pero
aplicada a la **detección**, no solo a capturar la fecha real de cierre.

### 2. Detección por lotes (rendimiento)

Nuevo `MarketsCollector.fetch_closed_markets_by_ids()`: agrupa los
`condition_ids` en chunks de 20 y consulta `?condition_ids=…&closed=true` en una
sola petición por chunk (Gamma acepta varios ids; el filtro corre en servidor y
solo devuelve los cerrados). El barrido pasa de ~668 peticiones (>6 min bajo el
rate limit) a ~34 (~20s). No se paraleliza porque el límite es de
peticiones/min, no de concurrencia: bajar el nº de peticiones es lo que ataca la
causa.

### 3. Logging DEBUG → INFO

Ingestion no configuraba structlog y volcaba todo (los `http_request`/
`http_response` en DEBUG generaban ~331k líneas/24h, desgastando la SD). Nueva
`_configure_logging()` en `ingestion/main.py` fija el nivel desde `LOG_LEVEL`
(default INFO; `LOG_LEVEL=DEBUG` para depurar). Añadido `LOG_LEVEL=INFO` al
servicio `ingestion` en `docker-compose.yml`.

### 4. Caché de edades de wallets (Etherscan)

Tras el fix anterior salió a la luz el siguiente cuello: procesar un mercado
pesado tardaba **~35 min** consultando la edad de 930 wallets contra Etherscan
(tier gratis = 3 req/s, con `rate_limit_exhausted` repetido).

`WalletAgeCollector` ahora tiene caché de dos niveles:
- **L1** dict en memoria (rápido, se pierde al reiniciar).
- **L2** Redis con TTL de 30 días (sobrevive reinicios). `fetch_wallet_ages_batch`
  precarga con un único `MGET` y solo llama a Etherscan por las wallets nuevas.

Además se corrige un **envenenamiento de caché**: antes se cacheaban resultados
`None` (un fallo por rate limit quedaba como "sin edad" permanente). Ahora solo
se cachean lookups con `first_tx_polygon` válido; un trader de Polymarket
siempre tiene actividad en Polygon, así que un `None` es un fallo transitorio y
se reintenta. Log `wallet_age_cache_stats cached=X to_fetch=Y` para observar el
ratio.

> No se sube el rate limit a 5 req/s: el tier gratis de Etherscan V2 son 3 req/s
> (documentado en `config.py`). El caché es el único camino gratis que reduce
> llamadas de raíz.

---

## Aclaración: el backfill NO corre indefinidamente

`ingestion/backfill_runner.py` es un job **one-shot**: recorre un conjunto
finito (ventana `--days` + cutoff) y sale (`exit 0`). No es un demonio. Por eso
el hueco de 3 días: el backfill terminó su trabajo el 24-06 **y** L1 estaba roto
→ nada alimentaba la BD. El flujo continuo es responsabilidad de L1; el backfill
solo sirve para carga inicial o tapar huecos puntuales:

```bash
docker compose run --rm backfill python -m ingestion.backfill_runner --days=5
```

Si se quiere una red de seguridad periódica, lo correcto es una **reconciliación
programada y acotada** (p.ej. Celery Beat diario con `--days=2`), no un backfill
perpetuo.

---

## Mejoras pendientes (opcionales)

- **WebSocket on-chain descubre 0** (`markets_discovered_ws: 0`): todo el
  descubrimiento viene de Gamma. Funcional, pero se pierde la detección
  instantánea. Pendiente de revisar.
- **Calentar la caché de wallets desde la BD** al arrancar (la tabla `wallets`
  ya guarda las edades de forma permanente), para que el primer mercado tras un
  deploy no pague el coste completo.
- **No bloquear la publicación con el wallet-age**: hoy `_handle_market_close`
  espera a resolver todas las edades antes de publicar; un mercado pesado
  bloquea el loop de detección.
