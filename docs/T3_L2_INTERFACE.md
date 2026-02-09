# Interfaz T3 → L2: Comandos de Insider

> Documento para Persona C. Describe cómo T3 debe publicar comandos al stream `insider_commands` para que el Listener L2 (Ingesta) monitoree trades de wallets insider.

---

## Resumen del Flujo

```
(D) Motor de Cálculo
        │
        │  HTTP → wallet es insider
        ▼
(E) REST API
        │
        │  UPDATE wallets SET insider=true, categories_insider=[...]
        ▼
(C) Base de Datos
        │
        │  Trigger / evento: nueva wallet insider o nuevas categories
        ▼
(T) Task T3
        │
        │  XADD insider_commands * payload '{...}'
        ▼
(A) Listener L2  ──→  poll trades de esa wallet  ──→  XADD insider_trades
```

**T3 publica un comando → L2 lo consume y empieza a monitorear los trades de esa wallet en las categorías indicadas.**

---

## Stream Redis

| Parámetro | Valor |
|-----------|-------|
| **Stream** | `insider_commands` |
| **Consumer group** | `ingestion_l2` (lo crea L2 automáticamente) |
| **Consumer** | `l2_worker_1` |
| **Lectura** | `XREADGROUP` con block 5000ms |

L2 consume con `XREADGROUP GROUP ingestion_l2 l2_worker_1 COUNT 10 BLOCK 5000 STREAMS insider_commands >`.

Tras procesar cada mensaje, L2 hace `XACK insider_commands ingestion_l2 <msg_id>`.

---

## Contrato: `InsiderTradeCommand`

Definido en `contracts/raw_market_data.py`:

```python
class InsiderTradeCommand(BaseModel):
    wallet: str                          # Dirección de la wallet (0x...)
    categories_insider: list[str]        # Categorías en las que es insider
    market_ids_insider: list[str] | None # (Opcional) Markets específicos donde se detectó
    command_type: str                    # "ADD_WALLET" | "UPDATE_CATEGORIES" | "REMOVE_WALLET"
```

---

## Formato del Mensaje en Redis

T3 debe hacer `XADD insider_commands *` con **una** de estas dos opciones:

### Opción A: Campo `payload` con JSON completo (recomendada)

```
XADD insider_commands * payload '{"wallet":"0xabc123...","categories_insider":["Sports","Crypto"],"command_type":"ADD_WALLET"}'
```

El campo `payload` contiene el JSON serializado del `InsiderTradeCommand`. L2 lo deserializa con `InsiderTradeCommand.model_validate_json(payload)`.

### Opción B: Campos individuales

```
XADD insider_commands * wallet "0xabc123..." categories_insider '["Sports","Crypto"]' command_type "ADD_WALLET"
```

L2 también acepta campos separados. `categories_insider` debe ser un JSON array serializado como string.

**Recomendación:** Usar Opción A (`payload`) para consistencia y para incluir `market_ids_insider` cuando aplique.

---

## Comandos Disponibles

### 1. `ADD_WALLET` — Nueva wallet insider

Se envía cuando el Motor de Cálculo (D) determina que una wallet es insider por primera vez.

```json
{
  "wallet": "0x9876fedcba543210...",
  "categories_insider": ["Sports", "Crypto"],
  "market_ids_insider": ["0xmarket_donde_se_detecto"],
  "command_type": "ADD_WALLET"
}
```

**Efecto en L2:**
- Añade la wallet al mapa `_monitored_wallets`
- Cada 30 segundos, L2 consulta la Data API de Polymarket por trades recientes de esta wallet
- Solo publica los trades cuyo market pertenezca a alguna de las `categories_insider` indicadas

### 2. `UPDATE_CATEGORIES` — Añadir categorías a un insider existente

Se envía cuando una wallet ya marcada como insider es detectada en una categoría nueva.

```json
{
  "wallet": "0x9876fedcba543210...",
  "categories_insider": ["Sports", "Crypto", "Pop Culture"],
  "market_ids_insider": ["0xmarket1", "0xmarket2"],
  "command_type": "UPDATE_CATEGORIES"
}
```

**Efecto en L2:**
- Reemplaza las `categories_insider` de esa wallet por las nuevas
- Si la wallet no existe en L2 (por reinicio), la trata como `ADD_WALLET`

**Importante:** Enviar la lista **completa** de categorías, no solo la nueva. L2 reemplaza, no hace append.

### 3. `REMOVE_WALLET` — Dejar de monitorear

Se envía si se reclasifica una wallet como no-insider (opcional).

```json
{
  "wallet": "0x9876fedcba543210...",
  "categories_insider": [],
  "command_type": "REMOVE_WALLET"
}
```

**Efecto en L2:**
- Elimina la wallet del mapa `_monitored_wallets`
- Deja de hacer polling de sus trades

---

## Ejemplo con Python (redis-py)

```python
import redis
import json

r = redis.from_url("redis://localhost:6379/0")

# ADD_WALLET
command = {
    "wallet": "0x9876fedcba543210abcdef1234567890abcdef12",
    "categories_insider": ["Sports", "Crypto"],
    "market_ids_insider": ["0xa4b4b2502bd548401bf97aa5eb3357af9d49d8f8"],
    "command_type": "ADD_WALLET",
}

r.xadd("insider_commands", {
    "payload": json.dumps(command),
})

# UPDATE_CATEGORIES (añadir Pop Culture)
r.xadd("insider_commands", {
    "payload": json.dumps({
        "wallet": "0x9876fedcba543210abcdef1234567890abcdef12",
        "categories_insider": ["Sports", "Crypto", "Pop Culture"],
        "command_type": "UPDATE_CATEGORIES",
    }),
})

# REMOVE_WALLET
r.xadd("insider_commands", {
    "payload": json.dumps({
        "wallet": "0x9876fedcba543210abcdef1234567890abcdef12",
        "categories_insider": [],
        "command_type": "REMOVE_WALLET",
    }),
})
```

---

## Ejemplo con redis-cli

```bash
# ADD_WALLET
XADD insider_commands * payload '{"wallet":"0x9876fedcba543210abcdef1234567890abcdef12","categories_insider":["Sports","Crypto"],"command_type":"ADD_WALLET"}'

# Verificar que está en el stream
XLEN insider_commands
XRANGE insider_commands - + COUNT 5

# Verificar que L2 lo consumió (si L2 está corriendo)
XINFO GROUPS insider_commands
# → pending = 0 si L2 ya lo procesó y envió XACK
```

---

## Qué Hace L2 Después de Recibir un Comando

1. **Registra la wallet** en memoria (mapa `_monitored_wallets`)
2. **Cada 30 segundos** (configurable: `L2_POLL_INTERVAL_SECONDS`):
   - Para cada wallet monitoreada, consulta `GET https://data-api.polymarket.com/activity?user={wallet}&type=TRADE`
   - Filtra trades nuevos (timestamp > último trade visto)
   - Para cada trade, obtiene la metadata del market via Gamma API
   - Comprueba si el market pertenece a alguna de las `categories_insider` de la wallet
   - Si match → normaliza y publica a `insider_trades`
3. **Publica a `insider_trades`** stream (formato `InsiderTradeData`):

```json
{
  "payload": "{...InsiderTradeData JSON...}",
  "wallet": "0x9876...",
  "trades_count": "3",
  "categories": "[\"Sports\",\"Crypto\"]",
  "published_at": "2026-02-09T15:30:05.789012"
}
```

---

## Categorías Válidas

L2 compara las `categories_insider` del comando contra la category y tags del market en Gamma API. La comparación es **case-insensitive**.

Categorías configuradas actualmente en `CATEGORIAS_DE_INTERES`:

| Categoría |
|-----------|
| `Sports` |
| `Crypto` |
| `US-current-affairs` |
| `Pop Culture` |

Los tags del market (ej: `NBA`, `Bitcoin`, `NFL`) también se incluyen en la comparación. Si `categories_insider` contiene `NBA` y el market tiene un tag `NBA`, es match.

---

## Notas para Implementación de T3

1. **Cuándo enviar `ADD_WALLET`:** Cada vez que `(E) REST API` actualice la tabla Wallets con `insider=true` y se añadan `categories_insider` nuevas.

2. **Wallet address:** Puede ser en cualquier case (L2 lo normaliza a lowercase internamente). Preferiblemente enviar en lowercase con prefijo `0x`.

3. **Idempotencia:** Enviar `ADD_WALLET` dos veces para la misma wallet no causa problemas — L2 simplemente sobrescribe la entrada.

4. **Persistencia:** L2 mantiene las wallets **solo en memoria**. Si el servicio de ingestion se reinicia, pierde la lista. T3 debería re-enviar los comandos de todas las wallets insider activas tras un reinicio de L2. (Alternativa: L2 podría leer de BD al arrancar, pero eso acopla A con C, así que por ahora T3 las re-envía).

5. **market_ids_insider** es opcional. L2 no lo usa actualmente para filtrado, solo lo almacena para referencia. El filtrado se basa exclusivamente en `categories_insider` vs category/tags del market.

6. **Sin respuesta:** T3 publica fire-and-forget. No hay stream de respuesta de L2 → T3. T3 puede verificar que L2 está consumiendo con `XINFO GROUPS insider_commands` (campo `pending`).
