# API Insider Detection - Guia de integracion para el Motor de Calculo

## Endpoint

```
POST /insider-detection
Content-Type: application/json
```

La API no tiene autenticacion por ahora. Se ejecuta en el puerto **8000** (Docker) o el que se configure localmente.

---

## Request

Se envia **un request por mercado** con todas las wallets detectadas como insiders en ese mercado.

```json
{
  "market_id": "0x<conditionId del mercado analizado>",
  "wallets": [
    {
      "wallet": "0x<direccion de la wallet>",
      "categories_insider": ["Crypto", "Politics"],
      "command_type": "ADD_WALLET"
    }
  ]
}
```

### Campos

| Campo | Tipo | Requerido | Descripcion |
|-------|------|-----------|-------------|
| `market_id` | `string` | Si | conditionId del mercado que se analizo (max 66 chars) |
| `wallets` | `array` | Si | Lista de wallets insider detectadas. Minimo 1 elemento |
| `wallets[].wallet` | `string` | Si | Direccion de la wallet (lowercase, 0x-prefixed, 42 chars) |
| `wallets[].categories_insider` | `string[]` | Si | Categorias donde se detecto comportamiento insider |
| `wallets[].command_type` | `string` | No | Tipo de comando. Default: `"ADD_WALLET"` |

### Valores de `command_type`

| Valor | Cuando usarlo |
|-------|---------------|
| `ADD_WALLET` | Primera vez que se detecta esta wallet como insider (default) |
| `UPDATE_CATEGORIES` | La wallet ya era insider pero se detectaron nuevas categorias |
| `REMOVE_WALLET` | Se determina que la wallet ya no es insider |

> Las **categories** deben coincidir con las categorias de los mercados de Polymarket (campo `categories` de la tabla `markets`). Ejemplos: `"Crypto"`, `"Politics"`, `"Sports"`, `"Finance"`, `"Pop Culture"`.

---

## Response

```json
{
  "status": "accepted",
  "market_id": "0x...",
  "wallets_updated": 3,
  "tasks_dispatched": 3,
  "errors": null
}
```

### Campos de la respuesta

| Campo | Tipo | Descripcion |
|-------|------|-------------|
| `status` | `string` | `"accepted"`, `"partial_failure"`, o `"error"` |
| `market_id` | `string` | Echo del market_id enviado |
| `wallets_updated` | `int` | Numero de wallets actualizadas en la BD |
| `tasks_dispatched` | `int` | Numero de comandos enviados al stream Redis |
| `errors` | `string[] \| null` | Detalle de errores si los hubo, `null` si todo fue bien |

### Codigos de status

| Status | Significado |
|--------|-------------|
| `accepted` | Todas las wallets se procesaron correctamente |
| `partial_failure` | Algunas wallets fallaron, otras se procesaron. Ver `errors` |
| `error` | Ninguna wallet se pudo procesar |

### Codigos HTTP

| Codigo | Cuando |
|--------|--------|
| `200` | Request procesado (ver campo `status` para resultado) |
| `422` | Request invalido: falta `market_id`, `wallets` vacia, `command_type` invalido |

---

## Ejemplos

### Caso basico: 1 insider detectado

```bash
curl -X POST http://localhost:8000/insider-detection \
  -H "Content-Type: application/json" \
  -d '{
    "market_id": "0xabc123...",
    "wallets": [
      {
        "wallet": "0x1234567890abcdef1234567890abcdef12345678",
        "categories_insider": ["Crypto"]
      }
    ]
  }'
```

Respuesta:
```json
{
  "status": "accepted",
  "market_id": "0xabc123...",
  "wallets_updated": 1,
  "tasks_dispatched": 1,
  "errors": null
}
```

### Caso tipico: multiples insiders en un mercado

```bash
curl -X POST http://localhost:8000/insider-detection \
  -H "Content-Type: application/json" \
  -d '{
    "market_id": "0xdef456...",
    "wallets": [
      {
        "wallet": "0xaaaa...aaaa",
        "categories_insider": ["Crypto", "Finance"],
        "command_type": "ADD_WALLET"
      },
      {
        "wallet": "0xbbbb...bbbb",
        "categories_insider": ["Crypto"],
        "command_type": "ADD_WALLET"
      },
      {
        "wallet": "0xcccc...cccc",
        "categories_insider": ["Finance"],
        "command_type": "UPDATE_CATEGORIES"
      }
    ]
  }'
```

### Caso: wallet ya conocida, nueva categoria

Si la wallet `0xaaaa...` ya fue detectada como insider en la categoria `"Crypto"` y ahora se detecta tambien en `"Politics"`, se envia:

```json
{
  "market_id": "0xnuevo_market...",
  "wallets": [
    {
      "wallet": "0xaaaa...aaaa",
      "categories_insider": ["Politics"],
      "command_type": "UPDATE_CATEGORIES"
    }
  ]
}
```

La API **acumula** las categorias y market IDs automaticamente. Despues de esta llamada, la wallet tendra `categories_insider = ["Crypto", "Politics"]` (sin duplicados).

---

## Comportamiento interno (para referencia)

Por cada wallet en el batch, la API hace dos cosas:

1. **Actualiza la BD** (`wallets` table):
   - `insider = true`
   - `categories_insider` se acumulan (no se sobreescriben)
   - `marketids_insider` se acumulan (no se sobreescriben)

2. **Publica un comando** al stream Redis `insider_commands` via Celery task T3, con el formato:
   ```json
   {"wallet": "0x...", "categories_insider": [...], "market_ids_insider": [...], "command_type": "ADD_WALLET"}
   ```
   Este comando sera consumido por el listener L2 para comenzar a monitorear los trades de esa wallet.

---

## Errores comunes

| Error | Causa | Solucion |
|-------|-------|----------|
| `422 Unprocessable Entity` | JSON invalido o campos faltantes | Verificar que `market_id` y `wallets` estan presentes, y que `wallets` no esta vacio |
| `status: "error"` con `wallets_updated: 0` | Fallo de BD para todas las wallets | Verificar que la BD esta accesible |
| `status: "partial_failure"` | Algunas wallets fallaron | Revisar el array `errors` para detalles. Se puede reintentar solo las wallets fallidas |
