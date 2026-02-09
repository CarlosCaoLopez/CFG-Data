# Arquitectura del algoritmo de detección de insiders en mercados predictivos de CFG Data

## Distribución de capas

Vamos a construir un sistema organizado en 6 capas:

- Capa (A) Ingesta de Datos: Esta capa se encargará de leer los datos de las API de Polymarket y Polygon, según los criterios especificados, y enviarlos a la siguiente capa. Implementada en Python.
- Capa (B) Colas REDIS: Esta capa actuará como un intermediario que recibirá los datos de la capa (A) y los enviará a la capa (C) Base de Datos. Implementada en Python.
- Capa (C) Base de Datos: Esta capa almacenará los datos recibidos en una base de datos estructurada con varias tablas. Implementada en PostgreSQL.
- Capa (D) Motor de Cálculo: Esta capa se encargará de procesar los datos almacenados en la base de datos y aplicar el criterio de insider para identificar posibles insiders. Implementada en R.
- Capa (E) REST API: Esta capa se encargará de recibir las peticiones HTTP del motor de cálculo y actualizar la base de datos en consecuencia. Implementada en Python.
- Capa (T) Tasks: Esta capa se encargará de ejecutar tareas específicas para trasladar información entre las otras capas, como mover datos de REDIS a la base de datos, calcular variables al cierre de un market, etc. Implementada en Python, con Celery para la gestión de tareas asíncronas.

Vamos a definir globalmente un array de strings de CATEGORIAS_DE_INTERES.

## A. Ingesta de datos

Vamos a tener en primer lugar dos listeners en la capa (A) Ingesta de Datos. 

1. Un primer listener está leyendo siempre de las API de Polymarket y Polygon, según los criterios que ya están especificados en otro documento, y solo manda los datos al cierre de un market. En tiempo real, estará escuchando todos los markets con categories y tags que nos interesen (CATEGORIAS_DE_INTERES), que tenemos que decirle de antemano. Entonces este listener va a estar leyendo constantemente, pero solo va a mandar los datos al cierre de un market.
2. El segundo listener solo va a leer de las API la información de todos los trades de las wallets que reciban como entrada. La entrada la va a recibir mediante una task. La entrada irá acompañada de las categories en las que la wallet es insider, por lo que solo mandara los trades de esa wallet en esas categories. Este listener va a estar leyendo constantemente y mandando los datos que queremos constantemente.

## B. Colas REDIS

El contenedor de las colas REDIS va a recibir toda la información de los listeners de la capa (A) Ingesta de Datos y los va a mandar a la capa (T) Tasks. La información que va a mandar a la capa (C) Base de Datos va a ser de dos tipos:

1. Información de los markets cerrados, que va a ser enviada por el primer listener.
2. Información de los trades de las wallets, que va a ser enviada por el segundo listener

## C. Base de datos

Va a tener 4 tablas:

1. Tabla de Markets: guarda lo que se recibe a través del REDIS del listener de markets cerrados.
   - IDMarket (pk): hexadecimal
   - Categories: array de strings
   - Tags: array de strings
   - Question: string
   - EndDate: timestamp
   - OutcomePrizes: array de floats
   - Volume_usdc: float
   - Updated_at: timestamp
   - WinningOutcome: string

2. Tabla de Trades: guarda lo que se recibe a través del REDIS del listener de markets cerrados.
    - IDTrade (pk): hexadecimal
    - IDWallet: hexadecimal (fk)
    - IDMarket: hexadecimal (fk)
    - Side: string
    - Outcome: string
    - Prize: float
    - Size_usdc: float
    - Size_tokens: float
    - Timestamp: timestamp
    - Updated_at: timestamp
    - Last_trade: boolean si es o no el último trade de ese market, antes de que cierre

3. Tabla de Wallets: guarda lo que se recibe a través del REDIS del listener de markets cerrados.
    - IDWallet (pk): hexadecimal
    - First_seen_at: timestamp. Min(First_tx_polymarket, First_tx_polygon)
    - First_tx_polymarket: timestamp
    - First_tx_polygon: timestamp
    - Total_volume_usdc: float
    - Markets_traded: integer
    - Updated_at: timestamp
    - Insider: boolean (sí/no)
    - Categories_insider: array de strings (categorías en las que es insider)
    - MarketIDs_insider: array de hexadecimales (markets en los que es insider)
  
En la tabla de wallets habrá un index de insiders, que devuelve las filas en las que Insider = true, ordenadas por Updated_at de forma descendente, para que se pueda acceder rápidamente a los insiders más recientes.

4. Tabla de Insiders_Trades: guardará lo que se recibe a través del REDIS del listener de insiders.
    - IDInsider_Trade (pk): hexadecimal
    - IDWallet: hexadecimal (fk)
    - IDMarket: hexadecimal (fk)
    - Side: string
    - Outcome: string
    - Prize: float
    - Size_usdc: float
    - Size_tokens: float
    - Timestamp: timestamp
    - Updated_at: timestamp

5. Tabla temporal de Closed_Market_Variables: esta tabla se calcula por la Task 2 al cierre de un market mediante un script de SQL.
    - IDWallet (fk): hexadecimal
    - IDMarket (fk): hexadecimal
    - IDTrade (fk): hexadecimal
    - Δt: float
    - Age: float
    - Winning_volume: float
    - α_slippage: float
    - H (entropía): float

Las columnas de Updated_at de las tablas de Market y Wallets se van a actualizar cada vez que se actualice un registro de esa tabla, para tener un control de cuándo se actualizó por última vez cada registro.

## D. Motor de cálculo

Se encarga de procesar los datos de la capa (C) Base de Datos. La capa (T) Tasks manda de forma asíncrona los datos al motor de cálculo y se olvida. La información se puede recibir a través de un endpoint con la librería Plumber. La información que va a recibir tendrá el siguiente formato:
    - IDWallet: hexadecimal
    - IDMarket: hexadecimal
    - Δt: float
    - Age: float
    - Winning_volume: float
    - α_slippage: float
    - H (entropía): float

El motor de cálculo tendrá una función que se ejecuta cada vez que recibe datos nuevos y hará:

1. Ajuste de bondad de distribucion para cada columna (solo de vez en cuando).
2. Obtener región crítica para cada variable (Δt, Age, Winning_volume, α_slippage, H).
3. Ejecutar el **criterio de insider**. De momento, por ejemplo un criterio será que una wallet esté en las 5 region críticas de las 5 variables, pero esto se puede ir ajustando.
4. Devolver como resultado el conjunto IDMarket, IDWallet, Categories_insider, MarketIDs_insider. Cuando el motor termina, manda una petición HTTP al módulo (E) REST API. Si no hay ningún insider, el IDMarket, para que luego se puedan eliminar los trades por una task. Se pueden mandar por separado el IDMarket y luego el resto de datos, eso ya es cuestión de cómo se quiera implementar.

## E. REST API

La función de este módulo es recibir las peticiones HTTP del motor de cálculo y actualizar la tabla de Wallets de la capa (C) Base de Datos, poniendo Insider = true, actualizando las Categories_insider y MarketIDs_insider, y actualizando el Updated_at. Además, cada vez que se actualice un registro de la tabla de Wallets, se va a actualizar también el registro de la tabla de Markets correspondiente a cada market en el que esa wallet es insider, para actualizar su Updated_at.

También borrará la tabla de Closed_Market_Variables de la capa (C) Base de Datos, para eliminar los datos temporales que ya no necesitamos, una vez se reciba la llamada de (D) Motor de Cálculo de que se ha terminado de procesar un market.

## T. Tasks

Vamos a tener diversas Tasks de Celery que ejecutan las tareas de trasladar información entre los componentes anteriores:

1. Task 1 (T1): manda todo lo que llega en el REDIS a la (C) Base de Datos, ya que no solo tienen por qué ser markets cerrados, sino que también pueden ser trades de insiders. Mueve información de la capa (B) Colas REDIS a la capa (C) Base de Datos.
   
2. Task 2 (T2): al cierre de un market, llama a un script de SQL que calcula unas variables. Solo traba en (C) Base de Datos, no mueve información a otra capa. Estas variables son las que se almacenan en la tabla de Closed_Market_Variables. Para que la (D) Capa de Cálculo reciba el formato especificado de IDWallet, IDMarket, Δt, Age, Winning_volume, α_slippage, H, se obtendrán estas cuentas directamente en el SQL, ya que está optimizado para hacer estos cálculos sencillos. Estas cuentas son:
    - Δt = Market.EndDate - Trade.Timestamp. Se mide en horas.
    - Age = Máximo(1, Market.EndDate - Wallet.First_seen_at). Se mide en días.
    - Winning_volume = Suma(Trade.Size_usdc * Trade.Prize * (Trade.Outcome == Market.WinningOutcome)), donde (Trade.Outcome == Market.WinningOutcome) = 1 si el outcome del trade es el mismo que el winning outcome del market, y -1 en caso contrario. Se mide en USDC.
    - α_slippage = 0, si Trade.Prize = 0. Además, α_slippage = (Trade.Prize * (1 + Trade.Size_usdc / Market.Volume_usdc) - Trade.Prize) / Trade.Prize, si Trade.Prize > 0. Se mide en porcentaje, entre 0 y 1.
    - H (entropía) = - Suma(P * log(P)), siendo P = volumen de la wallet en una cierta categoría / volumen total de la wallet, para cada categoría en la que ha apostado la wallet.

3. Task 3 (T3): cada vez que se fije con Insider = true una wallet en la tabla de Wallets, o se añada una Categories_insider a una fila de un insider en esta misma tabla, manda la información necesaria al listener de insiders de la capa (A) Ingesta de Datos, para que este listener pueda empezar a leer los trades de esa wallet en las categories en las que es insider. Mueve información de la capa (C) Base de Datos a la capa (A) Ingesta de Datos.
   
4. Task 4 (T4): cuando la capa (E) recibe la petición HTTP del (D) Motor de Cálculo, significa que ya se ha terminado de procesar el market con IDMarket. Esta Task debe eliminar de la (C) Base de Datos todos los trades de la tabla de Trades asociados con el IDMarket que ya se ha procesado, no queremos guardar más esta información.

---

## Diagrama de Arquitectura

```
┌─────────────────┐     ┌─────────────────┐
│ Listener Markets│     │Listener Insiders│◄────────┐
└────────┬────────┘     └────────┬────────┘         │
         │                       │                  │
         └───────────┬───────────┘                  │
                     ▼                              │
              ┌─────────────┐                       │
              │    REDIS    │                       │
              └──────┬──────┘                       │
                     │                              │
                     ▼                              │
              ┌─────────────┐                       │
              │     T1      │                       │
              └──────┬──────┘                       │
                     │                              │
                     ▼                              │
              ┌─────────────┐                       │
┌─────────────│     BD      │◄──────┐               │
│             └──────┬──────┘       │               │
│                    │              │               │
│        ┌───────────│              │               │
│        ▼           ▼              │               │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐              │
│  │   T2    │ │   T3    │ │   T4    │              │
│  └─────────┘ └────┬────┘ └──────▲──┘              │
└───┐               │             │                 │
    │               └─────────────┼─────────────────┘
    ▼                             │
  ┌───────────────┐               │
  │Motor Cálculo  │               │
  └───────┬───────┘               │
          │                       │
          ▼                       │
┌───────────────┐                 │
│   REST API    │─────────────────┘
└───────────────┘
``` 





