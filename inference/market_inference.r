library(DBI)
library(RPostgres)
library(dplyr)
library(fitdistrplus)
library(MASS) 
library(httr)      # Necesario para el Webhook
library(jsonlite)  # Para formatear el JSON correctamente

# --- 1. CONEXIÓN Y CARGA DE DATOS ---
con <- dbConnect(RPostgres::Postgres(), 
                 dbname = "insider_detection", 
                 host = "localhost", 
                 port = 5432, 
                 user = "insider", 
                 password = "insider_pass")

df <- dbGetQuery(con, "SELECT * FROM closed_market_variables")
market_id_val <- df$market_id[1]  # Primer market_id del df
categories <- dbGetQuery(con, paste0(
  "SELECT categories FROM markets WHERE market_id = '", market_id_val, "'"
))dbDisconnect(con)

# Constante para evitar ceros
epsilon <- 1e-6

# --- 2. APLICACIÓN DE AJUSTES Y TESTS ---

# ---------------------------------------------------------
# A. Dimensión Temporal (Δt) -> Gamma -> Anderson-Darling
# ---------------------------------------------------------
data_delta_t <- df$delta_t + epsilon 
fit_delta_t <- fitdist(data_delta_t, "gamma")

# Calculamos estadísticas de bondad de ajuste
gof_delta_t <- gofstat(fit_delta_t)

# Extraemos el resultado del test Anderson-Darling
# (statistic: valor del test, p-value: probabilidad)
ad_test_delta_t <- list(
  statistic = gof_delta_t$ad,
  p_value = gof_delta_t$adtest, # "rejected" o "not rejected"
  is_valid = gof_delta_t$adtest == "not rejected"
)


# ---------------------------------------------------------
# B. Frescura de Cuenta (Age) -> Gamma -> Anderson-Darling
# ---------------------------------------------------------
data_age <- df$age + epsilon
fit_age <- fitdist(data_age, "gamma")

gof_age <- gofstat(fit_age)

ad_test_age <- list(
  statistic = gof_age$ad,
  p_value = gof_age$adtest,
  is_valid = gof_age$adtest == "not rejected"
)


# ---------------------------------------------------------
# C. CAMBIO CRÍTICO: Retorno (Sharpe) para N > 5000
# ---------------------------------------------------------

# 1. Ajuste de parámetros a una T-Student
# Esto te da la media (m), escala (s) y grados de libertad (df)
fit_returns <- fitdistr(df$winning_volume, "t")


# TO-DO: tiene sentido hacer AD o no?
# 2. Test de Normalidad (Reemplazo de Shapiro-Wilk)
# Usamos Anderson-Darling. Si p-value < 0.05, confirmamos que NO es normal
# (lo cual es bueno si tu hipótesis es que tiene colas pesadas/T-Student).
# ad_result_normalidad <- ad.test(df$winning_volume)

# 3. Test de Bondad de Ajuste para la T-Student calculada (KS Test)
# Como ad.test solo valida normalidad, usamos Kolmogorov-Smirnov 
# para validar si se ajusta a la T-Student con los parámetros encontrados.

# Si el p-value es bajo (lo cual es normal con N > 5000), guíate por el valor stat. 
# Si es cercano a 0 (ej. 0.01 - 0.05), el ajuste a T-Student es sólido.

# Estandarizamos los datos usando los parámetros estimados
params <- fit_returns$estimate
data_std <- (df$winning_volume - params["m"]) / params["s"]

# Aplicamos KS comparando contra la distribución teórica 'pt' (T-Student)
ks_test_t_student <- ks.test(data_std, "pt", df = params["df"])

t_student_valid <- ks_test_t_student$statistic < 0.05

# ---------------------------------------------------------
# D. Slippage -> Gamma -> Anderson-Darling
# ---------------------------------------------------------
data_slippage <- df$alpha_slippage + epsilon
fit_slippage <- fitdist(data_slippage, "gamma")

gof_slippage <- gofstat(fit_slippage)

ad_test_slippage <- list(
  statistic = gof_slippage$ad,
  p_value = gof_slippage$adtest,
  is_valid = gof_slippage$adtest == "not rejected"
)


# ---------------------------------------------------------
# E. Entropía -> Beta -> Kolmogorov-Smirnov
# ---------------------------------------------------------
n <- length(df$entropy)
data_entropy <- (df$entropy * (n - 1) + 0.5) / n

fit_entropy <- fitdist(data_entropy, "beta")

gof_entropy <- gofstat(fit_entropy)

# Extraemos específicamente el test Kolmogorov-Smirnov
ks_test_entropy <- list(
  statistic = gof_entropy$ks,
  p_value = gof_entropy$kstest,
  is_valid = gof_entropy$kstest == "not rejected"
)

# --- 3. DETECCIÓN DE INSIDERS Y WEBHOOK ---

# A. Verificación Global de Ajustes
# Solo procedemos si todas las distribuciones se ajustan correctamente a los datos
distribuciones_validas <- ad_test_delta_t$is_valid & 
                          ad_test_age$is_valid & 
                          t_student_valid & 
                          ad_test_slippage$is_valid & 
                          ks_test_entropy$is_valid

insiders_found <- data.frame()
market_id_val <- if(nrow(df) > 0) df$market_id[1] else "UNKNOWN"

if (distribuciones_validas) {
  
  # B. Cálculo de Umbrales (Thresholds) basados en las distribuciones ajustadas
  # Buscamos el 1% extremo (0.01 o 0.99) según la lógica de "Insider"
  
  # 1. Delta T: "Muy poco" -> Quantile 0.01 de la Gamma ajustada
  th_delta_t <- qgamma(0.01, shape = fit_delta_t$estimate["shape"], rate = fit_delta_t$estimate["rate"])
  
  # 2. Age: "Muy poca" -> Quantile 0.01 de la Gamma ajustada
  th_age <- qgamma(0.01, shape = fit_age$estimate["shape"], rate = fit_age$estimate["rate"])
  
  # 3. Winning Volume: "Mucho" -> Quantile 0.99 de la T-Student ajustada
  # Formula: media + escala * qt(0.99, df)
  th_volume <- params_t["m"] + params_t["s"] * qt(0.99, df = params_t["df"])
  
  # 4. Slippage: "Mucho" -> Quantile 0.99 de la Gamma ajustada
  th_slippage <- qgamma(0.99, shape = fit_slippage$estimate["shape"], rate = fit_slippage$estimate["rate"])
  
  # 5. Entropía: "Muy poca" -> Quantile 0.01 de la Beta ajustada
  # Nota: Usamos la escala transformada para comparar
  th_entropy_scaled <- qbeta(0.01, shape1 = fit_entropy$estimate["shape1"], shape2 = fit_entropy$estimate["shape2"])
  
  # C. Filtrado de Insiders
  # Añadimos la columna de entropía escalada al DF original para filtrar
  df$entropy_scaled <- data_entropy_scaled
  
  insiders_df <- df %>%
    filter(
      delta_t < th_delta_t,          # Muy rápido
      age < th_age,                  # Cuenta muy nueva
      winning_volume > th_volume,    # Mucho volumen ganador
      alpha_slippage > th_slippage,  # Mucho slippage (agresivo)
      entropy_scaled < th_entropy_scaled # Muy poca entropía (certeza)
    )
  
  # Preparamos los datos. TO-DO: faltan categories
  if (nrow(insiders_df) > 0) {
    insiders_list <- insiders_df$wallet_id
    # categories ya inicializada
  } else {
    # devolvemos listas vacías, solo market_id
    insiders_list <- list()
    categories <- list()
  }

} else {
  # Si los ajustes fallan, no devolvemos insiders (seguridad estadística)
  print("ADVERTENCIA: Los datos no se ajustan a las distribuciones teóricas. Abortando detección.")
  insiders_list <- list()
  categories <- list()
}

# --- 4. ENVÍO AL API REST (FastAPI) ---

# URL de tu endpoint (ajustar según entorno)
api_url <- "http://localhost:8000/api/v1/market-analysis/complete"

payload <- list(
  market_id = market_id_val,
  insiders_addresses = insiders_list,
  categories = categories,
  meta = list(
    status = if(distribuciones_validas) "success" else "distribution_fit_failed",
    processed_at = format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  )
)

tryCatch({
  response <- POST(
    url = api_url,
    body = payload,
    encode = "json",
    add_headers(`Content-Type` = "application/json")
  )
  
  if (status_code(response) == 200) {
    print(paste("Webhook enviado con éxito para Market:", market_id_val))
  } else {
    print(paste("Error en Webhook:", status_code(response)))
    print(content(response, "text"))
  }
}, error = function(e) {
  print(paste("Fallo de conexión al enviar Webhook:", e$message))
})