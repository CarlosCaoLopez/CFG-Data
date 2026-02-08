library(DBI)
library(RPostgres)
library(dplyr)
library(fitdistrplus)
library(MASS) 

# --- 1. CONEXIÓN Y CARGA DE DATOS ---
con <- dbConnect(RPostgres::Postgres(), 
                 dbname = "tu_base_datos", 
                 host = "localhost", 
                 port = 5432, 
                 user = "tu_usuario", 
                 password = "tu_password")

df <- dbGetQuery(con, "SELECT * FROM user_metrics")
dbDisconnect(con)

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
  code_result = gof_delta_t$adtest # "rejected" o "not rejected" según nivel de significancia
)


# ---------------------------------------------------------
# B. Frescura de Cuenta (Age) -> Gamma -> Anderson-Darling
# ---------------------------------------------------------
data_age <- df$account_age + epsilon
fit_age <- fitdist(data_age, "gamma")

gof_age <- gofstat(fit_age)

ad_test_age <- list(
  statistic = gof_age$ad,
  code_result = gof_age$adtest
)


# ---------------------------------------------------------
# C. CAMBIO CRÍTICO: Retorno (Sharpe) para N > 5000
# ---------------------------------------------------------

# 1. Ajuste de parámetros a una T-Student
# Esto te da la media (m), escala (s) y grados de libertad (df)
fit_returns <- fitdistr(df$return_sharpe, "t")

# 2. Test de Normalidad (Reemplazo de Shapiro-Wilk)
# Usamos Anderson-Darling. Si p-value < 0.05, confirmamos que NO es normal 
# (lo cual es bueno si tu hipótesis es que tiene colas pesadas/T-Student).
ad_result_normalidad <- ad.test(df$return_sharpe)

# 3. Test de Bondad de Ajuste para la T-Student calculada (KS Test)
# Como ad.test solo valida normalidad, usamos Kolmogorov-Smirnov 
# para validar si se ajusta a la T-Student con los parámetros encontrados.

# Si el p-value es bajo (lo cual es normal con N > 5000), guíate por el valor stat. 
# Si es cercano a 0 (ej. 0.01 - 0.05), el ajuste a T-Student es sólido.

# Estandarizamos los datos usando los parámetros estimados
params <- fit_returns$estimate
data_std <- (df$return_sharpe - params["m"]) / params["s"]

# Aplicamos KS comparando contra la distribución teórica 'pt' (T-Student)
ks_test_t_student <- ks.test(data_std, "pt", df = params["df"])

# Guardamos los resultados en una lista para inspección limpia
resultados_sharpe <- list(
  parametros_t = fit_returns$estimate,
  test_normalidad_AD = list(stat = ad_result_normalidad$statistic, p = ad_result_normalidad$p.value),
  test_t_student_KS = list(stat = ks_test_t_student$statistic, p = ks_test_t_student$p.value)
)

# ---------------------------------------------------------
# D. Slippage -> Gamma -> Anderson-Darling
# ---------------------------------------------------------
data_slippage <- df$slippage + epsilon
fit_slippage <- fitdist(data_slippage, "gamma")

gof_slippage <- gofstat(fit_slippage)

ad_test_slippage <- list(
  statistic = gof_slippage$ad,
  code_result = gof_slippage$adtest
)


# ---------------------------------------------------------
# E. Entropía -> Beta -> Kolmogorov-Smirnov
# ---------------------------------------------------------
n <- length(df$entropy_norm)
data_entropy <- (df$entropy_norm * (n - 1) + 0.5) / n

fit_entropy <- fitdist(data_entropy, "beta")

gof_entropy <- gofstat(fit_entropy)

# Extraemos específicamente el test Kolmogorov-Smirnov
ks_test_entropy <- list(
  statistic = gof_entropy$ks,
  code_result = gof_entropy$kstest
)

# TODO: Usar webhook http para informar de que ha terminado