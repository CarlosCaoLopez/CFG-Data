# market_inference.r — Motor de Cálculo D (Plumber endpoint)
#
# Expone:  POST /analyze  { "market_id": "0x..." }
#
# Flujo:
#   1. Lee closed_market_variables filtrado por market_id
#   2. Ajusta distribuciones para las 5 variables estadísticas
#   3. Detecta insiders (wallets en las 5 regiones críticas simultáneamente)
#   4. POST al endpoint E (REST API FastAPI) → /insider-detection
#
# Variables de entorno:
#   POSTGRES_HOST / POSTGRES_PORT / POSTGRES_DB / POSTGRES_USER / POSTGRES_PASSWORD
#   API_URL  (default: http://api:8000)

library(DBI)
library(RPostgres)
library(dplyr)
library(fitdistrplus)
library(MASS)
library(httr)
library(jsonlite)

epsilon <- 1e-6

# ──────────────────────────────────────────────────────────────────
# Helper: DB connection from env vars
# ──────────────────────────────────────────────────────────────────
db_connect <- function() {
  dbConnect(
    RPostgres::Postgres(),
    dbname   = Sys.getenv("POSTGRES_DB",       "insider_detection"),
    host     = Sys.getenv("POSTGRES_HOST",      "localhost"),
    port     = as.integer(Sys.getenv("POSTGRES_PORT", "5432")),
    user     = Sys.getenv("POSTGRES_USER",      "insider"),
    password = Sys.getenv("POSTGRES_PASSWORD",  "insider_pass")
  )
}

# ──────────────────────────────────────────────────────────────────
# Plumber endpoint
# ──────────────────────────────────────────────────────────────────

#* Analyze a closed market for insider trading
#* @param market_id The market condition ID to analyze
#* @serializer json list(auto_unbox = TRUE)
#* @post /analyze
function(market_id) {

  con <- db_connect()
  on.exit(dbDisconnect(con))

  # ── 1. Load data ──────────────────────────────────────────────
  df <- dbGetQuery(con, paste0(
    "SELECT * FROM closed_market_variables WHERE market_id = '", market_id, "'"
  ))

  categories_df <- dbGetQuery(con, paste0(
    "SELECT categories FROM markets WHERE market_id = '", market_id, "'"
  ))

  if (nrow(df) == 0) {
    return(list(
      status    = "no_data",
      market_id = market_id,
      insiders  = list()
    ))
  }

  MIN_ROWS <- 5L
  if (nrow(df) < MIN_ROWS) {
    message("ADVERTENCIA: datos insuficientes para ajuste (", nrow(df), " filas < ", MIN_ROWS, ")")
    return(list(
      status              = "insufficient_data",
      market_id           = market_id,
      insiders            = list(),
      distributions_valid = FALSE
    ))
  }

  # ── 2. Distribution fitting ───────────────────────────────────
  # Each fit is wrapped in tryCatch so that a single failure marks
  # that variable as invalid instead of crashing the endpoint.

  valid_delta_t  <- FALSE
  valid_age      <- FALSE
  valid_returns  <- FALSE
  valid_slippage <- FALSE
  valid_entropy  <- FALSE
  params         <- NULL   # T-student params, needed for threshold computation

  # A. Δt → Gamma → Anderson-Darling
  data_delta_t <- df$delta_t + epsilon
  fit_delta_t  <- tryCatch({
    f   <- fitdist(data_delta_t, "gamma")
    gof <- gofstat(f)
    valid_delta_t <<- gof$adtest == "not rejected"
    f
  }, error = function(e) {
    message("WARN: Gamma fit for delta_t failed: ", e$message)
    NULL
  })

  # B. Age → Gamma → Anderson-Darling
  data_age <- df$age + epsilon
  fit_age  <- tryCatch({
    f   <- fitdist(data_age, "gamma")
    gof <- gofstat(f)
    valid_age <<- gof$adtest == "not rejected"
    f
  }, error = function(e) {
    message("WARN: Gamma fit for age failed: ", e$message)
    NULL
  })

  # C. Winning Volume → T-Student → Kolmogorov-Smirnov
  fit_returns <- tryCatch({
    f        <- fitdistr(df$winning_volume, "t")
    p        <- f$estimate
    data_std <- (df$winning_volume - p["m"]) / p["s"]
    ks_ret   <- ks.test(data_std, "pt", df = p["df"])
    valid_returns <<- ks_ret$statistic < 0.05
    params        <<- p
    f
  }, error = function(e) {
    message("WARN: T-Student fit for winning_volume failed: ", e$message)
    NULL
  })

  # D. Slippage → Gamma → Anderson-Darling
  data_slippage <- df$alpha_slippage + epsilon
  fit_slippage  <- tryCatch({
    f   <- fitdist(data_slippage, "gamma")
    gof <- gofstat(f)
    valid_slippage <<- gof$adtest == "not rejected"
    f
  }, error = function(e) {
    message("WARN: Gamma fit for slippage failed: ", e$message)
    NULL
  })

  # E. Entropy → Beta → Kolmogorov-Smirnov
  n            <- length(df$entropy)
  data_entropy <- (df$entropy * (n - 1) + 0.5) / n
  fit_entropy  <- tryCatch({
    f   <- fitdist(data_entropy, "beta")
    gof <- gofstat(f)
    valid_entropy <<- gof$kstest == "not rejected"
    f
  }, error = function(e) {
    message("WARN: Beta fit for entropy failed: ", e$message)
    NULL
  })

  distribuciones_validas <- valid_delta_t & valid_age & valid_returns &
                            valid_slippage & valid_entropy

  # ── 3. Insider detection ──────────────────────────────────────
  insiders_list <- character(0)

  if (distribuciones_validas) {

    th_delta_t  <- qgamma(0.01,
                          shape = fit_delta_t$estimate["shape"],
                          rate  = fit_delta_t$estimate["rate"])

    th_age      <- qgamma(0.01,
                          shape = fit_age$estimate["shape"],
                          rate  = fit_age$estimate["rate"])

    th_volume   <- params["m"] + params["s"] * qt(0.99, df = params["df"])

    th_slippage <- qgamma(0.99,
                          shape = fit_slippage$estimate["shape"],
                          rate  = fit_slippage$estimate["rate"])

    th_entropy  <- qbeta(0.01,
                         shape1 = fit_entropy$estimate["shape1"],
                         shape2 = fit_entropy$estimate["shape2"])

    df$entropy_scaled <- data_entropy

    insiders_df   <- df %>%
      filter(
        delta_t        < th_delta_t,
        age            < th_age,
        winning_volume > th_volume,
        alpha_slippage > th_slippage,
        entropy_scaled < th_entropy
      )

    insiders_list <- insiders_df$wallet_id

  } else {
    message("ADVERTENCIA: distribuciones no validas para market_id=", market_id)
  }

  # ── 4. POST to REST API (E) ───────────────────────────────────
  api_url        <- paste0(Sys.getenv("API_URL", "http://api:8000"), "/insider-detection")
  categories_raw <- if (nrow(categories_df) > 0) unlist(categories_df$categories) else character(0)
  status         <- if (distribuciones_validas) "success" else "distribution_fit_failed"

  if (length(insiders_list) > 0) {
    wallets_payload <- lapply(insiders_list, function(addr) {
      list(
        wallet            = addr,
        categories_insider = as.list(categories_raw),
        command_type      = "ADD_WALLET"
      )
    })

    payload <- list(market_id = market_id, wallets = wallets_payload)

    tryCatch({
      resp <- POST(
        url    = api_url,
        body   = toJSON(payload, auto_unbox = TRUE),
        encode = "raw",
        add_headers(`Content-Type` = "application/json")
      )
      if (status_code(resp) == 200) {
        message("Insiders enviados a API para market: ", market_id,
                " (", length(insiders_list), " wallets)")
      } else {
        message("Error API: ", status_code(resp), " — ", content(resp, "text"))
      }
    }, error = function(e) {
      message("Fallo de conexion al API: ", e$message)
    })
  }

  list(
    status              = status,
    market_id           = market_id,
    insiders            = as.list(insiders_list),
    distributions_valid = distribuciones_validas
  )
}
