# market_inference.r — Motor de Cálculo D (Plumber endpoint)
#
# Expone:  POST /analyze  { "market_id": "0x..." }
#
# Flujo:
#   1. Lee closed_market_variables filtrado por market_id
#   2. Calcula percentiles empíricos para las variables estadísticas
#   3. Detecta insiders (wallets en las regiones críticas simultáneamente)
#   4. POST al endpoint E (REST API FastAPI) → /insider-detection
#
# Variables de entorno:
#   POSTGRES_HOST / POSTGRES_PORT / POSTGRES_DB / POSTGRES_USER / POSTGRES_PASSWORD
#   API_URL  (default: http://api:8000)

library(DBI)
library(RPostgres)
library(dplyr)
# library(fitdistrplus)  # OLD: parametric fitting (kept for future use)
# library(MASS)          # OLD: T-Student fitting (kept for future use)
library(httr)
library(jsonlite)

epsilon <- 1e-6

# ──────────────────────────────────────────────────────────────────
# Global: activar/desactivar entropy en el criterio insider
# Poner a TRUE cuando haya datos de múltiples categorías en la BD
# ──────────────────────────────────────────────────────────────────
ENTROPY_ENABLED <- FALSE

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
    "SELECT * FROM closed_market_variables WHERE market_id = '",
    market_id, "'"
  ))

  categories_df <- dbGetQuery(con, paste0(
    "SELECT categories FROM markets WHERE market_id = '",
    market_id, "'"
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
    message("ADVERTENCIA: datos insuficientes (",
            nrow(df), " filas < ", MIN_ROWS, ")")
    return(list(
      status              = "insufficient_data",
      market_id           = market_id,
      insiders            = list(),
      distributions_valid = FALSE
    ))
  }

  # ── 2. Empirical quantiles ──────────────────────────────────
  # All variables use empirical percentiles instead of parametric
  # distribution fitting. This is robust to any data shape.
  #
  # OLD APPROACH (parametric fitting) is commented out below each
  # variable for reference. Can be re-enabled per variable if the
  # data quality improves with more markets.

  # ── Helper: log data summary ──────────────────────────────────
  log_data_summary <- function(name, x) {
    message("  [DATA] ", name, ": n=", length(x),
            " min=", round(min(x), 6),
            " max=", round(max(x), 6),
            " mean=", round(mean(x), 6),
            " median=", round(median(x), 6),
            " sd=", round(sd(x), 6),
            " n_unique=", length(unique(x)),
            " n_zeros=", sum(x == 0),
            " n_negative=", sum(x < 0))
  }

  message("========== EMPIRICAL QUANTILES for market_id=",
          market_id, " ==========")
  message("  Total wallets (rows): ", nrow(df))
  message("  ENTROPY_ENABLED: ", ENTROPY_ENABLED)

  # A. Δt → Empirical percentile 1%
  # OLD: Log-normal fit + AD test
  # data_delta_t <- df$delta_t + epsilon
  # fit_delta_t  <- tryCatch({
  #   f   <- fitdist(data_delta_t, "lnorm")
  #   gof <- gofstat(f)
  #   valid_delta_t <<- gof$adtest == "not rejected"
  #   f
  # }, error = function(e) { NULL })
  # OLDER: Gamma fit + AD test
  # fit_delta_t  <- tryCatch({
  #   f   <- fitdist(data_delta_t, "gamma")
  #   gof <- gofstat(f)
  #   valid_delta_t <<- gof$adtest == "not rejected"
  #   f
  # }, error = function(e) { NULL })
  log_data_summary("delta_t", df$delta_t)
  th_delta_t <- quantile(df$delta_t, 0.01)
  message("  [QUANTILE] delta_t P01=", round(th_delta_t, 6))

  # B. Age → Empirical percentile 1%
  # OLD: Log-normal fit + AD test
  # data_age <- df$age + epsilon
  # fit_age  <- tryCatch({
  #   f   <- fitdist(data_age, "lnorm")
  #   gof <- gofstat(f)
  #   valid_age <<- gof$adtest == "not rejected"
  #   f
  # }, error = function(e) { NULL })
  # OLDER: Gamma fit + AD test
  # fit_age  <- tryCatch({
  #   f   <- fitdist(data_age, "gamma")
  #   gof <- gofstat(f)
  #   valid_age <<- gof$adtest == "not rejected"
  #   f
  # }, error = function(e) { NULL })
  log_data_summary("age", df$age)
  th_age <- quantile(df$age, 0.01)
  message("  [QUANTILE] age P01=", round(th_age, 6))

  # C. Winning Volume → Empirical percentile 99%
  # OLD: T-Student fit + KS test
  # fit_returns <- tryCatch({
  #   wv    <- df$winning_volume
  #   m_hat <- median(wv)
  #   s_hat <- mad(wv, constant = 1)
  #   if (s_hat < 1e-6) s_hat <- sd(wv)
  #   if (s_hat < 1e-6) s_hat <- 1
  #   f <- suppressWarnings(
  #     fitdistr(wv, "t",
  #              start = list(m = m_hat, s = s_hat, df = 5),
  #              lower = c(m = -Inf, s = 1e-6, df = 1))
  #   )
  #   p        <- f$estimate
  #   data_std <- (wv - p["m"]) / p["s"]
  #   ks_ret   <- ks.test(data_std, "pt", df = p["df"])
  #   valid_returns <<- ks_ret$p.value > 0.05
  #   params        <<- p
  #   f
  # }, error = function(e) { NULL })
  log_data_summary("winning_volume", df$winning_volume)
  th_volume <- quantile(df$winning_volume, 0.99)
  message("  [QUANTILE] winning_volume P99=",
          round(th_volume, 6))

  # D. Slippage → Empirical percentile 99%
  # OLD: Log-normal fit + AD test
  # data_slippage <- df$alpha_slippage + epsilon
  # fit_slippage  <- tryCatch({
  #   f   <- fitdist(data_slippage, "lnorm")
  #   gof <- gofstat(f)
  #   valid_slippage <<- gof$adtest == "not rejected"
  #   f
  # }, error = function(e) { NULL })
  # OLDER: Gamma fit + AD test
  # fit_slippage  <- tryCatch({
  #   f   <- fitdist(data_slippage, "gamma")
  #   gof <- gofstat(f)
  #   valid_slippage <<- gof$adtest == "not rejected"
  #   f
  # }, error = function(e) { NULL })
  log_data_summary("alpha_slippage", df$alpha_slippage)
  th_slippage <- quantile(df$alpha_slippage, 0.99)
  message("  [QUANTILE] alpha_slippage P99=",
          round(th_slippage, 6))

  # E. Entropy → Empirical percentile 1% (if enabled)
  # OLD: Beta fit + KS test
  # n            <- length(df$entropy)
  # data_entropy <- (df$entropy * (n - 1) + 0.5) / n
  # fit_entropy  <- tryCatch({
  #   f   <- fitdist(data_entropy, "beta")
  #   gof <- gofstat(f)
  #   valid_entropy <<- gof$kstest == "not rejected"
  #   f
  # }, error = function(e) { NULL })
  th_entropy <- NULL
  if (ENTROPY_ENABLED) {
    log_data_summary("entropy", df$entropy)
    th_entropy <- quantile(df$entropy, 0.01)
    message("  [QUANTILE] entropy P01=",
            round(th_entropy, 6))
  } else {
    message("  [SKIP] entropy: DISABLED via ENTROPY_ENABLED=FALSE")
  }

  message("========== THRESHOLDS ==========")
  message("  delta_t        < ", round(th_delta_t, 6))
  message("  age            < ", round(th_age, 6))
  message("  winning_volume > ", round(th_volume, 6))
  message("  alpha_slippage > ", round(th_slippage, 6))
  if (ENTROPY_ENABLED) {
    message("  entropy        < ", round(th_entropy, 6))
  }
  message("================================")

  # ── 3. Insider detection ──────────────────────────────────────
  # With empirical quantiles there is no distribution fit to fail,
  # so we always proceed to detection.

  # OLD: distribuciones_validas gate
  # distribuciones_validas <- valid_delta_t & valid_age &
  #                           valid_returns & valid_slippage &
  #                           valid_entropy
  # if (distribuciones_validas) { ... } else { skip }

  insiders_df <- df %>%
    filter(
      delta_t        <= th_delta_t,
      age            <= th_age,
      winning_volume >= th_volume,
      alpha_slippage >= th_slippage
    )

  # OLD: entropy filter (parametric)
  # if (ENTROPY_ENABLED) {
  #   df$entropy_scaled <- data_entropy
  #   insiders_df <- insiders_df %>%
  #     filter(entropy_scaled < th_entropy)
  # }
  if (ENTROPY_ENABLED && !is.null(th_entropy)) {
    insiders_df <- insiders_df %>%
      filter(entropy <= th_entropy)
  }

  insiders_list <- insiders_df$wallet_id

  message("  [DETECTION] Wallets passing all filters: ",
          length(insiders_list))
  if (length(insiders_list) > 0) {
    message("  [INSIDERS] ", paste(insiders_list, collapse = ", "))
  }

  # ── 4. POST to REST API (E) ───────────────────────────────────
  api_url        <- paste0(Sys.getenv("API_URL", "http://api:8000"),
                           "/insider-detection")
  categories_raw <- if (nrow(categories_df) > 0) {
    unlist(categories_df$categories)
  } else {
    character(0)
  }

  if (length(insiders_list) > 0) {
    wallets_payload <- lapply(insiders_list, function(addr) {
      list(
        wallet            = addr,
        categories_insider = as.list(categories_raw),
        command_type      = "ADD_WALLET"
      )
    })

    payload <- list(market_id = market_id,
                    wallets   = wallets_payload)

    tryCatch({
      api_key <- Sys.getenv("API_KEY", "")
      resp <- POST(
        url    = api_url,
        body   = toJSON(payload, auto_unbox = TRUE),
        encode = "raw",
        add_headers(
          `Content-Type` = "application/json",
          `X-API-Key`    = api_key
        )
      )
      if (status_code(resp) == 200) {
        message("Insiders enviados a API para market: ", market_id,
                " (", length(insiders_list), " wallets)")
      } else {
        message("Error API: ", status_code(resp),
                " — ", content(resp, "text"))
      }
    }, error = function(e) {
      message("Fallo de conexion al API: ", e$message)
    })
  }

  list(
    status              = "success",
    market_id           = market_id,
    insiders            = as.list(insiders_list),
    insiders_found      = length(insiders_list),
    thresholds          = list(
      delta_t    = as.numeric(th_delta_t),
      age        = as.numeric(th_age),
      volume     = as.numeric(th_volume),
      slippage   = as.numeric(th_slippage),
      entropy    = if (ENTROPY_ENABLED) as.numeric(th_entropy)
                   else "DISABLED"
    )
  )
}
