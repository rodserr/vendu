library(dplyr)
library(bigrquery)
library(blastula)
library(purrr)
source('scripts/helpers.R')

# Decrypt BQ key and authenticate
bigrquery::bq_auth(
  path = gargle::secret_decrypt_json(
    'inst/secrets/vendu-tech-general-encrypted.json',
    key = 'BIGQUERY_ENCRYPTED_KEY'
  )
)

# Caracas Time
current_time_locale <- lubridate::with_tz(Sys.time(), 'America/Caracas')
current_hour <- format(current_time_locale, '%I%p') %>% tolower()

# GET  Today Sales
today_sales <- bq_get_today_sales()

# Send Email if meet criteria
if(all(!is.na(today_sales$ventas_n))){
  
  cat('\nTodas las maquinas han vendido al menos un producto hoy\n')
  
} else{
  
  cat('\nAl menos una maquina no cumple los criterios, enviando Email\n')
  
  # Compose Email
  alert_email <- compose_noSales_alert_email(today_sales, current_hour)
  
  # Create Credentials
  email_creds <- blastula::creds_envvar(
    user = Sys.getenv('GMAIL_ACCOUNT'),
    pass_envvar = 'GOOGLE_APP_PASSWORD',
    provider = 'gmail'
  )
  
  # Send email
  # .to <- c('carlos@tuvendu.com', 'miguel@tuvendu.com', 'alessandro@tuvendu.com')
  tryCatch({
    alert_email %>% 
      smtp_send(
        from = Sys.getenv('GMAIL_ACCOUNT'),
        # to = .to,
        to = 'rodrigoserranom8@gmail.com',
        # bcc = 'rodrigoserranom8@gmail.com',
        subject = 'Vendu Alert: Resumen de ventas',
        credentials = email_creds,
        verbose = TRUE
      )
    
  },
  error = function(e){
    cat("Error de R capturado:\n")
    print(e)
    stop(e)
    
  })
  
  
}
