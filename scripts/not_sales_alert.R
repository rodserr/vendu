library(dplyr)
library(bigrquery)
library(blastula)
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
if(nrow(today_sales) == length(maquinas_list) ){
  
  cat('\nTodas las maquinas han vendido al menos un producto hoy\n')
  
} else{
  
  cat('\nAl menos una maquina no cumple los criterios, enviando Email\n')
  
  # Compose Email
  maquinas <- names(maquinas_list)
  maquinas_w_sales <- today_sales$id_maquina
  maquinas_wo_sales <- maquinas[!maquinas %in% maquinas_w_sales]
  
  alert_email <- compose_noSales_alert_email(maquinas_wo_sales, current_hour)
  
  # Create Credentials
  email_creds <- blastula::creds_envvar(
    user = Sys.getenv('GMAIL_ACCOUNT'),
    pass_envvar = 'GOOGLE_APP_PASSWORD',
    provider = 'gmail'
  )
  
  # Send email
  alert_email %>% 
    smtp_send(
      from = Sys.getenv('GMAIL_ACCOUNT'),
      to = 'rodrigoserranom8@gmail.com',
      subject = 'Vendu Alert: Maquinas sin ventas a la hora actual',
      credentials = email_creds
    )
  
}
