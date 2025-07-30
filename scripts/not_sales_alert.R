print('Initializing Libraries')
library(dplyr)
library(bigrquery)
library(blastula)
library(purrr)
library(lubridate)
library(gt)
source('scripts/helpers.R')

print('Authenticating to BigQuery')
# Decrypt BQ key and authenticate
bigrquery::bq_auth(
  path = gargle::secret_decrypt_json(
    'inst/secrets/vendu-tech-general-encrypted.json',
    key = 'BIGQUERY_ENCRYPTED_KEY'
  )
)

# Caracas Time
current_time_locale <- lubridate::with_tz(Sys.time(), 'America/Caracas')
current_hour <- lubridate::hour(current_time_locale)

# GET  Today Sales
print('Getting data from BigQuery')
today_sales <- bq_get_today_sales(current_hour)

# Compose Email
print('Composing Email body')
alert_email <- compose_noSales_alert_email(today_sales, current_time_locale)
# alert_email <- blastula::compose_email(body='email de prueba')

# Create Credentials
email_creds <- blastula::creds_envvar(
  user = Sys.getenv('GMAIL_ACCOUNT'),
  pass_envvar = 'GOOGLE_APP_PASSWORD',
  provider = 'gmail'
)

# Report state
print(sessionInfo())
cat('Size of Email: ', object.size(alert_email)/1000, '\n')
cat('Size of Table: ', object.size(today_sales)/1000, '\n')

# Send email
print('Sending Email')
.to <- c('carlos@tuvendu.com', 'miguel@tuvendu.com', 'alessandro@tuvendu.com')
.from <- Sys.getenv('GMAIL_ACCOUNT')
cat('from:', .from, '\n')
tryCatch({
  alert_email %>% 
    smtp_send(
      from = .from,
      to = .to,
      bcc = 'rodrigoserranom8@gmail.com',
      subject = 'Vendu Alert: Resumen de ventas',
      credentials = email_creds,
      verbose = TRUE
    )
  
},
error = function(e){
  cat('Error de R capturado:\n')
  print(e)
  stop(e)
  
})
