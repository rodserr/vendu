# Libraries
library(dplyr)
library(bigrquery)
library(httr2)
library(purrr)
library(lubridate)
source('scripts/helpers.R')

# Decrypt BQ key and authenticate
bigrquery::bq_auth(
  path = gargle::secret_decrypt_json(
    'inst/secrets/vendu-tech-general-encrypted.json',
    key = 'BIGQUERY_ENCRYPTED_KEY'
  )
)

# Set configs
maquinas_vendu <- bq_get_maquinas()
maquinas <- maquinas_vendu$puntov %>% set_names(maquinas_vendu$puntov)

last_sale_at <- bq_get_last_sale('puntov')
last_sale_at_locale <- lubridate::with_tz(last_sale_at$lastSaleAt, 'America/Caracas') %m+% hours(4)

current_time_locale <- lubridate::with_tz(Sys.time(), 'America/Caracas')
today <- lubridate::floor_date(current_time_locale, 'day') %>% as_date()
yesterday <- today %m-% days(1)

# Get Token
.esgaman_token <- get_token(Sys.getenv('ESGAMAN_MAIL'), Sys.getenv('ESGAMAN_PWD'))

# Get sales
cat('Starting sales ETL\n')
ventas_resp <- maquinas %>% 
  map(
    possibly(
      ~get_ventas_puntov(.x, .esgaman_token, yesterday, today), 
      list()
    ) 
  )

ventas <- ventas_resp %>% 
  discard(is_empty) %>% 
  map(
    ~resp_body_json(.x) %>%
      pluck('ventas', 1) %>% 
      clean_response(.fecha_consulta = current_time_locale)
  ) %>% 
  list_rbind(names_to = 'id_maquina') %>% 
  filter(fecha > last_sale_at_locale)

ventas %>% 
  write_vendu_table(
    dataset = 'puntov',
    table = 'odsSales', 
    write_disposition = 'WRITE_APPEND'
  )

# GET stores
cat('Starting store ETL\n')
almacen <- httr2::request('https://gamantoken.esgaman.com/public/api/get_almacen') %>% 
  httr2::req_auth_bearer_token(.esgaman_token) %>% 
  httr2::req_perform() %>% 
  httr2::resp_body_json() %>% 
  pluck('producto') %>% 
  clean_response(.fecha_consulta = current_time_locale)

almacen %>%
  write_vendu_table(
    dataset = 'puntov',
    table = 'odsStore',
    write_disposition = 'WRITE_TRUNCATE'
  )

# GET Machine slot positions
cat('Starting positions ETL\n')
posicion <- maquinas %>% 
  map(
    ~glue::glue('https://gamantoken.esgaman.com/public/api/{.x}/get_posicion') %>% 
      httr2::request() %>% 
      httr2::req_auth_bearer_token(.esgaman_token) %>% 
      httr2::req_perform() %>% 
      resp_body_json() %>%
      pluck('posicion') %>% 
      clean_response(.fecha_consulta = current_time_locale)
  ) %>% 
  list_rbind(names_to = 'id_maquina')

posicion %>%
  write_vendu_table(
    dataset = 'puntov',
    table = 'odsPosition',
    write_disposition = 'WRITE_APPEND'
  )
