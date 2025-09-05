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
maquinas <- maquinas_vendu$epay %>% set_names(maquinas_vendu$epay)

last_sale_at <- bq_get_last_sale('epay')
last_sale_at_locale <- lubridate::as_datetime(last_sale_at$lastSaleAt, tz = 'America/Caracas') %m+% hours(4)
cat('Last Sale in BQ at: ', last_sale_at_locale,' \n')
cat('Last Sale in BQ at: ', as.character(last_sale_at_locale),' \n')

current_time_locale <- lubridate::with_tz(Sys.time(), 'America/Caracas')
today <- lubridate::floor_date(current_time_locale, 'day') %>% as_date()
month <- lubridate::month(today)
year <- lubridate::year(today)

# Get sales
cat('Starting sales ETL\n')
ventas_resp <- maquinas %>% 
  map(
    possibly(
      ~get_ventas_epay(.x, endpoint = 'venta', month, year),
      list()
    )
  )

ventas <- ventas_resp %>% 
  discard(is_empty) %>% 
  map(
    ~resp_body_json(.x) %>%
      clean_response(
        .fecha_consulta = current_time_locale,
        .numeric_fields = field_map$epay$numeric,
        .integer_fields = field_map$epay$integer
      )
  ) %>% 
  list_rbind(names_to = 'id_maquina') %>% 
  filter(fecha > last_sale_at_locale)

ventas %>% 
  write_vendu_table(
    dataset = 'epay',
    table = 'odsSales', 
    write_disposition = 'WRITE_APPEND'
  )

# GET stores
cat('Starting store ETL\n')
almacen <- glue::glue('https://epay.uno/api/?e=prods&id={maquinas[[1]]}') %>% 
  httr2::request() %>%
  httr2::req_perform() %>% 
  httr2::resp_body_json() %>% 
  clean_response(
    .fecha_consulta = current_time_locale,
    .numeric_fields = field_map$epay$numeric,
    .integer_fields = c(field_map$epay$integer, 'codigo')
  )

almacen %>%
  write_vendu_table(
    dataset = 'epay',
    table = 'odsStore',
    write_disposition = 'WRITE_TRUNCATE'
  )

# GET Machine slot positions
cat('Starting positions ETL\n')
posicion <- maquinas %>% 
  map(
    ~glue::glue('https://epay.uno/api/?e=canal&id={.x}') %>% 
      httr2::request() %>% 
      httr2::req_perform() %>% 
      resp_body_json() %>%
      clean_response(
        .fecha_consulta = current_time_locale,
        .numeric_fields = field_map$epay$numeric,
        .integer_fields = field_map$epay$integer
      )
  ) %>% 
  list_rbind(names_to = 'id_maquina')

posicion %>%
  write_vendu_table(
    dataset = 'epay',
    table = 'odsPosition',
    write_disposition = 'WRITE_APPEND'
  )
