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
maquinas <- maquinas_list$epay %>% set_names(maquinas_list$epay)

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
  filter(lubridate::floor_date(fecha, 'day') == today)

ventas %>% 
  write_vendu_table(
    dataset = 'epay',
    table = 'odsSalesCurrentDay', 
    write_disposition = 'WRITE_TRUNCATE'
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
