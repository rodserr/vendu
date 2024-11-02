# Libraries
library(dplyr)
library(bigrquery)
library(httr2)
library(purrr)
source('scripts/helpers.R')

# Set configs
bigrquery::bq_auth(path = here('.secrets/vendu-tech-general.json'))
maquinas_list <- list(GVRC004_VENDU1 = 'GVRC004_VENDU1', GVRC004_VENDU2 = 'GVRC004_VENDU2')
fecha_consulta_global <- Sys.time()
hoy <- Sys.Date()

# Get Token
.esgaman_token <- get_token(Sys.getenv('ESGAMAN_MAIL'), Sys.getenv('ESGAMAN_PWD'))

# Get sales
cat('Starting sales ETL\n')
ventas_resp <- maquinas_list %>% 
  map(~get_ventas(.x, .esgaman_token, hoy, hoy))

ventas <- ventas_resp %>% 
  map(
    ~resp_body_json(.x) %>%
      pluck('ventas', 1) %>% 
      clean_response(.fecha_consulta = fecha_consulta_global)
  ) %>% 
  list_rbind(names_to = 'id_maquina')

ventas %>% 
  write_vendu_table(
    table = 'odsSalesCurrentDay', 
    write_disposition = 'WRITE_TRUNCATE'
  )

# GET stores
cat('Starting store ETL\n')
almacen <- httr2::request('https://gamantoken.esgaman.com/public/api/get_almacen') %>% 
  httr2::req_auth_bearer_token(.esgaman_token) %>% 
  httr2::req_perform() %>% 
  httr2::resp_body_json() %>% 
  pluck('producto') %>% 
  clean_response(.fecha_consulta = fecha_consulta_global)

almacen %>%
  write_vendu_table(
    table = 'odsStore',
    write_disposition = 'WRITE_TRUNCATE'
  )

# GET Machine slot positions
cat('Starting positions ETL\n')
posicion <- maquinas_list %>% 
  map(
    ~glue::glue('https://gamantoken.esgaman.com/public/api/{.x}/get_posicion') %>% 
      httr2::request() %>% 
      httr2::req_auth_bearer_token(.esgaman_token) %>% 
      httr2::req_perform() %>% 
      resp_body_json(.x) %>%
      pluck('posicion') %>% 
      clean_response(.fecha_consulta = fecha_consulta_global)
  ) %>% 
  list_rbind(names_to = 'id_maquina')

posicion %>%
  write_vendu_table(
    table = 'odsPosition',
    write_disposition = 'WRITE_TRUNCATE'
  )