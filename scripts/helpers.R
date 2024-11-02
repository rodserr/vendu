# Renew Token
get_token <- function(email, pwd){
  
  cat('Rewnewing token\n')
  
  token_resp <- httr2::request('https://gamantoken.esgaman.com/public/api/login') %>% 
    httr2::req_method('POST') %>%
    httr2::req_url_query(
      correo = email,
      password = pwd,
    ) %>% 
    httr2::req_perform()
  
  token_resp %>%
    httr2::resp_body_json() %>% 
    pluck('token')
  
}


# GET ventas
get_ventas <- function(id_maq, token, desde, hasta, tipo_tran = 'TODO'){
  
  glue::glue('https://gamantoken.esgaman.com/public/api/{id_maq}/venta_formato_excel') %>% 
    httr2::request() %>%
    httr2::req_auth_bearer_token(token) %>%
    httr2::req_method('POST') %>%
    httr2::req_url_query(
      fecha_desde = desde,
      fecha_hasta = hasta,
      tipo_transaccion = tipo_tran
    ) %>%
    httr2::req_perform()

}

# Bind response and formatting columns 
clean_response <- function(l, .fecha_consulta){
  .numeric_fields <- c('cantidad', 'cantidad_divisas', 'tasa', 'costo_unitario', 'precio_de_venta',
                       'precio_venta_divisas', 'precio_venta_bs', 'costo')
  .integer_fields <- c('id', 'pos', 'id_producto', 'existencia', 'minimo', 'maximo', 'id_venta', 'posicion')
  
  l %>% 
    map(~.x %>% discard(is.null) %>% as_tibble) %>% 
    list_rbind() %>%
    mutate(
      across(any_of(.numeric_fields), as.numeric),
      across(any_of(.integer_fields), as.integer), 
      across(any_of('fecha'), as.POSIXct), 
      fecha_consulta = .fecha_consulta
    )
}

# Write table to BQ
write_vendu_table <- function(
    data, 
    project = 'vendu-tech',
    dataset = 'puntov', 
    table, 
    create_disposition = "CREATE_IF_NEEDED", 
    write_disposition = "WRITE_TRUNCATE", 
    quiet = F
){
  
  cat('Writting to: ', table, '\n')
  bigrquery::bq_table(
    project = project, 
    dataset = dataset, 
    table = table
  ) %>% 
    bigrquery::bq_table_upload(
      values = data, 
      create_disposition = create_disposition, 
      write_disposition = write_disposition, 
      quiet = quiet, 
      fields = bigrquery::as_bq_fields(data)
    )
}
