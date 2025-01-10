# Maquinas List
maquinas_list <- list(
  GVRC004_VENDU='GVRC004_VENDU1', 
  GVRC004_VENDU2='GVRC004_VENDU2', 
  MERC1='MERC1'
)

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
  .integer_fields <- c('id', 'pos', 'id_producto', 'existencia', 'minimo', 'maximo', 
                       'minimo_maq', 'maximo_maq', 'id_venta', 'posicion')
  
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


# Write Sales conditional
write_sales_condition <- function(df, current_hour){
  
  # Si son las 11pm entonces APPEND a tabla historica y limpiar tabla diaria 
  if(lubridate::hour(current_hour) == 23){
    cat('Appending to odsSales\n')
    df %>% 
      write_vendu_table(
        table = 'odsSales', 
        write_disposition = 'WRITE_APPEND'
      )
    
    cat('Dropping odsSalesCurrentDay\n')
    df %>% 
      filter(is.na(id)) %>%
      write_vendu_table(
        table = 'odsSalesCurrentDay', 
        write_disposition = 'WRITE_TRUNCATE'
      )
    
    
  } else{
    
    df %>% 
      write_vendu_table(
        table = 'odsSalesCurrentDay', 
        write_disposition = 'WRITE_TRUNCATE'
      )
    
  }
}

# GET today sales from bigquery
bq_get_today_sales <- function(){
  query <- "
  SELECT 
  id_maquina,
  count(*) as ventas_n,
  sum(precio_venta_divisas) as ventas_usd,
  FROM `vendu-tech.puntov.odsSalesCurrentDay` 
  group by 1
  "
  
  bigrquery::bq_project_query(
    x = 'vendu-tech',
    query = query
  ) %>% 
    bigrquery::bq_table_download()
  
}

# Compose Email alert
compose_noSales_alert_email <- function(sales, current_machines, hour){
  
  # Identify Machines without sales
  maquinas <- names(current_machines)
  maquinas_w_sales <- sales$id_maquina
  maquinas_wo_sales <- maquinas[!maquinas %in% maquinas_w_sales]
  
  # Build sales resume
  if(nrow(sales) > 0){
    sales_vector <- sales %>% 
      mutate(
        sales_ = glue::glue('- **`{id_maquina}`**: ${round(ventas_usd, 2)}')
      ) %>% 
      pull(sales_) %>% 
      paste0(collapse = '\n')
    
  }
  else{
    sales_vector <- 'No hay ventas registradas'
  }
  
  # Add Vendu Logo
  img_file <- add_image(
    file = "inst/styles/logo_vendunegro-01.png",
    width = 150,
    align = 'center'
  )
  
  maq_fmt <- glue::glue('- **`{maquinas_wo_sales}`**') %>% paste0(collapse = '\n')
  # print(maq_fmt)
  body_text <-
    glue::glue(
      "
      
      ## 🚨  Alerta de Ventas 
      
      A las `{hour}` de hoy, las máquinas:
      
      {maq_fmt}
      
      No han vendido ningún producto. 
      Quizás quieras revisar que estén operando correctamente.
      
      #### Resumen de Ventas del Día
      {sales_vector}
      "   
    )
  
  blastula::compose_email(body = md(c(img_file, body_text)))
  
}
