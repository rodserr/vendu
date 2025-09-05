################ GLOBAL FUNCTIONS ################

bq_get_maquinas <- function(){
  query <- "
  SELECT 
    distinct id_maquina 
  from `prodVendu.assignments` 
  where fecha > date_sub(current_date(), INTERVAL 15 DAY)"
  
  ids_df <- bigrquery::bq_project_query(
    x = 'vendu-tech',
    query = query
  ) %>% 
    bigrquery::bq_table_download()
  
  maqs_puntov <- c('GVRC004_VENDU1','GVRC004_VENDU2', 'MERC1')
  maqs_epay <- ids_df %>% filter(!id_maquina %in% maqs_puntov) %>% pull(id_maquina)
  
  maqs_list <- list(
    puntov = maqs_puntov,
    epay = maqs_epay
  )
  
  return(maqs_list)
  
}

bq_get_last_sale <- function(dataset){
  query <- glue::glue("SELECT UNIX_SECONDS( max(fecha) ) as lastSaleAt from `{dataset}.odsSales`")
  
  bigrquery::bq_project_query(
    x = 'vendu-tech',
    query = query
  ) %>% 
    bigrquery::bq_table_download()
}

# Maquinas List
# maquinas_list <- list(
#   puntov = list(
#     'GVRC004_VENDU1', 
#     'GVRC004_VENDU2', 
#     'MERC1'
#   ),
#   epay = list(
#     '6edeae11-a06d-4dd7-b766-75257b55a1d8', 
#     '52b69d4d-864f-4641-8be6-80a6438cb34e',
#     'a70e5131-3f79-4773-b064-681f5afd407b', 
#     '7b5dac83-116b-43f2-b98a-706b5112b4c3',
#     '7f0a2baa-b739-4c0c-bddd-23bc8d10da94',
#     '348952bc-590d-4f4b-984f-ed4c210a1436',
#     '6c193483-84c3-467b-ab39-1c217ffe7a87',
#     '0fb541ae-f56d-48bf-8fd9-4279667a4799', 
#     'b8a894d0-64bf-4cd0-9f90-c874936d9d19', 
#     '04e71197-83f5-41e7-89d8-deb10aa3905e',
#     '96374528-7ecb-46fa-8e8a-6d1c9aac0399',
#     '98102415-cd5d-4db6-ab5b-001b8bb1341b',
#     '920dd6f8-9833-457c-b32d-53d11d1ebec3', 
#     'b5fd0924-afa6-490a-a458-dab1c888a104',
#     'e07d26b2-2f8c-4225-802f-26cfbd39948b',
#     'b7aa53ea-5938-4b51-80d0-c8c49973393e', 
#     'ka4d7fd39-afaf-4a59-8659-b192675e64e3',
#     'f3d84ff8-57b5-4ba0-8a87-6b3cdb93b07f',
#     'k106f56eb-3146-45bc-b9ab-d875a59a896e',
#     '95755383-0eaa-4859-bfe8-6e3fd2a374aax',
#     '95755383-0eaa-4859-bfe8-6e3fd2a374aa',
#     '2e974aa9-3836-4432-9152-8fc2d59bbc71',
#     'db7bb43f-c75e-4527-bcd0-5b42615cbc80',
#     '7822e461-5603-4ab3-a188-0b2a491504eb',
#     '74f8996d-598e-4b65-a7b8-872f406bf037',
#     '95755383-0eaa-4859-bfe8-6e3fd2a374a'
#   )
# )

field_map <- list(
  puntov = list(
    numeric = c(
      'cantidad', 'cantidad_divisas', 'tasa', 'costo_unitario', 'precio_de_venta','precio_venta_divisas', 
      'precio_venta_bs', 'costo'
    ),
    integer = c(
      'id', 'pos', 'id_producto', 'existencia', 'minimo', 'maximo', 'minimo_maq', 'maximo_maq', 'id_venta', 
      'posicion'
    )
  ),
  epay = list(
    numeric = c(
      'usd', 'precio', 'monto'
    ),
    integer = c(
      'rowid', 'activo', 'cantidad', 'minimo', 'maximo', 'producto', 'medio', 'tid'
    )
    
  )
)

# Bind response and formatting columns 
clean_response <- function(
    l,
    .fecha_consulta,
    .numeric_fields = field_map$puntov$numeric,
    .integer_fields = field_map$puntov$integer
){
  
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
    create_disposition = 'CREATE_IF_NEEDED', 
    write_disposition = 'WRITE_TRUNCATE', 
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
write_sales_condition <- function(df, current_hour, dataset = 'puntov'){
  
  # Si son las 11pm entonces APPEND a tabla historica y limpiar tabla diaria 
  if(lubridate::hour(current_hour) == 23){
    cat('Appending to odsSales\n')
    df %>% 
      write_vendu_table(
        dataset = dataset,
        table = 'odsSales', 
        write_disposition = 'WRITE_APPEND'
      )
    
    cat('Dropping odsSalesCurrentDay\n')
    df %>% 
      filter(is.na(id)) %>%
      write_vendu_table(
        dataset = dataset,
        table = 'odsSalesCurrentDay', 
        write_disposition = 'WRITE_TRUNCATE'
      )
    
    
  } else{
    
    df %>% 
      write_vendu_table(
        dataset = dataset,
        table = 'odsSalesCurrentDay', 
        write_disposition = 'WRITE_TRUNCATE'
      )
    
  }
}

################ PUNTO V ################

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
get_ventas_puntov <- function(id_maq, token, desde, hasta, tipo_tran = 'TODO'){
  
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

################ EPAY ################
get_ventas_epay <- function(id_maq, endpoint = 'venta', month, year){
  
  # endpoint: 'pago' o 'venta'
  
  glue::glue('https://epay.uno/api/?e={endpoint}') %>%
    httr2::request() %>%
    httr2::req_url_query(
      id = id_maq,
      m = month,
      a = year
    ) %>%
    httr2::req_perform()
  
}

################ EMAIL ALERT ################

# GET today sales from bigquery
bq_get_today_sales <- function(hour_){
  
  query <- glue::glue('
SELECT * FROM `vendu-tech.prodVendu.emailSales`({hour_})
  ')
  
  bigrquery::bq_project_query(
    x = 'vendu-tech',
    query = query
  ) %>% 
    bigrquery::bq_table_download()
  
}

# Compose Email alert
compose_noSales_alert_email_deprecated <- function(sales, hour){
  
  # Identify Machines without sales
  maquinas_wo_sales <- sales %>%
    filter(is.na(ventas_n)) %>%
    pull(assigned)
  
  # Build sales resume
  if(nrow(sales) > 0){
    sales_vector <- sales %>%
      filter(!is.na(ventas_n)) %>%
      arrange(desc(ventas_usd)) %>%
      mutate(
        hour_ = strftime(lastSaleHour, '%I:%M %p', tz = 'America/Caracas'),
        sales_ = glue::glue('- **`{assigned}`**: `${round(ventas_usd, 2)}` | `{hour_}`')
      ) %>%
      pull(sales_) %>%
      paste0(collapse = '\n')
    
    total_sales_usd <- sales$ventas_usd %>% sum(na.rm = TRUE) %>% round(2)
    total_sales_n <- sales$ventas_n %>% sum(na.rm = TRUE)
    
  }
  else{
    sales_vector <- 'No hay ventas registradas'
  }
  
  # Add Vendu Logo
  img_file <- add_image(
    file = 'inst/styles/logo_vendunegro-01.png',
    width = 150,
    align = 'center'
  )
  
  maq_fmt <- glue::glue('- **`{maquinas_wo_sales}`**') %>% paste0(collapse = '\n')
  # print(maq_fmt)
  body_text <-
    glue::glue(
      '
      
      ## 📊  Resumen de Ventas del Día
      A las `{hour}` de hoy
      
      - Monto Facturado: **`${total_sales_usd}`** 
      
      - Transacciones Registradas: **`{total_sales_n}`**
      
      Detalle por Máquina (Facturado | Hora de Última venta):
      
      {sales_vector}
      
      #### Máquinas sin ventas registradas:
      
      {maq_fmt}
      '   
    )
  
  blastula::compose_email(body = md(c(img_file, body_text)))
  
}


# Compose Email alert
compose_noSales_alert_email <- function(sales, hour_){
  
  current_hour_fmt <- format(hour_, '%I%p') %>% tolower()
  
  # Build sales resume
  if(nrow(sales) > 0){
    
    sales_vector <- sales %>% 
      filter(!is.na(ventas_n)) %>% 
      arrange(desc(ventas_usd)) %>%
      select(assigned, ventas_n, ventas_usd, lastSaleAt) %>%
      gt(
        rowname_col = 'assigned',
      ) %>% 
      fmt_currency(
        columns = ventas_usd,
        currency = 'USD'
      ) %>%
      fmt_datetime(
        columns = lastSaleAt,
        format = '%I:%M %p'
      ) %>% 
      cols_label(
        ventas_n = 'Órdenes',
        ventas_usd = 'Facturado',
        lastSaleAt = 'Hora de Última venta'
      ) %>% 
      gt::as_raw_html()
    
    total_sales_usd <- sales$ventas_usd %>% sum(na.rm = TRUE) %>% round(2)
    total_sales_n <- sales$ventas_n %>% sum(na.rm = TRUE)
    
  }
  else{
    sales_vector <- 'No hay ventas registradas'
  }
  
  # Identify Machines without sales
  no_sales_vector <- sales %>% 
    filter(is.na(ventas_n)) %>% 
    arrange(desc(lastSaleAt)) %>%
    mutate(
      days_no_sale = difftime(current_time_locale, lastSaleAt),
      prob = round(daysWithSalesAtHour/daysWithSales, 2)
    ) %>% 
    select(assigned, lastSaleAt, days_no_sale, prob) %>% 
    gt(
      rowname_col = 'assigned',
    ) %>% 
    fmt_duration(
      columns = days_no_sale,
      output_units = c('days', "hours")
    ) %>% 
    cols_label(
      lastSaleAt = 'Fecha Última venta',
      days_no_sale = 'Tiempo desde Última venta',
      prob = glue::glue('P(Al menos una Venta antes de las {current_hour_fmt})')
    ) %>% 
    gt::as_raw_html()
  
  # Add Vendu Logo
  img_file <- add_image(
    file = 'inst/styles/logo_vendunegro-01.png',
    width = 150,
    align = 'center'
  )
  
  body_text <-
    glue::glue(
      '
      
      ## 📊  Resumen de Ventas del Día
      A las `{current_hour_fmt}` de hoy
      
      - Monto Facturado: **`${total_sales_usd}`** 
      
      - Transacciones Registradas: **`{total_sales_n}`**
      
      {sales_vector}
      
      #### Máquinas sin ventas registradas:
      
      {no_sales_vector}
      '   
    )
  
  blastula::compose_email(body = md(c(img_file, body_text)))
  
}
