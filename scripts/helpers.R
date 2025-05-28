################ GLOBAL FUNCTIONS ################

# Maquinas List
maquinas_list <- list(
  puntov = list(
    'GVRC004_VENDU1', 
    'GVRC004_VENDU2', 
    'MERC1'
  ),
  epay = list(
    '6edeae11-a06d-4dd7-b766-75257b55a1d8', # netuno
    '52b69d4d-864f-4641-8be6-80a6438cb34e', # oficentro
    'a70e5131-3f79-4773-b064-681f5afd407b', # idet
    '7b5dac83-116b-43f2-b98a-706b5112b4c3', # plaza
    '7f0a2baa-b739-4c0c-bddd-23bc8d10da94', # venemer
    '348952bc-590d-4f4b-984f-ed4c210a1436', # clinica_el_avila
    '6c193483-84c3-467b-ab39-1c217ffe7a87', # cashea
    '0fb541ae-f56d-48bf-8fd9-4279667a4799', # cementerio_del_este
    'b8a894d0-64bf-4cd0-9f90-c874936d9d19', # cincuentenario_ucab
    '04e71197-83f5-41e7-89d8-deb10aa3905e' # mercantil
  )
)

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
bq_get_today_sales <- function(){
  query <- '
with
t0 as (
  SELECT 
    id_maquina,
    count(*) as ventas_n,
    sum(precio_venta_divisas) as ventas_usd,
    max(fecha) as lastSaleHour
  FROM `vendu-tech.puntov.odsSalesCurrentDay` 
  WHERE date(timestamp_sub(fecha, INTERVAL 4 HOUR)) = current_date()
  GROUP BY 1
  
  UNION ALL 
  
  SELECT 
    sales.id_maquina, 
    count(*) as ventas_n,
    sum(pv.precio_de_venta) as ventas_usd,
    max(sales.fecha) as lastSaleHour 
  FROM `vendu-tech.epay.odsSalesCurrentDay` sales
  LEFT JOIN `epay.odsStore` st on st.rowid = sales.producto
  LEFT JOIN `puntov.odsStore` pv on pv.id_producto = st.codigo
  WHERE date(timestamp_sub(sales.fecha, INTERVAL 4 HOUR)) = current_date()
  GROUP BY 1
)

select
a.assigned,
t0.*
from t0
full join `puntov.idtAssignments` a on t0.id_maquina = a.id_maquina
where a.fecha = current_date("America/Caracas")
  '
  
  bigrquery::bq_project_query(
    x = 'vendu-tech',
    query = query
  ) %>% 
    bigrquery::bq_table_download()
  
}

# Compose Email alert
compose_noSales_alert_email <- function(sales, hour){
  
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
        sales_ = glue::glue('- **`{assigned}`**: `${round(ventas_usd, 2)}`   Última venta a las `{hour_}`')
      ) %>% 
      pull(sales_) %>% 
      paste0(collapse = '\n')
    
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
      
      ## 🚨  Alerta de Ventas 
      
      A las `{hour}` de hoy, las máquinas:
      
      {maq_fmt}
      
      No han vendido ningún producto. 
      Quizás quieras revisar que estén operando correctamente.
      
      #### Resumen de Ventas del Día
      {sales_vector}
      '   
    )
  
  blastula::compose_email(body = md(c(img_file, body_text)))
  
}
