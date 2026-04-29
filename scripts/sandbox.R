
maquinas %in% check_maq

check_maq <- c(
  'Fc86e02f-ebc8-470a-8dd2-ba8979cc2c96'
)

ventas_resp <- check_maq %>% 
  set_names(check_maq) |> 
  map(
    possibly(
      ~get_ventas_epay(.x, endpoint = 'venta', 3, 2026),
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
  filter(fecha <= last_sale_at_locale)

ventas |> 
  summarise(
    .by = id_maquina,
    min_date = as.Date(min(fecha)),
    max_date = as.Date(max(fecha)),
    n_rows = n()
  )

ventas %>% 
  write_vendu_table(
    dataset = 'epay',
    table = 'odsSales', 
    write_disposition = 'WRITE_APPEND'
  )



###

ventas |> 
  filter(
    id_maquina == '4dce41ef-faf2-4f42-ae5d-833b86ba4940',
    fecha > '2026-01-27'
  ) |> View()
