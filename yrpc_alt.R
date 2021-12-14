# YRPC alt ----------------------------------------------------------------

#* Echo back the result of a query on yrpc alt table
#* @param y Year
#* @get /yrpc_alt

function(y = NULL) {
  y <- as.integer(y)
  y <- check_year(y)

  query <- d_yrpc_alt %>% 
    filter(year == y)
  
  data <- query %>% 
    collect() %>% 
    mutate(
      year = as.integer(remove_hive(year)),
      reporter_iso = remove_hive(reporter_iso)
    ) %>% 
    select(year, reporter_iso, everything())
  
  if (nrow(data) == 0) {
    data <- no_data("yrpc", y = y)
  }
  
  return(data)
}
