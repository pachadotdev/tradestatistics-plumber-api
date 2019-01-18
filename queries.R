# queries.R

# Packages ----------------------------------------------------------------

library(dplyr)
library(glue)
library(RPostgreSQL)

# DB connection parameters ------------------------------------------------

drv <- dbDriver("PostgreSQL") # choose the driver
source("/api/credentials.R")

con <- dbConnect(
  drv,
  host = dbhost,
  port = 5432,
  user = dbusr,
  password = dbpwd,
  dbname = dbname
)

# List of countries (to filter API parameters) ----------------------------

countries_query <- glue_sql(
  "
  SELECT *
  FROM public.attributes_countries
  ",
  .con = con
)

countries_data <- dbGetQuery(con, countries_query)

# create vectors by continent to filter by using meta variables like americas, africa, etc

## Africa
countries_africa <- countries_data %>% 
  filter(continent == "Africa") %>% 
  select(country_iso)

countries_africa <- paste0("('", paste(countries_africa$country_iso, collapse = "', '"), "')")

## Americas
countries_americas <- countries_data %>% 
  filter(continent == "Americas") %>% 
  select(country_iso)

countries_americas <- paste0("('", paste(countries_americas$country_iso, collapse = "', '"), "')")

## Asia
countries_asia <- countries_data %>% 
  filter(continent == "Asia") %>% 
  select(country_iso)

countries_asia <- paste0("('", paste(countries_asia$country_iso, collapse = "', '"), "')")

## Europe
countries_europe <- countries_data %>% 
  filter(continent == "Europe") %>% 
  select(country_iso)

countries_europe <- paste0("('", paste(countries_europe$country_iso, collapse = "', '"), "')")

## Oceania
countries_oceania <- countries_data %>% 
  filter(continent == "Oceania") %>% 
  select(country_iso)

countries_oceania <- paste0("('", paste(countries_oceania$country_iso, collapse = "', '"), "')")

# List of products (to filter API parameters) -----------------------------

products_query <- glue_sql(
  "
  SELECT *
  FROM public.attributes_products
  ",
  .con = con
)

products_data <- dbGetQuery(con, products_query)

group_codes <- unique(products_data$group_code)

# Hello World -------------------------------------------------------------

#* Echo back Hello World!
#* @get /

function() {
  paste("Hello World! This is Open Trade Statistics' API")
}

# API status --------------------------------------------------------------

#* Echo back API status
#* @get /status
function() {
  paste("The API is working :)")
}

# Countries ---------------------------------------------------------------

#* Echo back the result of a query on attributes_countries table
#* @get /countries
function() {
  countries_data
}

# Countries short (for Shiny) ---------------------------------------------

#* Echo back the result of a query on attributes_countries table
#* @get /countries_short
function() {
  countries_data %>% 
    select(country_iso, country_name_english, country_fullname_english)
}

# Products ----------------------------------------------------------------

#* Echo back the result of a query on attributes_countries table
#* @get /products
function() {
  products_data
}

# YC ----------------------------------------------------------------------

#* Echo back the result of a query on yc table
#* @param y Year
#* @param l Commodity code length
#* @get /yc
function(y = NULL, c = "all", g = "all", l = 4) {
  y <- as.integer(y)
  c <- tolower(substr(as.character(c), 1, 6))
  g <- tolower(substr(as.character(g), 1, 3))
  l <- tolower(substr(as.character(l), 1, 3))
  
  if (nchar(y) != 4 | !y %in% 1962:2016) {
    return("The specified year is not a valid integer value.")
    stop()
  }
  
  if (!nchar(c) <= 6 | !c %in% c(products_data$commodity_code, "all")) {
    return("The specified commodity is not a valid string.")
    stop()
  }
  
  if (!nchar(g) <= 3 | !g %in% c(group_codes, "all")) {
    return("The specified group is not a valid string.")
    stop()
  }
  
  if (!nchar(l) <= 3 | !l %in% c(4, 6, "all")) {
    return("The specified length is not a valid integer value.")
    stop()
  }
  
  query <- glue_sql(
    "
    SELECT *
    FROM public.hs07_yc
    WHERE year = {y}
    ", 
    .con = con
  )
  
  if (c != "all") {
    query <- glue_sql(
      query, 
      " AND commodity_code = {c}",
      .con = con
    )
  }
  
  if (g != "all") {
    query <- glue_sql(
      query, 
      " AND LEFT(commodity_code, 2) = {g}",
      .con = con
    )
  }
  
  if (l != "all") {  
    query <- glue_sql(
      query, 
      " AND commodity_code_length = {l}",
      .con = con
    )
  }
  
  data <- dbGetQuery(con, query)
  
  return(data)
}

# Product rankings --------------------------------------------------------

#* Echo back the result of a query on yr table
#* @param y Year
#* @get /product_rankings

function(y = 2016) {
  y <- as.integer(y)
  
  if (nchar(y) != 4 | !y %in% 1962:2016) {
    return("The specified year is not a valid integer value.")
    stop()
  }
  
  stopifnot(is.integer(y))
  
  query <- glue_sql(
    "
    SELECT year, commodity_code, export_value_usd, import_value_usd, pci_4_digits_commodity_code
    FROM public.hs07_yc
    WHERE year = {y}
    AND commodity_code_length = 4
    ",
    .con = con
  )
  
  data <- dbGetQuery(con, query)
  
  data <- data %>%
    as_tibble() %>%
    
    arrange(-export_value_usd) %>%
    mutate(export_value_usd_rank = row_number()) %>%
    
    arrange(-import_value_usd) %>%
    mutate(import_value_usd_rank = row_number()) %>%
    
    arrange(-pci_4_digits_commodity_code) %>%
    mutate(pci_4_digits_commodity_code_rank = row_number())
  
  return(data)
}

# YR ----------------------------------------------------------------------

#* Echo back the result of a query on yr table
#* @param y Year
#* @param r Reporter ISO
#* @get /yr
function(y = NULL, r = NULL) {
  y <- as.integer(y)
  r <- tolower(substr(as.character(r), 1, 3))
  
  if (nchar(y) != 4 | !y %in% 1962:2016) {
    return("The specified year is not a valid integer value.")
    stop()
  }
  
  if (nchar(r) != 3 | !r %in% c(countries_data$country_iso, "all")) {
    return("The specified reporter is not a valid ISO code, please check /countries.")
    stop()
  }
  
  query <- glue_sql(
    "
    SELECT * 
    FROM public.hs07_yr 
    WHERE year = {y}
    ", 
    .con = con
  )
  
  if (r != "all") {  
    query <- glue_sql(
      query, 
      " AND reporter_iso = {r}", 
      .con = con
    )
  }
  
  data <- dbGetQuery(con, query)
  
  return(data)
}

# Reporters ---------------------------------------------------------------

#* Echo back the result of a query on yr table
#* @param r Reporter ISO
#* @get /reporters
function(y = 2016) {
  y <- as.integer(y)
  
  if (nchar(y) != 4 | !y %in% 1962:2016) {
    return("The specified year is not a valid integer value.")
    stop()
  }
  
  query <- glue_sql(
    "
    SELECT reporter_iso
    FROM public.hs07_yr
    WHERE year = {y}
    ",
    .con = con
  )
  
  data <- dbGetQuery(con, query)
  
  return(data)
}

# Country rankings --------------------------------------------------------

#* Echo back the result of a query on yr table
#* @param y Year
#* @get /country_rankings
function(y = 2016) {
  y <- as.integer(y)
  
  if (nchar(y) != 4 | !y %in% 1962:2016) {
    return("The specified year is not a valid integer value.")
    stop()
  }
  
  stopifnot(is.integer(y))
  
  query <- glue_sql(
    "
    SELECT year, reporter_iso, export_value_usd, import_value_usd, eci_4_digits_commodity_code
    FROM public.hs07_yr
    WHERE year = {y}
    ",
    .con = con
  )
  
  data <- dbGetQuery(con, query)
  
  data <- data %>%
    as_tibble() %>%
    
    arrange(-export_value_usd) %>%
    mutate(export_value_usd_rank = row_number()) %>%
    
    arrange(-import_value_usd) %>%
    mutate(import_value_usd_rank = row_number()) %>%
    
    arrange(-eci_4_digits_commodity_code) %>%
    mutate(eci_4_digits_commodity_code_rank = row_number())
  
  return(data)
}

# YRC ---------------------------------------------------------------------

#* Echo back the result of a query on yrc table
#* @param y Year
#* @param r Reporter ISO
#* @param l Commodity code length
#* @get /yrc
function(y = NULL, r = NULL, c = "all", g = "all", l = 4) {
  y <- as.integer(y)
  r <- tolower(substr(as.character(r), 1, 3))
  c <- tolower(substr(as.character(c), 1, 6))
  g <- tolower(substr(as.character(g), 1, 3))
  l <- tolower(substr(as.character(l), 1, 3))
  
  if (nchar(y) != 4 | !y %in% 1962:2016) {
    return("The specified year is not a valid integer value.")
    stop()
  }
  
  if (nchar(r) != 3 | !r %in% c(countries_data$country_iso, "all")) {
    return("The specified reporter is not a valid ISO code, please check /countries.")
    stop()
  }
  
  if (!nchar(c) <= 6 | !c %in% c(products_data$commodity_code, "all")) {
    return("The specified commodity is not a valid string.")
    stop()
  }
  
  if (!nchar(g) <= 3 | !g %in% c(group_codes, "all")) {
    return("The specified group is not a valid string.")
    stop()
  }
  
  if (!nchar(l) <= 3 | !l %in% c(4, 6, "all")) {
    return("The specified length is not a valid integer value.")
    stop()
  }
  
  query <- glue_sql(
    "
    SELECT *
    FROM public.hs07_yrc
    WHERE year = {y}
    ", 
    .con = con
  )
  
  if (r != "all") {
    query <- glue_sql(
      query,
      " AND reporter_iso = {r}",
      .con = con
    )
  }
  
  if (c != "all") {  
    query <- glue_sql(
      query, 
      " AND commodity_code = {c}",
      .con = con
    )
  }
  
  if (g != "all") {  
    query <- glue_sql(
      query, 
      " AND LEFT(commodity_code, 2) = {g}",
      .con = con
    )
  }
  
  if (l != "all") {
    query <- glue_sql(
      query,
      " AND commodity_code_length = {l}",
      .con = con
    )
  }
  
  data <- dbGetQuery(con, query)
  
  return(data)
}

# YRP ---------------------------------------------------------------------

#* Echo back the result of a query on yrp table
#* @param y Year
#* @param r Reporter ISO
#* @param p Partner ISO
#* @param l Commodity code length
#* @get /yrp
function(y = NULL, r = NULL, p = NULL, l = NULL) {
  y <- as.integer(y)
  r <- tolower(substr(as.character(r), 1, 3))
  p <- tolower(substr(as.character(p), 1, 3))
  
  if (nchar(y) != 4 | !y %in% 1962:2016) {
    return("The specified year is not a valid integer value.")
    stop()
  }
  
  if (nchar(r) != 3 | !r %in% c(countries_data$country_iso, "all")) {
    return("The specified reporter is not a valid ISO code, please check /countries.")
    stop()
  }
  
  if (nchar(p) != 3 | !p %in% c(countries_data$country_iso, "all")) {
    return("The specified reporter is not a valid ISO code, please check /countries.")
    stop()
  }
  
  query <- glue_sql("
                    SELECT *
                    FROM public.hs07_yrp
                    WHERE year = {y}
                    ", .con = con)
  
  if (r != "all") {
    query <- glue_sql(
      query,
      " AND reporter_iso = {r}",
      .con = con
    )
  }
  
  if (p != "all") {
    query <- glue_sql(
      query,
      " AND partner_iso = {p}",
      .con = con
    )
  }
  
  data <- dbGetQuery(con, query)
  
  return(data)
}

# YRPC --------------------------------------------------------------------

#* Echo back the result of a query on yrpc table
#* @param y Year
#* @param r Reporter ISO
#* @param p Partner ISO
#* @param l Commodity code length
#* @get /yrpc
function(y = NULL, r = NULL, p = NULL, c = "all", g = "all", l = 4) {
  y <- as.integer(y)
  r <- tolower(substr(as.character(r), 1, 3))
  p <- tolower(substr(as.character(p), 1, 3))
  c <- tolower(substr(as.character(c), 1, 6))
  g <- tolower(substr(as.character(g), 1, 3))
  l <- tolower(substr(as.character(l), 1, 3))
  
  if (nchar(y) != 4 | !y %in% 1962:2016) {
    return("The specified year is not a valid integer value.")
    stop()
  }
  
  if (nchar(r) != 3 | !r %in% c(countries_data$country_iso, "all")) {
    return("The specified reporter is not a valid ISO code, please check /countries.")
    stop()
  }
  
  if (nchar(p) != 3 | !p %in% c(countries_data$country_iso, "all")) {
    return("The specified reporter is not a valid ISO code, please check /countries.")
    stop()
  }
  
  if (!nchar(c) <= 6 | !c %in% c(products_data$commodity_code, "all")) {
    return("The specified commodity is not a valid string.")
    stop()
  }
  
  if (!nchar(g) <= 3 | !g %in% c(group_codes, "all")) {
    return("The specified group is not a valid string.")
    stop()
  }
  
  if (!nchar(l) <= 3 | !l %in% c(4, 6, "all")) {
    return("The specified length is not a valid integer value.")
    stop()
  }
  
  query <- glue_sql(
    "
    SELECT *
    FROM public.hs07_yrpc
    WHERE year = {y}
    ",
    .con = con
  )
  
  if (r != "all") {
    query <- glue_sql(
      query,
      " AND reporter_iso = {r}",
      .con = con
    )
  }
  
  if (p != "all") {
    query <- glue_sql(
      query,
      " AND partner_iso = {p}",
      .con = con
    )
  }
  
  if (c != "all") {  
    query <- glue_sql(
      query, 
      " AND commodity_code = {c}",
      .con = con
    )
  }
  
  if (g != "all") {  
    query <- glue_sql(
      query, 
      " AND LEFT(commodity_code, 2) = {g}",
      .con = con
    )
  }
  
  if (l != "all") {
    query <- glue_sql(
      query,
      " AND commodity_code_length = {l}",
      .con = con
    )
  }
  
  data <- dbGetQuery(con, query)
  
  return(data)
}
