# queries.R

# Packages ----------------------------------------------------------------

library(dplyr)
library(glue)
library(RPostgreSQL)
library(tradestatistics)
library(highcharter)

# Read credentials from file in .gitignore --------------------------------

readRenviron("/apis/tradestatistics")

# DB connection parameters ------------------------------------------------

drv <- dbDriver("PostgreSQL") # choose the driver

dbusr <- Sys.getenv("dbusr")
dbpwd <- Sys.getenv("dbpwd")
dbhost <- Sys.getenv("dbhost")
dbname <- Sys.getenv("dbname")

con <- dbConnect(
  drv,
  host = dbhost,
  port = 5432,
  user = dbusr,
  password = dbpwd,
  dbname = dbname
)

# Clean inputs ------------------------------------------------------------

clean_char_input <- function(x, i, j) {
  y <- iconv(x, to = "ASCII//TRANSLIT", sub = " ")
  y <- stringr::str_replace_all(y, "[^[:alpha:]]", " ")
  y <- stringr::str_squish(y)
  y <- stringr::str_trim(y)
  y <- stringr::str_to_lower(y)
  y <- stringr::str_sub(y, i, j)
  
  return(y)
}

clean_num_input <- function(x, i, j) {
  y <- iconv(x, to = "ASCII//TRANSLIT", sub = " ")
  y <- ifelse(y == "all", y, stringr::str_replace_all(y, "[^[:digit:]]", " "))
  y <- stringr::str_squish(y)
  y <- stringr::str_trim(y)
  y <- stringr::str_to_lower(y)
  y <- stringr::str_sub(y, i, j)
  
  return(y)
}

# Available years in the DB -----------------------------------------------

min_year <- dbGetQuery(con, glue_sql("SELECT MIN(year) FROM public.hs07_yr")) %>% as.numeric()
max_year <- dbGetQuery(con, glue_sql("SELECT MAX(year) FROM public.hs07_yr")) %>% as.numeric()

# List of countries (to filter API parameters) ----------------------------

# create vectors by continent to filter by using meta variables like americas, africa, etc

## Africa
countries_africa <- ots_attributes_countries %>% 
  filter(continent == "Africa") %>% 
  select(country_iso) %>% 
  as.vector()

## Americas
countries_americas <- ots_attributes_countries %>% 
  filter(continent == "Americas") %>% 
  select(country_iso) %>% 
  as.vector()

## Asia
countries_asia <- ots_attributes_countries %>% 
  filter(continent == "Asia") %>% 
  select(country_iso) %>% 
  as.vector()

## Europe
countries_europe <- ots_attributes_countries %>% 
  filter(continent == "Europe") %>% 
  select(country_iso) %>% 
  as.vector()

## Oceania
countries_oceania <- ots_attributes_countries %>% 
  filter(continent == "Oceania") %>% 
  select(country_iso) %>% 
  as.vector()

continents_data <- tibble(
  country_iso = c(paste0("c-", c("af", "am", "as", "eu", "oc")), "all"),
  country_name_english = c(
    "Alias for all valid ISO codes in Africa",
    "Alias for all valid ISO codes in the Americas",
    "Alias for all valid ISO codes in Asia",
    "Alias for all valid ISO codes in Europe",
    "Alias for all valid ISO codes in Oceania",
    "Alias for all valid ISO codes in the World"
  )
)

# Title and description ---------------------------------------------------

#* @apiTitle Open Trade Statistics API
#* @apiDescription Sandbox to experiment with the available functions

# Hello World -------------------------------------------------------------

#* Echo back Hello World!
#* @get /

function() {
  paste("Hello World! Welcome to Open Trade Statistics API")
}

# Countries ---------------------------------------------------------------

#* Echo back the result of a query on attributes_countries table
#* @get /countries

function() {
  ots_attributes_countries
}

# Products ----------------------------------------------------------------

#* Echo back the result of a query on attributes_products table
#* @get /products

function() {
  ots_attributes_products
}

# Communities -------------------------------------------------------------

#* Echo back the result of a query on attributes_communities table
#* @get /communities

function() {
  ots_attributes_communities
}

# Product shortnames ------------------------------------------------------

#* Echo back the result of a query on attributes_products_shortnames table
#* @get /product_shortnames

function() {
  ots_attributes_product_shortnames
}

# YC ----------------------------------------------------------------------

#* Echo back the result of a query on yc table
#* @param y Year
#* @param c Product code
#* @param l Product code length
#* @get /yc

function(y = NULL, c = "all", l = 4) {
  y <- as.integer(y)
  c <- clean_num_input(c, 1, 6)
  l <- clean_num_input(l, 1, 3)
  
  if (nchar(c) == 2) { l <- "all" }
  
  if (nchar(y) != 4 | !y >= min_year | !y <= max_year) {
    return("The specified year is not a valid integer value. Read the documentation: tradestatistics.io")
    stop()
  }
  
  if (!nchar(c) <= 6 | !c %in% ots_attributes_products$product_code) {
    return("The specified product is not a valid string. Read the documentation: tradestatistics.io")
    stop()
  }
  
  if (!nchar(l) <= 3 | !l %in% c(4, 6, "all")) {
    return("The specified length is not a valid integer value. Read the documentation: tradestatistics.io")
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
  
  if (c != "all" & nchar(c) != 2) {
    query <- glue_sql(
      query, 
      " AND product_code = {c}",
      .con = con
    )
  }
  
  if (c != "all" & nchar(c) == 2) {
    query <- glue_sql(
      query, 
      " AND LEFT(product_code, 2) = {c}",
      .con = con
    )
  }
  
  if (l != "all") {  
    query <- glue_sql(
      query, 
      " AND product_code_length = {l}",
      .con = con
    )
  }
  
  data <- dbGetQuery(con, query)
  
  if (nrow(data) == 0) {
    data <- tibble(
      year = y,
      product_code = c,
      observation = "No data available for these filtering parameters"
    )
  }
  
  return(data)
}

# Product rankings --------------------------------------------------------

#* Echo back the result of a query on yr table
#* @param y Year
#* @get /product_rankings

function(y = 2017) {
  y <- as.integer(y)
  
  if (nchar(y) != 4 | !y >= min_year | !y <= max_year) {
    return("The specified year is not a valid integer value.")
    stop()
  }
  
  stopifnot(is.integer(y))
  
  query <- glue_sql(
    "
    SELECT year, product_code, export_value_usd, import_value_usd, pci_4_digits_product_code
    FROM public.hs07_yc
    WHERE year = {y}
    AND product_code_length = 4
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
    
    arrange(-pci_4_digits_product_code) %>%
    mutate(pci_4_digits_product_code_rank = row_number())
  
  return(data)
}

# YR ----------------------------------------------------------------------

#* Echo back the result of a query on yr table
#* @param y Year
#* @param r Reporter ISO
#* @get /yr

function(y = NULL, r = NULL) {
  y <- as.integer(y)
  r <- clean_char_input(r, 1, 4)
  
  if (nchar(y) != 4 | !y >= min_year | !y <= max_year) {
    return("The specified year is not a valid integer value. Read the documentation: tradestatistics.io")
    stop()
  }
  
  if (!nchar(r) <= 4 | !r %in% c(ots_attributes_countries$country_iso, continents_data$country_iso)) {
    return("The specified reporter is not a valid ISO code or alias. Read the documentation: tradestatistics.io")
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
  
  if (r != "all" & nchar(r) == 3) {  
    query <- glue_sql(
      query, 
      " AND reporter_iso = {r}", 
      .con = con
    )
  }
  
  if (r != "all" & nchar(r) == 4) {  
    r_expanded_alias <- switch(
      r, 
      "c-af" = countries_africa,
      "c-am" = countries_americas,
      "c-as" = countries_asia,
      "c-eu" = countries_europe,
      "c-oc" = countries_oceania
    )
    
    query <- glue_sql(
      query, 
      " AND reporter_iso IN ({vals*})", 
      vals = r_expanded_alias$country_iso,
      .con = con
    )
  }
  
  data <- dbGetQuery(con, query)
  
  if (nrow(data) == 0) {
    data <- tibble(
      year = y,
      reporter_iso = r,
      observation = "No data available for these filtering parameters"
    )
  }
  
  return(data)
}

# YR short ----------------------------------------------------------------

#* Echo back the result of a query on yr table
#* @param y Year
#* @param r Reporter ISO
#* @get /yr_short

function(y = NULL, r = NULL) {
  y <- as.integer(y)
  r <- clean_char_input(r, 1, 4)
  
  if (nchar(y) != 4 | !y >= min_year | !y <= max_year) {
    return("The specified year is not a valid integer value. Read the documentation: tradestatistics.io")
    stop()
  }
  
  if (!nchar(r) <= 4 | !r %in% c(ots_attributes_countries$country_iso, continents_data$country_iso)) {
    return("The specified reporter is not a valid ISO code or alias. Read the documentation: tradestatistics.io")
    stop()
  }
  
  query <- glue_sql(
    "
    SELECT year, reporter_iso, export_value_usd, import_value_usd
    FROM public.hs07_yr 
    WHERE year = {y}
    ", 
    .con = con
  )
  
  if (r != "all" & nchar(r) == 3) {  
    query <- glue_sql(
      query, 
      " AND reporter_iso = {r}", 
      .con = con
    )
  }
  
  if (r != "all" & nchar(r) == 4) {  
    r_expanded_alias <- switch(
      r, 
      "c-af" = countries_africa,
      "c-am" = countries_americas,
      "c-as" = countries_asia,
      "c-eu" = countries_europe,
      "c-oc" = countries_oceania
    )
    
    query <- glue_sql(
      query, 
      " AND reporter_iso IN ({vals*})", 
      vals = r_expanded_alias$country_iso,
      .con = con
    )
  }
  
  data <- dbGetQuery(con, query)
  
  if (nrow(data) == 0) {
    data <- tibble(
      year = y,
      reporter_iso = r,
      observation = "No data available for these filtering parameters"
    )
  }
  
  return(data)
}

# Reporters ---------------------------------------------------------------

#* Echo back the result of a query on yr table
#* @get /reporters

function(y = 2017) {
  y <- as.integer(y)
  
  if (nchar(y) != 4 | !y >= min_year | !y <= max_year) {
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

function(y = 2017) {
  y <- as.integer(y)
  
  if (nchar(y) != 4 | !y >= min_year | !y <= max_year) {
    return("The specified year is not a valid integer value.")
    stop()
  }
  
  stopifnot(is.integer(y))
  
  query <- glue_sql(
    "
    SELECT year, reporter_iso, export_value_usd, import_value_usd, eci_4_digits_product_code
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
    
    arrange(-eci_4_digits_product_code) %>%
    mutate(eci_4_digits_product_code_rank = row_number())
  
  return(data)
}

# YRC ---------------------------------------------------------------------

#* Echo back the result of a query on yrc table
#* @param y Year
#* @param r Reporter ISO
#* @param c Product code
#* @param l Product code length
#* @get /yrc

function(y = NULL, r = NULL, c = "all", l = 4) {
  y <- as.integer(y)
  r <- clean_char_input(r, 1, 4)
  c <- clean_num_input(c, 1, 6)
  l <- clean_num_input(l, 1, 3)
  
  if (nchar(c) == 2) { l <- "all" }
  
  if (nchar(y) != 4 | !y >= min_year | !y <= max_year) {
    return("The specified year is not a valid integer value. Read the documentation: tradestatistics.io")
    stop()
  }
  
  if (!nchar(r) <= 4 | !r %in% c(ots_attributes_countries$country_iso, continents_data$country_iso)) {
    return("The specified reporter is not a valid ISO code or alias. Read the documentation: tradestatistics.io")
    stop()
  }
  
  if (!nchar(c) <= 6 | !c %in% ots_attributes_products$product_code) {
    return("The specified product is not a valid string. Read the documentation: tradestatistics.io")
    stop()
  }
  
  if (!nchar(l) <= 3 | !l %in% c(4, 6, "all")) {
    return("The specified length is not a valid integer value. Read the documentation: tradestatistics.io")
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
  
  if (r != "all" & nchar(r) == 3) {  
    query <- glue_sql(
      query, 
      " AND reporter_iso = {r}", 
      .con = con
    )
  }
  
  if (r != "all" & nchar(r) == 4) {  
    r_expanded_alias <- switch(
      r, 
      "c-af" = countries_africa,
      "c-am" = countries_americas,
      "c-as" = countries_asia,
      "c-eu" = countries_europe,
      "c-oc" = countries_oceania
    )
    
    query <- glue_sql(
      query, 
      " AND reporter_iso IN ({vals*})", 
      vals = r_expanded_alias$country_iso,
      .con = con
    )
  }
  
  if (c != "all" & nchar(c) != 2) {
    query <- glue_sql(
      query, 
      " AND product_code = {c}",
      .con = con
    )
  }
  
  if (c != "all" & nchar(c) == 2) {
    query <- glue_sql(
      query, 
      " AND LEFT(product_code, 2) = {c}",
      .con = con
    )
  }
  
  if (l != "all") {
    query <- glue_sql(
      query,
      " AND product_code_length = {l}",
      .con = con
    )
  }
  
  data <- dbGetQuery(con, query)
  
  if (nrow(data) == 0) {
    data <- tibble(
      year = y,
      reporter_iso = r,
      product_code = c,
      observation = "No data available for these filtering parameters"
    )
  }
  
  return(data)
}

# YRP ---------------------------------------------------------------------

#* Echo back the result of a query on yrp table
#* @param y Year
#* @param r Reporter ISO
#* @param p Partner ISO
#* @get /yrp

function(y = NULL, r = NULL, p = NULL) {
  y <- as.integer(y)
  r <- clean_char_input(r, 1, 4)
  p <- clean_char_input(p, 1, 4)
  
  if (nchar(y) != 4 | !y >= min_year | !y <= max_year) {
    return("The specified year is not a valid integer value. Read the documentation: tradestatistics.io")
    stop()
  }
  
  if (!nchar(r) <= 4 | !r %in% c(ots_attributes_countries$country_iso, continents_data$country_iso)) {
    return("The specified reporter is not a valid ISO code or alias. Read the documentation: tradestatistics.io")
    stop()
  }
  
  if (!nchar(p) <= 4 | !p %in% c(ots_attributes_countries$country_iso, continents_data$country_iso)) {
    return("The specified partner is not a valid ISO code or alias. Read the documentation: tradestatistics.io")
    stop()
  }
  
  query <- glue_sql("
                    SELECT *
                    FROM public.hs07_yrp
                    WHERE year = {y}
                    ", .con = con)
  
  if (r != "all" & nchar(r) == 3) {
    query <- glue_sql(
      query, 
      " AND reporter_iso = {r}", 
      .con = con
    )
  }
  
  if (r != "all" & nchar(r) == 4) {  
    r_expanded_alias <- switch(
      r, 
      "c-af" = countries_africa,
      "c-am" = countries_americas,
      "c-as" = countries_asia,
      "c-eu" = countries_europe,
      "c-oc" = countries_oceania
    )
    
    query <- glue_sql(
      query, 
      " AND reporter_iso IN ({vals*})", 
      vals = r_expanded_alias$country_iso,
      .con = con
    )
  }
  
  if (p != "all" & nchar(p) == 3) {  
    query <- glue_sql(
      query, 
      " AND partner_iso = {p}", 
      .con = con
    )
  }
  
  if (p != "all" & nchar(p) == 4) {  
    p2 <- switch(
      p, 
      "c-af" = countries_africa,
      "c-am" = countries_americas,
      "c-as" = countries_asia,
      "c-eu" = countries_europe,
      "c-oc" = countries_oceania
    )
    
    query <- glue_sql(
      query, 
      " AND partner_iso IN ({vals*})", 
      vals = p2$country_iso,
      .con = con
    )
  }
  
  data <- dbGetQuery(con, query)
  
  if (nrow(data) == 0) {
    data <- tibble(
      year = y,
      reporter_iso = r,
      partner_iso = p,
      observation = "No data available for these filtering parameters"
    )
  }
  
  return(data)
}

# YRPC --------------------------------------------------------------------

#* Echo back the result of a query on yrpc table
#* @param y Year
#* @param r Reporter ISO
#* @param p Partner ISO
#* @param c Product code
#* @param l Product code length
#* @get /yrpc

function(y = NULL, r = NULL, p = NULL, c = "all", l = 4) {
  y <- as.integer(y)
  r <- clean_char_input(r, 1, 4)
  p <- clean_char_input(p, 1, 4)
  c <- clean_num_input(c, 1, 6)
  l <- clean_num_input(l, 1, 3)
  
  if (nchar(c) == 2) { l <- "all" }
  
  if (nchar(y) != 4 | !y >= min_year | !y <= max_year) {
    return("The specified year is not a valid integer value. Read the documentation: tradestatistics.io")
    stop()
  }
  
  if (!nchar(r) <= 4 | !r %in% c(ots_attributes_countries$country_iso, continents_data$country_iso)) {
    return("The specified reporter is not a valid ISO code or alias. Read the documentation: tradestatistics.io")
    stop()
  }
  
  if (!nchar(p) <= 4 | !p %in% c(ots_attributes_countries$country_iso, continents_data$country_iso)) {
    return("The specified partner is not a valid ISO code or alias. Read the documentation: tradestatistics.io")
    stop()
  }
  
  if (!nchar(c) <= 6 | !c %in% ots_attributes_products$product_code) {
    return("The specified product is not a valid string. Read the documentation: tradestatistics.io")
    stop()
  }
  
  if (!nchar(l) <= 3 | !l %in% c(4, 6, "all")) {
    return("The specified length is not a valid integer value. Read the documentation: tradestatistics.io")
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
  
  if (r != "all" & nchar(r) == 3) {  
    query <- glue_sql(
      query, 
      " AND reporter_iso = {r}", 
      .con = con
    )
  }
  
  if (r != "all" & nchar(r) == 4) {  
    r_expanded_alias <- switch(
      r, 
      "c-af" = countries_africa,
      "c-am" = countries_americas,
      "c-as" = countries_asia,
      "c-eu" = countries_europe,
      "c-oc" = countries_oceania
    )
    
    query <- glue_sql(
      query, 
      " AND reporter_iso IN ({vals*})", 
      vals = r_expanded_alias$country_iso,
      .con = con
    )
  }
  
  if (p != "all" & nchar(p) == 3) {  
    query <- glue_sql(
      query, 
      " AND partner_iso = {p}", 
      .con = con
    )
  }
  
  if (p != "all" & nchar(p) == 4) {  
    p2 <- switch(
      p, 
      "c-af" = countries_africa,
      "c-am" = countries_americas,
      "c-as" = countries_asia,
      "c-eu" = countries_europe,
      "c-oc" = countries_oceania
    )
    
    query <- glue_sql(
      query, 
      " AND partner_iso IN ({vals*})", 
      vals = p2$country_iso,
      .con = con
    )
  }
  
  if (c != "all" & nchar(c) != 2) {
    query <- glue_sql(
      query, 
      " AND product_code = {c}",
      .con = con
    )
  }
  
  if (c != "all" & nchar(c) == 2) {
    query <- glue_sql(
      query, 
      " AND LEFT(product_code, 2) = {c}",
      .con = con
    )
  }
  
  if (l != "all") {
    query <- glue_sql(
      query,
      " AND product_code_length = {l}",
      .con = con
    )
  }
  
  data <- dbGetQuery(con, query)
  
  if (nrow(data) == 0) {
    data <- tibble(
      year = y,
      reporter_iso = r,
      partner_iso = p,
      product_code = c,
      observation = "No data available for these filtering parameters"
    )
  }
  
  return(data)
}

# Available tables --------------------------------------------------------

#* All the tables generated by this API
#* @get /tables

function() {
  ots_attributes_tables
}

# Year range --------------------------------------------------------------

#* All the tables generated by this API
#* @get /year_range

function() {
  tibble(year = c(min_year, max_year))
}
