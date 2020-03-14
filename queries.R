# queries.R

# Packages ----------------------------------------------------------------

library(dplyr)
library(dbplyr)
library(RPostgreSQL)
library(stringr)

# Read credentials from file excluded in .gitignore --------------------------------

readRenviron("/tradestatistics/plumber-api")

# DB connection parameters ------------------------------------------------

pool <- pool::dbPool(
  drv = dbDriver("PostgreSQL"),
  dbname = Sys.getenv("dbname"),
  host = Sys.getenv("dbhost"),
  user = Sys.getenv("dbusr"),
  password = Sys.getenv("dbpwd")
)

# Clean inputs ------------------------------------------------------------

clean_char_input <- function(x, i, j) {
  y <- iconv(x, to = "ASCII//TRANSLIT", sub = " ")
  y <- str_replace_all(y, "[^[:alpha:]-]", " ")
  y <- str_squish(y)
  y <- str_trim(y)
  y <- str_to_lower(y)
  y <- str_sub(y, i, j)
  
  return(y)
}

clean_num_input <- function(x, i, j) {
  y <- iconv(x, to = "ASCII//TRANSLIT", sub = " ")
  y <- ifelse(y == "all", y, str_replace_all(y, "[^[:digit:]]", " "))
  y <- str_squish(y)
  y <- str_trim(y)
  y <- str_to_lower(y)
  y <- str_sub(y, i, j)
  
  return(y)
}

# Available years in the DB -----------------------------------------------

min_year <- function() {
  return(
    tbl(pool, in_schema("public", "hs07_yr")) %>% 
      select(year) %>% 
      summarise(year = min(year, na.rm = TRUE)) %>% 
      collect() %>% 
      as.numeric()
  )
}

max_year <- function() {
  return(
    tbl(pool, in_schema("public", "hs07_yr")) %>% 
      select(year) %>% 
      summarise(year = max(year, na.rm = TRUE)) %>% 
      collect() %>% 
      as.numeric()
  )
}

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

countries <- function() {
  d <- tbl(pool, in_schema("public", "attributes_countries")) %>% collect()
  d2 <- readr::read_csv("aliases/countries.csv")
  d <- bind_rows(d, d2)
  return(d)
}

#* Echo back the result of a query on attributes_countries table
#* @get /countries

function() { countries() }

# Continents (to filter API parameters) -----------------------------------

# create vectors by continent to filter by using meta variables like americas, africa, etc

## Africa
countries_africa <- countries() %>%
  filter(continent == "Africa") %>%
  select(country_iso) %>%
  as.vector()

## Americas
countries_americas <- countries() %>%
  filter(continent == "Americas") %>%
  select(country_iso) %>%
  as.vector()

## Asia
countries_asia <- countries() %>%
  filter(continent == "Asia") %>%
  select(country_iso) %>%
  as.vector()

## Europe
countries_europe <- countries() %>%
  filter(continent == "Europe") %>%
  select(country_iso) %>%
  as.vector()

## Oceania
countries_oceania <- countries() %>%
  filter(continent == "Oceania") %>%
  select(country_iso) %>%
  as.vector()

# Products ----------------------------------------------------------------

products <- function() {
  d <- tbl(pool, in_schema("public", "attributes_products")) %>% collect()
  d2 <- readr::read_csv("aliases/products.csv")
  d <- bind_rows(d, d2)
  return(d)
}

#* Echo back the result of a query on attributes_products table
#* @get /products

function() { products() }

# Communities -------------------------------------------------------------

communities <- function() {
  return(
    tbl(pool, in_schema("public", "attributes_communities")) %>% collect()
  )
}

#* Echo back the result of a query on attributes_communities table
#* @get /communities

function() { communities() }

# Product shortnames ------------------------------------------------------

products_shortnames <- function() {
  return(
    tbl(pool, in_schema("public", "attributes_products_shortnames")) %>% collect()
  )
}

#* Echo back the result of a query on attributes_products_shortnames table
#* @get /product_shortnames

function() { products_shortnames() }

# YC ----------------------------------------------------------------------

#* Echo back the result of a query on yc table
#* @param y Year
#* @param c Product code
#* @get /yc

function(y = NULL, c = "all") {
  y <- as.integer(y)
  c <- clean_num_input(c, 1, 4)
  
  if (nchar(y) != 4 | !y >= min_year() | !y <= max_year()) {
    return("The specified year is not a valid integer value. Read the documentation: tradestatistics.io")
    stop()
  }
  
  if (!nchar(c) <= 4 | !c %in% products()$product_code) {
    return("The specified product is not a valid string. Read the documentation: tradestatistics.io")
    stop()
  }
  
  query <- tbl(pool, in_schema("public", "hs07_yc")) %>% 
    filter(year == y)
  
  if (c != "all" & nchar(c) != 2) {
    query <- query %>% 
      filter(product_code == c)
  }
  
  if (c != "all" & nchar(c) == 2) {
    query <- query %>% 
      filter(substr(product_code, 1, 2) == c)
  }
  
  data <- query %>% collect()
  
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
  
  if (nchar(y) != 4 | !y >= min_year() | !y <= max_year()) {
    return("The specified year is not a valid integer value.")
    stop()
  }
  
  stopifnot(is.integer(y))
  
  data <- tbl(pool, in_schema("public", "hs07_yc")) %>% 
    select(year, product_code, pci_fitness_method, pci_rank_fitness_method) %>% 
    filter(year == y, pci_rank_fitness_method > 0) %>% 
    arrange(pci_rank_fitness_method) %>% 
    collect()
  
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
  
  if (nchar(y) != 4 | !y >= min_year() | !y <= max_year()) {
    return("The specified year is not a valid integer value. Read the documentation: tradestatistics.io")
    stop()
  }
  
  if (!nchar(r) <= 4 | !r %in% c(countries()$country_iso)) {
    return("The specified reporter is not a valid ISO code or alias. Read the documentation: tradestatistics.io")
    stop()
  }
  
  query <- tbl(pool, in_schema("public", "hs07_yr")) %>% 
    filter(year == y)
  
  if (r != "all" & nchar(r) == 3) {
    query <- query %>% 
      filter(reporter_iso == r)
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
    
    query <- query %>% 
      filter(reporter_iso %in% r_expanded_alias$country_iso)
  }
  
  data <- query %>% collect()
  
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
  
  if (nchar(y) != 4 | !y >= min_year() | !y <= max_year()) {
    return("The specified year is not a valid integer value. Read the documentation: tradestatistics.io")
    stop()
  }
  
  if (!nchar(r) <= 4 | !r %in% c(countries()$country_iso)) {
    return("The specified reporter is not a valid ISO code or alias. Read the documentation: tradestatistics.io")
    stop()
  }
  
  query <- tbl(pool, in_schema("public", "hs07_yr")) %>% 
    select(year, reporter_iso, export_value_usd, import_value_usd,
           top_export_product_code, top_export_trade_value_usd,
           top_import_product_code, top_import_trade_value_usd) %>% 
    filter(year == y)
  
  if (r != "all" & nchar(r) == 3) {
    query <- query %>% 
      filter(reporter_iso == r)
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
    
    query <- query %>% 
      filter(reporter_iso %in% r_expanded_alias$country_iso)
  }
  
  data <- query %>% collect()
  
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
  
  if (nchar(y) != 4 | !y >= min_year() | !y <= max_year()) {
    return("The specified year is not a valid integer value.")
    stop()
  }
  
  data <- tbl(pool, in_schema("public", "hs07_yr")) %>% 
    filter(year == y) %>% 
    select(reporter_iso) %>% 
    collect()
  
  return(data)
}

# Country rankings --------------------------------------------------------

#* Echo back the result of a query on yr table
#* @param y Year
#* @get /country_rankings

function(y = 2017) {
  y <- as.integer(y)
  
  if (nchar(y) != 4 | !y >= min_year() | !y <= max_year()) {
    return("The specified year is not a valid integer value.")
    stop()
  }
  
  stopifnot(is.integer(y))
  
  data <- tbl(pool, in_schema("public", "hs07_yr")) %>% 
    select(year, reporter_iso, eci_fitness_method, eci_rank_fitness_method) %>% 
    filter(year == y, eci_rank_fitness_method > 0) %>% 
    arrange(eci_rank_fitness_method) %>% 
    collect()
  
  return(data)
}

# YRC ---------------------------------------------------------------------

#* Echo back the result of a query on yrc table
#* @param y Year
#* @param r Reporter ISO
#* @param c Product code
#* @get /yrc

function(y = NULL, r = NULL, c = "all") {
  y <- as.integer(y)
  r <- clean_char_input(r, 1, 4)
  c <- clean_num_input(c, 1, 4)
  
  if (nchar(y) != 4 | !y >= min_year() | !y <= max_year()) {
    return("The specified year is not a valid integer value. Read the documentation: tradestatistics.io")
    stop()
  }
  
  if (!nchar(r) <= 4 | !r %in% c(countries()$country_iso)) {
    return("The specified reporter is not a valid ISO code or alias. Read the documentation: tradestatistics.io")
    stop()
  }
  
  if (!nchar(c) <= 4 | !c %in% products()$product_code) {
    return("The specified product is not a valid string. Read the documentation: tradestatistics.io")
    stop()
  }
  
  query <- tbl(pool, in_schema("public", "hs07_yrc")) %>% 
    filter(year == y)

  if (r != "all" & nchar(r) == 3) {
    query <- query %>% 
      filter(reporter_iso == r)
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
    
    query <- query %>% 
      filter(reporter_iso %in% r_expanded_alias$country_iso)
  }
  
  if (c != "all" & nchar(c) != 2) {
    query <- query %>% 
      filter(product_code == c)
  }
  
  if (c != "all" & nchar(c) == 2) {
    query <- query %>% 
      filter(substr(reporter_iso, 1, 2) == c)
  }
  
  data <- query %>% collect()
  
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
  
  if (nchar(y) != 4 | !y >= min_year() | !y <= max_year()) {
    return("The specified year is not a valid integer value. Read the documentation: tradestatistics.io")
    stop()
  }
  
  if (!nchar(r) <= 4 | !r %in% c(countries()$country_iso)) {
    return("The specified reporter is not a valid ISO code or alias. Read the documentation: tradestatistics.io")
    stop()
  }
  
  if (!nchar(p) <= 4 | !p %in% c(countries()$country_iso)) {
    return("The specified partner is not a valid ISO code or alias. Read the documentation: tradestatistics.io")
    stop()
  }
  
  query <- tbl(pool, in_schema("public", "hs07_yrp")) %>% 
    filter(year == y)

  if (r != "all" & nchar(r) == 3) {
    query <- query %>% 
      filter(reporter_iso == r)
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
    
    query <- query %>% 
      filter(reporter_iso %in% r_expanded_alias$country_iso)
  }
  
  if (p != "all" & nchar(p) == 3) {
    query <- query %>% 
      filter(partner_iso == p)
  }
  
  if (p != "all" & nchar(p) == 4) {
    p_expanded_alias <- switch(
      p,
      "c-af" = countries_africa,
      "c-am" = countries_americas,
      "c-as" = countries_asia,
      "c-eu" = countries_europe,
      "c-oc" = countries_oceania
    )
    
    query <- query %>% 
      filter(partner_iso %in% p_expanded_alias$country_iso)
  }
  
  data <- query %>% collect()
  
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
#* @get /yrpc

function(y = NULL, r = NULL, p = NULL, c = "all") {
  y <- as.integer(y)
  r <- clean_char_input(r, 1, 4)
  p <- clean_char_input(p, 1, 4)
  c <- clean_num_input(c, 1, 4)
  
  if (nchar(y) != 4 | !y >= min_year() | !y <= max_year()) {
    return("The specified year is not a valid integer value. Read the documentation: tradestatistics.io")
    stop()
  }
  
  if (!nchar(r) <= 4 | !r %in% c(countries()$country_iso)) {
    return("The specified reporter is not a valid ISO code or alias. Read the documentation: tradestatistics.io")
    stop()
  }
  
  if (!nchar(p) <= 4 | !p %in% c(countries()$country_iso)) {
    return("The specified partner is not a valid ISO code or alias. Read the documentation: tradestatistics.io")
    stop()
  }
  
  if (!nchar(c) <= 4 | !c %in% products()$product_code) {
    return("The specified product is not a valid string. Read the documentation: tradestatistics.io")
    stop()
  }
  
  query <- tbl(pool, in_schema("public", "hs07_yrpc")) %>% 
    filter(year == y)
  
  if (r != "all" & nchar(r) == 3) {
    query <- query %>% 
      filter(reporter_iso == r)
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
    
    query <- query %>% 
      filter(reporter_iso %in% r_expanded_alias$country_iso)
  }
  
  if (p != "all" & nchar(p) == 3) {
    query <- query %>% 
      filter(partner_iso == p)
  }
  
  if (p != "all" & nchar(p) == 4) {
    p_expanded_alias <- switch(
      p,
      "c-af" = countries_africa,
      "c-am" = countries_americas,
      "c-as" = countries_asia,
      "c-eu" = countries_europe,
      "c-oc" = countries_oceania
    )
    
    query <- query %>% 
      filter(partner_iso %in% p_expanded_alias$country_iso)
  }
  
  if (c != "all" & nchar(c) != 2) {
    query <- query %>% 
      filter(product_code == c)
  }
  
  if (c != "all" & nchar(c) == 2) {
    query <- query %>% 
      filter(substr(product_code, 1, 2) == c)
  }
  
  data <- query %>% collect()
  
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
  tibble(
    table = c(
      "countries",
      "products",
      "reporters",
      "communities",
      "product_shortnames",
      "country_rankings",
      "product_rankings",
      "yrpc",
      "yrp",
      "yrc",
      "yr",
      "yc"
    ),
    description = c(
      "Countries metadata",
      "Product metadata",
      "Reporting countries",
      "Product communities",
      "Product short names",
      "Ranking of countries",
      "Ranking of products",
      "Bilateral trade at product level (Year, Reporter, Partner and Product Code)",
      "Reporter trade at aggregated level (Year, Reporter and Partner)",
      "Reporter trade at aggregated level (Year, Reporter and Product Code)",
      "Reporter trade at aggregated level (Year and Reporter)",
      "Product trade at aggregated level (Year and Product Code)"
    ),
    source = c(
      rep("UN Comtrade",3),
      "Center for International Development at Harvard University",
      "The Observatory of Economic Complexity (with modifications)",
      rep("Open Trade Statistics",7)
    )
  )
}

# Year range --------------------------------------------------------------

#* All the tables generated by this API
#* @get /year_range

function() {
  tibble(year = c(min_year(), max_year()))
}
