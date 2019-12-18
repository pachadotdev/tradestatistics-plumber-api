# queries.R

# Packages ----------------------------------------------------------------

library(dplyr)
library(RPostgreSQL)

# Read credentials from file in .gitignore --------------------------------

readRenviron("/apis/tradestatistics")

# DB connection parameters ------------------------------------------------

pool <- pool::dbPool(
  drv = dbDriver("PostgreSQL"),
  dbname = Sys.getenv("dbname"),
  host = Sys.getenv("dbhost"),
  user = Sys.getenv("dbusr"),
  password = Sys.getenv("dbpwd")
)

# pool <- pool::dbPool(
#   drv = dbDriver("PostgreSQL"),
#   dbname = "",
#   host = "",
#   user = "",
#   password = ""
# )

# Clean inputs ------------------------------------------------------------

clean_char_input <- function(x, i, j) {
  y <- iconv(x, to = "ASCII//TRANSLIT", sub = " ")
  y <- stringr::str_replace_all(y, "[^[:alpha:]-]", " ")
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

# Caching -----------------------------------------------------------------

dbGetQuery_cached <- memoise::memoise(dbGetQuery, cache = memoise::cache_filesystem("cache"))

# Available years in the DB -----------------------------------------------

min_year <- function() {
  return(
    dbGetQuery_cached(pool, glue::glue_sql("SELECT MIN(year) FROM public.hs07_yr")) %>% 
      as.numeric()
  )
}

max_year <- function() {
  return(
    dbGetQuery_cached(pool, glue::glue_sql("SELECT MAX(year) FROM public.hs07_yr")) %>% 
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
  return(
    dbGetQuery_cached(pool, glue::glue_sql("SELECT * FROM public.attributes_countries"))
  )
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
  return(
    dbGetQuery_cached(pool, glue::glue_sql("SELECT * FROM public.attributes_products"))
  )
}

#* Echo back the result of a query on attributes_products table
#* @get /products

function() { products() }

# Communities -------------------------------------------------------------

communities <- function() {
  return(
    dbGetQuery_cached(pool, glue::glue_sql("SELECT * FROM public.attributes_communities"))
  )
}

#* Echo back the result of a query on attributes_communities table
#* @get /communities

function() { communities() }

# Product shortnames ------------------------------------------------------

products_shortnames <- function() {
  return(
    dbGetQuery_cached(pool, glue::glue_sql("SELECT * FROM public.attributes_products_shortnames"))
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
  
  query <- glue::glue_sql(
    "
    SELECT *
    FROM public.hs07_yc
    WHERE year = {y}
    ",
    .con = pool
  )
  
  if (c != "all" & nchar(c) != 2) {
    query <- glue::glue_sql(
      query,
      " AND product_code = {c}",
      .con = pool
    )
  }
  
  if (c != "all" & nchar(c) == 2) {
    query <- glue::glue_sql(
      query,
      " AND LEFT(product_code, 2) = {c}",
      .con = pool
    )
  }
  
  data <- dbGetQuery_cached(pool, query)
  
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
  
  query <- glue::glue_sql(
    "
    SELECT year, product_code, pci_fitness_method, pci_rank_fitness_method
    FROM public.hs07_yc
    WHERE year = {y}
    ",
    .con = pool
  )
  
  data <- dbGetQuery_cached(pool, query)
  
  data <- data %>%
    filter(pci_rank_fitness_method > 0) %>% 
    arrange(pci_rank_fitness_method)
  
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
  
  query <- glue::glue_sql(
    "
    SELECT * 
    FROM public.hs07_yr 
    WHERE year = {y}
    ",
    .con = pool
  )
  
  if (r != "all" & nchar(r) == 3) {
    query <- glue::glue_sql(
      query,
      " AND reporter_iso = {r}",
      .con = pool
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
    
    query <- glue::glue_sql(
      query,
      " AND reporter_iso IN ({vals*})",
      vals = r_expanded_alias$country_iso,
      .con = pool
    )
  }
  
  data <- dbGetQuery_cached(pool, query)
  
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
  
  query <- glue::glue_sql(
    "
    SELECT year, reporter_iso, export_value_usd, import_value_usd,
      top_export_product_code, top_export_trade_value_usd,
      top_import_product_code, top_import_trade_value_usd
    FROM public.hs07_yr 
    WHERE year = {y}
    ",
    .con = pool
  )
  
  if (r != "all" & nchar(r) == 3) {
    query <- glue::glue_sql(
      query,
      " AND reporter_iso = {r}",
      .con = pool
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
    
    query <- glue::glue_sql(
      query,
      " AND reporter_iso IN ({vals*})",
      vals = r_expanded_alias$country_iso,
      .con = pool
    )
  }
  
  data <- dbGetQuery_cached(pool, query)
  
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
  
  query <- glue::glue_sql(
    "
    SELECT reporter_iso
    FROM public.hs07_yr
    WHERE year = {y}
    ",
    .con = pool
  )
  
  data <- dbGetQuery_cached(pool, query)
  
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
  
  query <- glue::glue_sql(
    "
    SELECT year, reporter_iso, eci_fitness_method, eci_rank_fitness_method
    FROM public.hs07_yr
    WHERE year = {y}
    ",
    .con = pool
  )
  
  data <- dbGetQuery_cached(pool, query)
  
  data <- data %>%
    filter(eci_rank_fitness_method > 0) %>% 
    arrange(eci_rank_fitness_method)
  
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
  
  query <- glue::glue_sql(
    "
    SELECT *
    FROM public.hs07_yrc
    WHERE year = {y}
    ",
    .con = pool
  )
  
  if (r != "all" & nchar(r) == 3) {
    query <- glue::glue_sql(
      query,
      " AND reporter_iso = {r}",
      .con = pool
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
    
    query <- glue::glue_sql(
      query,
      " AND reporter_iso IN ({vals*})",
      vals = r_expanded_alias$country_iso,
      .con = pool
    )
  }
  
  if (c != "all" & nchar(c) != 2) {
    query <- glue::glue_sql(
      query,
      " AND product_code = {c}",
      .con = pool
    )
  }
  
  if (c != "all" & nchar(c) == 2) {
    query <- glue::glue_sql(
      query,
      " AND LEFT(product_code, 2) = {c}",
      .con = pool
    )
  }
  
  data <- dbGetQuery_cached(pool, query)
  
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
  
  query <- glue::glue_sql("
                    SELECT *
                    FROM public.hs07_yrp
                    WHERE year = {y}
                    ", .con = pool)
  
  if (r != "all" & nchar(r) == 3) {
    query <- glue::glue_sql(
      query,
      " AND reporter_iso = {r}",
      .con = pool
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
    
    query <- glue::glue_sql(
      query,
      " AND reporter_iso IN ({vals*})",
      vals = r_expanded_alias$country_iso,
      .con = pool
    )
  }
  
  if (p != "all" & nchar(p) == 3) {
    query <- glue::glue_sql(
      query,
      " AND partner_iso = {p}",
      .con = pool
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
    
    query <- glue::glue_sql(
      query,
      " AND partner_iso IN ({vals*})",
      vals = p2$country_iso,
      .con = pool
    )
  }
  
  data <- dbGetQuery_cached(pool, query)
  
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

# YRP short ---------------------------------------------------------------

#* Echo back the result of a query on yrp table
#* @param y Year
#* @param r Reporter ISO
#* @param p Partner ISO
#* @get /yrp_short

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
  
  query <- glue::glue_sql("
                    SELECT year, reporter_iso, partner_iso, export_value_usd, import_value_usd
                    FROM public.hs07_yrp
                    WHERE year = {y}
                    ", .con = pool)
  
  if (r != "all" & nchar(r) == 3) {
    query <- glue::glue_sql(
      query,
      " AND reporter_iso = {r}",
      .con = pool
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
    
    query <- glue::glue_sql(
      query,
      " AND reporter_iso IN ({vals*})",
      vals = r_expanded_alias$country_iso,
      .con = pool
    )
  }
  
  if (p != "all" & nchar(p) == 3) {
    query <- glue::glue_sql(
      query,
      " AND partner_iso = {p}",
      .con = pool
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
    
    query <- glue::glue_sql(
      query,
      " AND partner_iso IN ({vals*})",
      vals = p2$country_iso,
      .con = pool
    )
  }
  
  data <- dbGetQuery_cached(pool, query)
  
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
  
  query <- glue::glue_sql(
    "
    SELECT *
    FROM public.hs07_yrpc
    WHERE year = {y}
    ",
    .con = pool
  )
  
  if (r != "all" & nchar(r) == 3) {
    query <- glue::glue_sql(
      query,
      " AND reporter_iso = {r}",
      .con = pool
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
    
    query <- glue::glue_sql(
      query,
      " AND reporter_iso IN ({vals*})",
      vals = r_expanded_alias$country_iso,
      .con = pool
    )
  }
  
  if (p != "all" & nchar(p) == 3) {
    query <- glue::glue_sql(
      query,
      " AND partner_iso = {p}",
      .con = pool
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
    
    query <- glue::glue_sql(
      query,
      " AND partner_iso IN ({vals*})",
      vals = p2$country_iso,
      .con = pool
    )
  }
  
  if (c != "all" & nchar(c) != 2) {
    query <- glue::glue_sql(
      query,
      " AND product_code = {c}",
      .con = pool
    )
  }
  
  if (c != "all" & nchar(c) == 2) {
    query <- glue::glue_sql(
      query,
      " AND LEFT(product_code, 2) = {c}",
      .con = pool
    )
  }
  
  data <- dbGetQuery_cached(pool, query)
  
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
  tibble::tibble(
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
      "yrp_short",
      "yrc",
      "yrc_exports",
      "yrc_imports",
      "yr",
      "yr_short",
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
      "Reporter trade at aggregated level (Year, Reporter and Partner), imports and exports only",
      "Reporter trade at aggregated level (Year, Reporter and Product Code)",
      "Reporter trade at aggregated level (Year, Reporter and Product Code), exports only",
      "Reporter trade at aggregated level (Year, Reporter and Product Code), imports only",
      "Reporter trade at aggregated level (Year and Reporter)",
      "Reporter trade at aggregated level (Year and Reporter), imports and exports only",
      "Product trade at aggregated level (Year and Product Code)"
    ),
    source = c(
      rep("UN Comtrade",3),
      "Center for International Development at Harvard University",
      "The Observatory of Economic Complexity (with modifications)",
      rep("Open Trade Statistics",11)
    )
  )
}

# Year range --------------------------------------------------------------

#* All the tables generated by this API
#* @get /year_range

function() {
  tibble(year = c(min_year(), max_year()))
}
