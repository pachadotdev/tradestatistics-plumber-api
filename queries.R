# queries.R

# Packages ----------------------------------------------------------------

library(dplyr)
library(glue)
library(RPostgreSQL)

# Read credentials from file in .gitignore --------------------------------

readRenviron("/api")

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
  select(country_iso) %>% 
  as.vector()

## Americas
countries_americas <- countries_data %>% 
  filter(continent == "Americas") %>% 
  select(country_iso) %>% 
  as.vector()

## Asia
countries_asia <- countries_data %>% 
  filter(continent == "Asia") %>% 
  select(country_iso) %>% 
  as.vector()

## Europe
countries_europe <- countries_data %>% 
  filter(continent == "Europe") %>% 
  select(country_iso) %>% 
  as.vector()

## Oceania
countries_oceania <- countries_data %>% 
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

# List of products (to filter API parameters) -----------------------------

products_query <- glue_sql(
  "
  SELECT *
  FROM public.attributes_products
  ",
  .con = con
)

products_data <- dbGetQuery(con, products_query)

groups_data <- products_data %>% 
  select(group_code, group_name) %>% 
  distinct() %>% 
  mutate(group_name = paste("Alias for all codes in the group", group_name)) %>% 
  add_row(group_code = "all", group_name = "Alias for all codes") %>% 
  rename(
    commodity_code = group_code,
    product_fullname_english = group_name
  )

# List of communities (for Data Visualization) ----------------------------

communities_query <- glue_sql(
  "
  SELECT *
  FROM public.attributes_communities
  ",
  .con = con
)

communities_data <- dbGetQuery(con, communities_query)

# Hello World -------------------------------------------------------------

#* Echo back Hello World!
#* @get /

function() {
  paste("Hello World! This is Open Trade Statistics' API")
}

# Countries ---------------------------------------------------------------

#* Echo back the result of a query on attributes_countries table
#* @get /countries

function() {
  countries_data %>% 
    bind_rows(continents_data)
}

# Countries short (for Shiny) ---------------------------------------------

# not currently in use
# #* Echo back the result of a query on attributes_countries table
# #* @get /countries_short
# 
# function() {
#   countries_data %>%
#     select(country_iso, country_name_english, country_fullname_english)
# }

# Products ----------------------------------------------------------------

#* Echo back the result of a query on attributes_countries table
#* @get /products

function() {
  products_data %>% 
    bind_rows(groups_data)
}

# Communities -------------------------------------------------------------

#* Echo back the result of a query on attributes_countries table
#* @get /communities

function() {
  communities_data
}

# YC ----------------------------------------------------------------------

#* Echo back the result of a query on yc table
#* @param y Year
#* @param c Commodity code
#* @param l Commodity code length
#* @get /yc

function(y = NULL, c = "all", l = 4) {
  y <- as.integer(y)
  c <- tolower(substr(as.character(c), 1, 6))
  l <- tolower(substr(as.character(l), 1, 3))
  
  if (nchar(c) == 2) { l <- "all" }
  
  if (nchar(y) != 4 | !y %in% 1962:2016) {
    return("The specified year is not a valid integer value. Read the documentation: tradestatistics.github.io/documentation")
    stop()
  }
  
  if (!nchar(c) <= 6 | !c %in% c(products_data$commodity_code, groups_data$commodity_code)) {
    return("The specified commodity is not a valid string. Read the documentation: tradestatistics.github.io/documentation")
    stop()
  }
  
  if (!nchar(l) <= 3 | !l %in% c(4, 6, "all")) {
    return("The specified length is not a valid integer value. Read the documentation: tradestatistics.github.io/documentation")
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
      " AND commodity_code = {c}",
      .con = con
    )
  }
  
  if (c != "all" & nchar(c) == 2) {
    query <- glue_sql(
      query, 
      " AND LEFT(commodity_code, 2) = {c}",
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
  
  if (nrow(data) == 0) {
    data <- tibble(
      year = y,
      commodity_code = c,
      observation = "No data available for these filtering parameters"
    )
  }
  
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
  r <- tolower(substr(as.character(r), 1, 4))
  
  if (nchar(y) != 4 | !y %in% 1962:2016) {
    return("The specified year is not a valid integer value. Read the documentation: tradestatistics.github.io/documentation")
    stop()
  }
  
  if (!nchar(r) <= 4 | !r %in% c(countries_data$country_iso, continents_data$country_iso)) {
    return("The specified reporter is not a valid ISO code or alias. Read the documentation: tradestatistics.github.io/documentation")
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
    r2 <- switch(
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
      vals = r2$country_iso,
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

# YR short (for Shiny) ----------------------------------------------------

#* Echo back the result of a query on yr table
#* @param y Year
#* @param r Reporter ISO
#* @get /yr_short

function(y = NULL, r = NULL) {
  y <- as.integer(y)
  r <- tolower(substr(as.character(r), 1, 4))
  
  if (nchar(y) != 4 | !y %in% 1962:2016) {
    return("The specified year is not a valid integer value. Read the documentation: tradestatistics.github.io/documentation")
    stop()
  }
  
  if (!nchar(r) <= 4 | !r %in% c(countries_data$country_iso, continents_data$country_iso)) {
    return("The specified reporter is not a valid ISO code or alias. Read the documentation: tradestatistics.github.io/documentation")
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
    r2 <- switch(
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
      vals = r2$country_iso,
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
#* @param c Commodity code
#* @param l Commodity code length
#* @get /yrc

function(y = NULL, r = NULL, c = "all", l = 4) {
  y <- as.integer(y)
  r <- tolower(substr(as.character(r), 1, 4))
  c <- tolower(substr(as.character(c), 1, 6))
  l <- tolower(substr(as.character(l), 1, 3))
  
  if (nchar(c) == 2) { l <- "all" }
  
  if (nchar(y) != 4 | !y %in% 1962:2016) {
    return("The specified year is not a valid integer value. Read the documentation: tradestatistics.github.io/documentation")
    stop()
  }
  
  if (!nchar(r) <= 4 | !r %in% c(countries_data$country_iso, continents_data$country_iso)) {
    return("The specified reporter is not a valid ISO code or alias. Read the documentation: tradestatistics.github.io/documentation")
    stop()
  }
  
  if (!nchar(c) <= 6 | !c %in% c(products_data$commodity_code, groups_data$commodity_code)) {
    return("The specified commodity is not a valid string. Read the documentation: tradestatistics.github.io/documentation")
    stop()
  }
  
  if (!nchar(l) <= 3 | !l %in% c(4, 6, "all")) {
    return("The specified length is not a valid integer value. Read the documentation: tradestatistics.github.io/documentation")
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
    r2 <- switch(
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
      vals = r2$country_iso,
      .con = con
    )
  }
  
  if (c != "all" & nchar(c) != 2) {
    query <- glue_sql(
      query, 
      " AND commodity_code = {c}",
      .con = con
    )
  }
  
  if (c != "all" & nchar(c) == 2) {
    query <- glue_sql(
      query, 
      " AND LEFT(commodity_code, 2) = {c}",
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
  
  if (nrow(data) == 0) {
    data <- tibble(
      year = y,
      reporter_iso = r,
      commodity_code = c,
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
#* @param l Commodity code length
#* @get /yrp

function(y = NULL, r = NULL, p = NULL, l = 4) {
  y <- as.integer(y)
  r <- tolower(substr(as.character(r), 1, 4))
  p <- tolower(substr(as.character(p), 1, 4))
  
  if (nchar(y) != 4 | !y %in% 1962:2016) {
    return("The specified year is not a valid integer value. Read the documentation: tradestatistics.github.io/documentation")
    stop()
  }
  
  if (!nchar(r) <= 4 | !r %in% c(countries_data$country_iso, continents_data$country_iso)) {
    return("The specified reporter is not a valid ISO code or alias. Read the documentation: tradestatistics.github.io/documentation")
    stop()
  }
  
  if (!nchar(p) <= 4 | !p %in% c(countries_data$country_iso, continents_data$country_iso)) {
    return("The specified partner is not a valid ISO code or alias. Read the documentation: tradestatistics.github.io/documentation")
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
    r2 <- switch(
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
      vals = r2$country_iso,
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
#* @param c Commodity code
#* @param l Commodity code length
#* @get /yrpc

function(y = NULL, r = NULL, p = NULL, c = "all", l = 4) {
  y <- as.integer(y)
  r <- tolower(substr(as.character(r), 1, 4))
  p <- tolower(substr(as.character(p), 1, 4))
  c <- tolower(substr(as.character(c), 1, 6))
  l <- tolower(substr(as.character(l), 1, 3))
  
  if (nchar(c) == 2) { l <- "all" }
  
  if (nchar(y) != 4 | !y %in% 1962:2016) {
    return("The specified year is not a valid integer value. Read the documentation: tradestatistics.github.io/documentation")
    stop()
  }
  
  if (!nchar(r) <= 4 | !r %in% c(countries_data$country_iso, continents_data$country_iso)) {
    return("The specified reporter is not a valid ISO code or alias. Read the documentation: tradestatistics.github.io/documentation")
    stop()
  }
  
  if (!nchar(p) <= 4 | !p %in% c(countries_data$country_iso, continents_data$country_iso)) {
    return("The specified partner is not a valid ISO code or alias. Read the documentation: tradestatistics.github.io/documentation")
    stop()
  }
  
  if (!nchar(c) <= 6 | !c %in% c(products_data$commodity_code, groups_data$commodity_code)) {
    return("The specified commodity is not a valid string. Read the documentation: tradestatistics.github.io/documentation")
    stop()
  }
  
  if (!nchar(l) <= 3 | !l %in% c(4, 6, "all")) {
    return("The specified length is not a valid integer value. Read the documentation: tradestatistics.github.io/documentation")
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
    r2 <- switch(
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
      vals = r2$country_iso,
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
      " AND commodity_code = {c}",
      .con = con
    )
  }
  
  if (c != "all" & nchar(c) == 2) {
    query <- glue_sql(
      query, 
      " AND LEFT(commodity_code, 2) = {c}",
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
  
  if (nrow(data) == 0) {
    data <- tibble(
      year = y,
      reporter_iso = r,
      partner_iso = p,
      commodity_code = c,
      observation = "No data available for these filtering parameters"
    )
  }
  
  return(data)
}
