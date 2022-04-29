# queries.R

library(dplyr)

# Read credentials from file excluded in .gitignore
readRenviron("/tradestatistics/plumber-api")

con <- pool::dbPool(
  drv = RPostgres::Postgres(),
  dbname = "tradestatistics",
  host = "localhost",
  user = Sys.getenv("TRADESTATISTICS_SQL_USR"),
  password = Sys.getenv("TRADESTATISTICS_SQL_PWD")
)

# Static data -------------------------------------------------------------

countries <- function() {
  return(
    tbl(con, "countries") %>% 
      collect()
  )
}

commodities <- function() {
  return(
    tbl(con, "commodities") %>% 
      collect()
  )
}

commodities_short <- function() {
  return(
    tbl(con, "commodities_short") %>% 
      collect()
  )
}

distances <- function() {
  return(
    tbl(con, "distances") %>% 
      collect()
  )
}

sections <- function() {
  return(
    tbl(con, "sections") %>% 
      collect()
  )
}

sections_colors <- function() {
  return(
    tbl(con, "sections_colors") %>% 
      collect()
  )
}

# create vectors by continent to filter by using meta variables like americas, africa, etc

## Africa
countries_africa <- countries() %>% 
  filter(continent_name_english == "Africa") %>% 
  select(country_iso) %>% 
  pull()

## Americas
countries_americas <- countries() %>% 
  filter(continent_name_english == "Americas") %>% 
  select(country_iso) %>% 
  pull()

## Asia
countries_asia <- countries() %>% 
  filter(continent_name_english == "Asia") %>% 
  select(country_iso) %>% 
  pull()

## Europe
countries_europe <- countries() %>% 
  filter(continent_name_english == "Europe") %>% 
  select(country_iso) %>% 
  pull()

## Oceania
countries_oceania <- countries() %>% 
  filter(continent_name_english == "Oceania") %>% 
  select(country_iso) %>% 
  pull()

# Clean inputs ------------------------------------------------------------

clean_char_input <- function(x, i, j) {
  y <- iconv(x, to = "ASCII//TRANSLIT", sub = "")
  y <- if (grepl("^e-", y)) { gsub("[^[:alpha:][:digit:]]-", "", y) } else { gsub("[^[:alpha:]]-", "", y) }
  y <- tolower(y)
  y <- substr(y, i, j)
  return(y)
}

clean_num_input <- function(x, i, j) {
  y <- iconv(x, to = "ASCII//TRANSLIT", sub = "")
  y <- ifelse(y == "all", y, gsub("[^[:digit:]]", "", y))
  y <- tolower(y)
  y <- substr(y, i, j)
  return(y)
}

# Checks ------------------------------------------------------------------

check_year <- function(y) {
  if (nchar(y) != 4 | !y >= min_year() | !y <= max_year()) {
    return("The specified year is not a valid integer value. Read the documentation: tradestatistics.io")
  } else {
    return(y)
  }
}

check_reporter <- function(r) {
  if (!nchar(r) <= 5 | !r %in% c(countries()$country_iso)) {
    return("The specified reporter is not a valid ISO code or alias. Read the documentation: tradestatistics.io")
  } else {
    return(r)
  }
}

check_partner <- function(p) {
  if (!nchar(p) <= 5 | !p %in% c(countries()$country_iso)) {
    return("The specified partner is not a valid ISO code or alias. Read the documentation: tradestatistics.io")
  } else {
    return(p)
  }
}

check_commodity <- function(c) {
  if (!nchar(c) <= 6 | !c %in% c(commodities()$commodity_code)) {
    return("The specified commodity code is not a valid string. Read the documentation: tradestatistics.io")
  } else {
    return(c)
  }
}

check_section <- function(s) {
  if (!nchar(s) <= 3 | !s %in% c(sections()$section_code)) {
    return("The specified section code is not a valid string. Read the documentation: tradestatistics.io")
  } else {
    return(s)
  }
}

# Helpers -----------------------------------------------------------------

multiple_reporters <- function(r) {
  r2 <- switch(
    r,
    "c-af" = countries_africa,
    "c-am" = countries_americas,
    "c-as" = countries_asia,
    "c-eu" = countries_europe,
    "c-oc" = countries_oceania
  )
  
  return(r2)
}

multiple_partners <- function(p) {
  switch(
    p,
    "c-af" = countries_africa,
    "c-am" = countries_americas,
    "c-as" = countries_asia,
    "c-eu" = countries_europe,
    "c-oc" = countries_oceania
  )
}

no_data <- function(table, y = NA, r = NA, p = NA, c = NA, s = NA) {
  if (table == "yrpc") {
    d <- tibble(
      year = y,
      reporter_iso = r,
      partner_iso = p,
      commodity_code = c,
      observation = "No data available for these filtering parameters"
    )
  }
  
  if (table == "yrp") {
    d <- tibble(
      year = y,
      reporter_iso = r,
      partner_iso = p,
      observation = "No data available for these filtering parameters"
    )
  }
  
  if (any(table %in% c("yrc","tariffs"))) {
    d <- tibble(
      year = y,
      reporter_iso = r,
      commodity_code = c,
      observation = "No data available for these filtering parameters"
    )
  }
  
  if (table == "yr") {
    d <- tibble(
      year = y,
      reporter_iso = r,
      observation = "No data available for these filtering parameters"
    )
  }
  
  if (table == "yc") {
    d <- tibble(
      year = y,
      commodity_code = c,
      observation = "No data available for these filtering parameters"
    )
  }
  
  if (table == "ysrpc") {
    d <- tibble(
      year = y,
      section_code = s,
      observation = "No data available for these filtering parameters"
    )
  }
  
  if (table == "rtas") {
    d <- tibble(
      year = y,
      observation = "No data available for these filtering parameters"
    )
  }
  
  return(d)
}

# Data functions ----------------------------------------------------------

yc <- function(y, c, d) {
  y <- as.integer(y)
  c <- clean_num_input(c, 1, 6)
  
  y <- check_year(y)
  c <- check_commodity(c)
  
  query <- d %>% 
    filter(year == y)
  
  if (c != "all" & nchar(c) != 2) {
    query <- query %>% 
      filter(commodity_code == c)
  }
  
  if (c != "all" & nchar(c) == 2) {
    query <- query %>% 
      filter(substr(commodity_code, 1, 2) == c)
  }
  
  data <- query %>% 
    collect()
  
  if (nrow(data) == 0) {
    data <- no_data("yc", y = y, c = c)
  }
  
  return(data)
}

yr <- function(y, r, d) {
  y <- as.integer(y)
  r <- clean_char_input(r, 1, 5)
  
  y <- check_year(y)
  r <- check_reporter(r)
  
  query <- d %>% 
    filter(year == y)
  
  if (r != "all" & (nchar(r) == 3 | grepl("e-", r))) {
    query <- query %>% 
      filter(reporter_iso == r)
  }
  
  if (r != "all" & nchar(r) == 4) {
    r2 <- multiple_reporters(r)
    
    query <- query %>% 
      filter(reporter_iso %in% r2)
  }
  
  data <- query %>% 
    collect()
  
  if (nrow(data) == 0) {
    data <- no_data("yr", y = y, r = r)
  }
  
  return(data)
}

yrc <- function(y, r, c, d) {
  y <- as.integer(y)
  r <- clean_char_input(r, 1, 5)
  c <- clean_num_input(c, 1, 6)
  
  y <- check_year(y)
  r <- check_reporter(r)
  c <- check_commodity(c)
  
  query <- d %>% 
    filter(year == y)
  
  if (r != "all" & (nchar(r) == 3 | grepl("e-", r))) {
    query <- query %>% 
      filter(reporter_iso == r)
  }
  
  if (r != "all" & nchar(r) == 4) {
    r2 <- multiple_reporters(r)
    
    query <- query %>% 
      filter(reporter_iso %in% r2)
  }
  
  if (c != "all" & nchar(c) != 2) {
    query <- query %>% 
      filter(commodity_code == c)
  }
  
  if (c != "all" & nchar(c) == 2) {
    query <- query %>% 
      filter(substr(commodity_code, 1, 2) == c)
  }
  
  data <- query %>% 
    collect()
  
  if (nrow(data) == 0) {
    data <- no_data("yrc", y = y, r = r, c = c)
  }
  
  return(data)
}

yrp <- function(y, r, p, d) {
  y <- as.integer(y)
  r <- clean_char_input(r, 1, 5)
  p <- clean_char_input(p, 1, 5)
  
  y <- check_year(y)
  r <- check_reporter(r)
  p <- check_partner(p)
  
  query <- d %>% 
    filter(year == y)
  
  if (r != "all" & (nchar(r) == 3 | grepl("e-", r))) {
    query <- query %>% 
      filter(reporter_iso == r)
  }
  
  if (r != "all" & nchar(r) == 4) {
    r2 <- multiple_reporters(r)
    
    query <- query %>% 
      filter(reporter_iso %in% r2)
  }
  
  if (p != "all" & (nchar(p) == 3 | grepl("e-", p))) {
    query <- query %>% 
      filter(partner_iso == p)
  }
  
  if (p != "all" & nchar(p) == 4) {
    p2 <- multiple_partners(p)
    
    query <- query %>% 
      filter(partner_iso %in% p2)
  }
  
  data <- query %>% 
    collect()
  
  if (nrow(data) == 0) {
    data <- no_data("yrp", y = y, r = r, p = p)
  }
  
  return(data)
}

yrpc <- function(y, r, p, c, d) {
  y <- as.integer(y)
  r <- clean_char_input(r, 1, 5)
  p <- clean_char_input(p, 1, 5)
  c <- clean_num_input(c, 1, 6)
  
  y <- check_year(y)
  r <- check_reporter(r)
  p <- check_partner(p)
  c <- check_commodity(c)
  
  if (r == "all" & p == "all") {
    data <- tibble(
      year = y,
      reporter_iso = r,
      partner_iso = p,
      commodity_code = c,
      observation = "You are better off downloading the compressed datasets from docs.tradestatistics.io/accesing-the-data.html"
    )
    
    return(data)
  }
  
  query <- d %>% 
    filter(year == y)
  
  if (r != "all" & (nchar(r) == 3 | grepl("e-", r))) {
    query <- query %>% 
      filter(reporter_iso == r)
  }
  
  if (r != "all" & nchar(r) == 4) {
    r2 <- multiple_reporters(r)
    
    query <- query %>% 
      filter(reporter_iso %in% r2)
  }
  
  if (p != "all" & (nchar(p) == 3 | grepl("e-", p))) {
    query <- query %>% 
      filter(partner_iso == p)
  }
  
  if (p != "all" & nchar(p) == 4) {
    p2 <- multiple_partners(p)
    
    query <- query %>% 
      filter(partner_iso %in% p2)
  }
  
  if (c != "all" & nchar(c) != 2) {
    query <- query %>% 
      filter(commodity_code == c)
  }
  
  if (c != "all" & nchar(c) == 2) {
    query <- query %>% 
      filter(substr(commodity_code, 1, 2) == c)
  }
  
  data <- query %>% 
    collect()
  
  if (nrow(data) == 0) {
    data <- no_data("yrpc", y = y, r = r, p = p, c = c)
  }
  
  return(data)
}

# ysrpc <- function(y, s, d) {
#   y <- as.integer(y)
#   s <- clean_num_input(s, 1, 3)
#   
#   y <- check_year(y)
#   s <- check_section(s)
#   
#   if (s == "all") {
#     data <- tibble(
#       year = y,
#       section_code = c,
#       observation = "You are better off downloading the compressed datasets from docs.tradestatistics.io/accesing-the-data.html"
#     )
#     
#     return(data)
#   }
#   
#   query <- d %>% 
#     filter(year == y, section_code == s)
#   
#   data <- query %>% 
#     collect() %>% 
#     mutate(
#       year = as.integer(year)
#     ) %>% 
#     select(year, section_code, everything())
#   
#   if (nrow(data) == 0) {
#     data <- no_data("ysrpc", y = y, s = s)
#   }
#   
#   return(data)
# }

rtas <- function(y) {
  y <- as.integer(y)
  y <- check_year(y)
  
  data <- tbl(con, "rtas") %>% 
    filter(year == y) %>% 
    collect()
  
  if (nrow(data) == 0) {
    data <- no_data("rtas", y = y, c = c)
  }
  
  return(data)
}

tariffs <- function(y, r, c) {
  y <- as.integer(y)
  r <- clean_char_input(r, 1, 5)
  c <- clean_num_input(c, 1, 6)
  
  y <- check_year(y)
  r <- check_reporter(r)
  c <- check_commodity(c)
  
  query <- tbl(con, "tariffs") %>% 
    filter(year == y)
  
  if (r != "all" & (nchar(r) == 3 | grepl("e-", r))) {
    query <- query %>% 
      filter(reporter_iso == r)
  }
  
  if (r != "all" & nchar(r) == 4) {
    r2 <- multiple_reporters(r)
    
    query <- query %>% 
      filter(reporter_iso %in% r2)
  }
  
  if (c != "all" & nchar(c) != 2) {
    query <- query %>% 
      filter(commodity_code == c)
  }
  
  if (c != "all" & nchar(c) == 2) {
    query <- query %>% 
      filter(substr(commodity_code, 1, 2) == c)
  }
  
  data <- query %>% 
    collect()
  
  if (nrow(data) == 0) {
    data <- no_data("tariffs", y = y, r = r, c = c)
  }
  
  return(data)
}

# Available years in the DB -----------------------------------------------

min_year <- function() {
  return(2002)
}

max_year <- function() {
  return(2020)
}

# Title and description ---------------------------------------------------

#* @apiTitle Open Trade Statistics API
#* @apiDescription International trade data available with different levels of aggregation
#* @apiTOS https://tradestatistics.io/accesing-the-data.html#before-downloading-datasets
#* @apiVersion 3.0.0
#* @apiContact list(name = "API Support", url = "https://github.com/tradestatistics/plumber-api/issues", email = "mavargas11@uc.cl")
#* @apiLicense list(name = "Apache 2.0", url = "https://www.apache.org/licenses/LICENSE-2.0.html")

# Hello World -------------------------------------------------------------

#* Echo back Hello World!
#* @get /

function() {
  paste("Hello World! Welcome to Open Trade Statistics API. Go to https://api.tradestatistics.io/__docs__/ or use the R client!")
}

# Countries ---------------------------------------------------------------

#* Echo back the result of a query on countries table
#* @get /countries
#* @serializer parquet
function() { countries() }


# Distances ---------------------------------------------------------------

#* Echo back the result of a query on countries table
#* @get /distances
#* @serializer parquet
function() { distances() }

# Reporters ---------------------------------------------------------------

#* Echo back the result of a query on yr table
#* @param y Year
#* @get /reporters
#* @serializer parquet
function(y = NA) {
  y <- as.integer(y)
  
  y <- check_year(y)
  
  data <- tbl(con, "yr") %>% 
    filter(year == y) %>% 
    select(reporter_iso) %>% 
    distinct() %>% 
    arrange(reporter_iso) %>% 
    collect()
  
  return(data)
}

# Partners ----------------------------------------------------------------

#* Echo back the result of a query on yrp table
#* @param y Year
#* @get /partners
#* @serializer parquet
function(y = NA) {
  y <- as.integer(y)
  
  y <- check_year(y)
  
  data <- tbl(con, "yrp") %>% 
    filter(year == y) %>% 
    select(partner_iso) %>% 
    distinct() %>% 
    arrange(partner_iso) %>% 
    collect()
  
  return(data)
}

# Sections ----------------------------------------------------------------

#* Echo back the result of a query on commodities table
#* @get /sections
#* @serializer parquet
function() { sections() }

#* Echo back the result of a query on commodities table
#* @get /sections_colors
#* @serializer parquet
function() { sections_colors() }

# Commodities ----------------------------------------------------------------

#* Echo back the result of a query on commodities table
#* @get /commodities
#* @serializer parquet
function() { commodities() }

#* Echo back the result of a query on commodities table
#* @get /commodities_short
#* @serializer parquet
function() { commodities_short() }

# YC ----------------------------------------------------------------------

#* Echo back the result of a query on yc table
#* @param y Year
#* @param c Commodity Code
#* @get /yc
#* @serializer parquet
function(y = NA, c = NA) {
  d <- tbl(con, "yc")
  yc(y, c, d)
}

# YR ----------------------------------------------------------------------

#* Echo back the result of a query on yr table
#* @param y Year
#* @param r Reporter ISO
#* @get /yr
#* @serializer parquet
function(y = NA, r = NA) {
  d <- tbl(con, "yr")
  yr(y, r, d)
}

# YRC ---------------------------------------------------------------------

#* Echo back the result of a query on yrc table
#* @param y Year
#* @param r Reporter ISO
#* @param c Commodity code
#* @get /yrc
#* @serializer parquet
function(y = NA, r = NA, c = "all") {
  d <- tbl(con, "yrc")
  yrc(y, r, c, d)
}

# YRP ---------------------------------------------------------------------

#* Echo back the result of a query on yrp table
#* @param y Year
#* @param r Reporter ISO
#* @param p Partner ISO
#* @get /yrp
#* @serializer parquet
function(y = NA, r = NA, p = NA) {
  d <- tbl(con, "yrp")
  yrp(y, r, p, d)
}

# YRPC --------------------------------------------------------------------

#* Echo back the result of a query on yrpc table
#* @param y Year
#* @param r Reporter ISO
#* @param p Partner ISO
#* @param c Commodity code
#* @get /yrpc
#* @serializer parquet
function(y = NA, r = NA, p = NA, c = "all") {
  d <- tbl(con, "yrpc")
  yrpc(y, r, p, c, d)
}

# YSRPC -------------------------------------------------------------------

# #* Echo back the result of a query on ysrpc table
# #* @param y Year
# #* @param s Section code
# #* @serializer parquet
# #* @get /ysrpc
# function(y = NA, s = NA) {
#   d <- d_ysrpc_tc
#   ysrpc(y, s, d)
# }

# RTAs --------------------------------------------------------------------

#* Echo back the result of a query on rtas table
#* @param y Year
#* @get /rtas

function(y = NA) {
  rtas(y)
}

# Tariffs -----------------------------------------------------------------

#* Echo back the result of a query on tariffs table
#* @param y Year
#* @param r Reporter ISO
#* @param c Commodity code
#* @get /tariffs
#* @serializer parquet
function(y = NA, r = NA, c = NA) {
  tariffs(y, r, c)
}

# Year range --------------------------------------------------------------

#* Minimum and maximum years with available data
#* @get /year_range
#* @serializer csv
function() {
  tibble(year = c(min_year(), max_year()))
}

# Available tables --------------------------------------------------------

#* All the tables generated by this API
#* @get /tables
#* @serializer csv
function() {
  readr::read_csv("available-tables.csv")
}
