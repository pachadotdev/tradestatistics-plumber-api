# queries.R

# Packages ----------------------------------------------------------------

library(arrow)
library(dplyr)

# Data with transportation costs ------------------------------------------

d_yrpc_tc <- open_dataset(
  "../hs12-visualization-transportation-costs/yrpc",
  partitioning = schema(year = int32(), reporter_iso = string())
)

d_yrp_tc <- open_dataset(
  "../hs12-visualization-transportation-costs/yrp",
  partitioning = schema(year = int32(), reporter_iso = string())
)

d_yrc_tc <- open_dataset(
  "../hs12-visualization-transportation-costs/yrc",
  partitioning = schema(year = int32(), reporter_iso = string())
)

d_yr_tc <- open_dataset(
  "../hs12-visualization-transportation-costs/yr",
  partitioning = schema(year = int32())
)

d_ysrpc_tc <- open_dataset(
  "../hs12-visualization-transportation-costs/ysrpc",
  partitioning = schema(year = int32(), section_code = string())
)

# Data without transportation costs ---------------------------------------

# d_yrpc_ntc <- open_dataset(
#   "../hs12-visualization-no-transportation-costs/yrpc",
#   partitioning = schema(year = int32(), reporter_iso = string())
# )
# 
# d_yrp_ntc <- open_dataset(
#   "../hs12-visualization-no-transportation-costs/yrp",
#   partitioning = schema(year = int32(), reporter_iso = string())
# )
# 
# d_yrc_ntc <- open_dataset(
#   "../hs12-visualization-no-transportation-costs/yrc",
#   partitioning = schema(year = int32(), reporter_iso = string())
# )
# 
# d_yr_ntc <- open_dataset(
#   "../hs12-visualization-no-transportation-costs/yr",
#   partitioning = schema(year = int32())
# )
# 
# d_ysrpc_ntc <- open_dataset(
#   "../hs12-visualization-no-transportation-costs/ysrpc",
#   partitioning = schema(year = int32(), section_code = string())
# )

# Static data -------------------------------------------------------------

# fix Aruba, Roumania and Timor-Leste ISO codes
# and add countries present in UN COMTRADE kast data
# Antarctica, Saint Barthelemy, Curacao, Sint Maarten and South Sudan
# also add aliases and areas
d_countries <- bind_rows(
  read_parquet("../hs12-visualization-transportation-costs/attributes/countries.parquet"),
  read_parquet("aliases/countries.parquet")
) %>% 
  arrange(country_iso)

d_commodities <- bind_rows(
  read_parquet("../hs12-visualization-transportation-costs/attributes/commodities.parquet"),
  read_parquet("aliases/commodities.parquet")
)

d_sections <- read_parquet("../hs12-visualization-transportation-costs/attributes/sections.parquet")

countries <- function() {
  return(d_countries)
}

commodities <- function() {
  return(d_commodities)
}

sections <- function() {
  return(d_sections)
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
    collect() %>% 
    mutate(
      year = as.integer(year),
      reporter_iso = reporter_iso
    ) %>% 
    select(year, reporter_iso, everything())
  
  
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
    collect() %>% 
    mutate(
      year = as.integer(year),
      reporter_iso = reporter_iso
    ) %>% 
    select(year, reporter_iso, everything())
  
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
    collect() %>% 
    mutate(
      year = as.integer(year),
      reporter_iso = reporter_iso
    ) %>% 
    select(year, reporter_iso, everything())
  
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
    collect() %>% 
    mutate(
      year = as.integer(year),
      reporter_iso = reporter_iso
    ) %>% 
    select(year, reporter_iso, everything())
  
  if (nrow(data) == 0) {
    data <- no_data("yrpc", y = y, r = r, p = p, c = c)
  }
  
  return(data)
}

ysrpc <- function(y, s, d) {
  y <- as.integer(y)
  s <- clean_num_input(s, 1, 3)
  
  y <- check_year(y)
  s <- check_section(s)
  
  if (s == "all") {
    data <- tibble(
      year = y,
      section_code = c,
      observation = "You are better off downloading the compressed datasets from docs.tradestatistics.io/accesing-the-data.html"
    )
    
    return(data)
  }
  
  query <- d %>% 
    filter(year == y, section_code == s)
  
  data <- query %>% 
    collect() %>% 
    mutate(
      year = as.integer(year)
    ) %>% 
    select(year, section_code, everything())
  
  if (nrow(data) == 0) {
    data <- no_data("ysrpc", y = y, s = s)
  }
  
  return(data)
}

rtas <- function(y) {
  y <- as.integer(y)
  y <- check_year(y)
  
  data <- open_dataset(
    "../rtas-and-tariffs/rtas/",
    partitioning = "year"
  ) %>% 
    filter(year == y) %>% 
    collect() %>% 
    mutate(year = as.integer(year))
  
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
  
  query <- open_dataset(
    "../rtas-and-tariffs/mfn/",
    partitioning = c("year", "reporter_iso")
  ) %>% 
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
    collect() %>% 
    mutate(
      year = as.integer(year),
      reporter_iso = reporter_iso
    ) %>% 
    select(year, everything())
  
  if (nrow(data) == 0) {
    data <- no_data("tariffs", y = y, r = r, c = c)
  }
  
  return(data)
}

# Available years in the DB -----------------------------------------------

min_year <- function() {
  return(1980)
}

max_year <- function() {
  return(2019)
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

function() { countries() }

# Reporters ---------------------------------------------------------------

#* Echo back the result of a query on yr table
#* @param y Year
#* @get /reporters

function(y = NA) {
  y <- as.integer(y)
  
  y <- check_year(y)
  
  data <- d_yr %>% 
    filter(year == y) %>% 
    select(reporter_iso) %>% 
    collect() %>% 
    mutate(reporter_iso = reporter_iso) %>% 
    arrange(reporter_iso)
  
  return(data)
}

# Partners ----------------------------------------------------------------

#* Echo back the result of a query on yrp table
#* @param y Year
#* @get /partners

function(y = NA) {
  y <- as.integer(y)
  
  y <- check_year(y)
  
  data <- d_yrp %>% 
    filter(year == y) %>% 
    select(partner_iso) %>% 
    collect() %>% 
    distinct() %>% 
    arrange(partner_iso)
  
  return(data)
}

# Sections ----------------------------------------------------------------

#* Echo back the result of a query on commodities table
#* @get /sections

function() { sections() }

# Commodities ----------------------------------------------------------------

#* Echo back the result of a query on commodities table
#* @get /commodities

function() { commodities() }

# YR ----------------------------------------------------------------------

#* Echo back the result of a query on yr table
#* @param y Year
#* @param r Reporter ISO
#* @get /yr_tc
function(y = NA, r = NA) {
  d <- d_yr_tc
  yr(y, r, d)
}

# #* Echo back the result of a query on yr table
# #* @param y Year
# #* @param r Reporter ISO
# #* @get /yr_ntc
# function(y = NA, r = NA) {
#   d <- d_yr_ntc
#   yr(y, r, d)
# }

# YRC ---------------------------------------------------------------------

#* Echo back the result of a query on yrc table
#* @param y Year
#* @param r Reporter ISO
#* @param c Commodity code
#* @get /yrc_tc
function(y = NA, r = NA, c = "all") {
  d <- d_yrc_tc
  yrc(y, r, c, d)
}

# #* Echo back the result of a query on yrc table
# #* @param y Year
# #* @param r Reporter ISO
# #* @param c Commodity code
# #* @get /yrc_ntc
# function(y = NA, r = NA, c = "all") {
#   d <- d_yrc_ntc
#   yrc(y, r, c, d)
# }

# YRP ---------------------------------------------------------------------

#* Echo back the result of a query on yrp table
#* @param y Year
#* @param r Reporter ISO
#* @param p Partner ISO
#* @get /yrp_tc
function(y = NA, r = NA, p = NA) {
  d <- d_yrp_tc
  yrp(y, r, p, d)
}

# #* Echo back the result of a query on yrp table
# #* @param y Year
# #* @param r Reporter ISO
# #* @param p Partner ISO
# #* @get /yrp_ntc
# function(y = NA, r = NA, p = NA) {
#   d <- d_yrp_ntc
#   yrp(y, r, p, d)
# }

# YRPC --------------------------------------------------------------------

#* Echo back the result of a query on yrpc table
#* @param y Year
#* @param r Reporter ISO
#* @param p Partner ISO
#* @param c Commodity code
#* @serializer parquet
#* @get /yrpc_tc
function(y = NA, r = NA, p = NA, c = "all") {
  d <- d_yrpc_tc
  yrpc(y, r, p, c, d)
}

# #* Echo back the result of a query on yrpc table
# #* @param y Year
# #* @param r Reporter ISO
# #* @param p Partner ISO
# #* @param c Commodity code
# #* @serializer parquet
# #* @get /yrpc_ntc
# function(y = NA, r = NA, p = NA, c = "all") {
#   d <- d_yrpc_ntc
#   yrpc(y, r, p, c, d)
# }

# YSRPC -------------------------------------------------------------------

#* Echo back the result of a query on ysrpc table
#* @param y Year
#* @param s Section code
#* @serializer parquet
#* @get /ysrpc_tc
function(y = NA, s = NA) {
  d <- d_ysrpc_tc
  ysrpc(y, s, d)
}

# #* Echo back the result of a query on ysrpc table
# #* @param y Year
# #* @param s Section code
# #* @serializer parquet
# #* @get /ysrpc_ntc
# function(y = NA, s = NA) {
#   d <- d_ysrpc_ntc
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

function(y = NA, r = NA, c = NA) {
  tariffs(y, r, c)
}

# Year range --------------------------------------------------------------

#* Minimum and maximum years with available data
#* @get /year_range

function() {
  tibble(year = c(min_year(), max_year()))
}

# Available tables --------------------------------------------------------

#* All the tables generated by this API
#* @get /tables

function() {
  readr::read_csv("available-tables.csv")
}
