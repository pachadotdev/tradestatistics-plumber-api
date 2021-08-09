# queries.R

# Packages ----------------------------------------------------------------

library(arrow)
library(dplyr)

# Arrow datasets ----------------------------------------------------------

d_yrpc <- open_dataset(
  "../hs-rev1992-visualization/yrpc",
  partitioning = c("year", "reporter_iso")
)

d_yrp <- open_dataset(
  "../hs-rev1992-visualization/yrp",
  partitioning = c("year", "reporter_iso")
)

d_yrc <- open_dataset(
  "../hs-rev1992-visualization/yrc",
  partitioning = c("year", "reporter_iso")
)

d_yr_communities <- open_dataset(
  "../hs-rev1992-visualization/yr-communities",
  partitioning = c("year", "reporter_iso")
)

d_yr_groups <- open_dataset(
  "../hs-rev1992-visualization/yr-groups",
  partitioning = c("year", "reporter_iso")
)

d_yr <- open_dataset(
  "../hs-rev1992-visualization/yr",
  partitioning = c("year", "reporter_iso")
)

d_yc <- open_dataset(
  "../hs-rev1992-visualization/yc",
  partitioning = c("year")
)

# Static data -------------------------------------------------------------

d_countries <- bind_rows(
  read_parquet("../hs-rev1992-visualization/attributes/countries.parquet"),
  read_parquet("aliases/countries.parquet")
)

d_commodities <- bind_rows(
  read_parquet("../hs-rev1992-visualization/attributes/commodities.parquet"),
  read_parquet("aliases/commodities.parquet")
)

d_commodities_shortnames <- read_parquet("../hs-rev1992-visualization/attributes/commodities_shortnames.parquet") %>% 
  select(-commodity_fullname_english)

d_communities <- read_parquet("../hs-rev1992-visualization/attributes/communities.parquet")

countries <- function() {
  return(d_countries)
}

commodities <- function() {
  return(d_commodities)
}

commodities_shortnames <- function() {
  return(d_commodities_shortnames)
}

commodities_communities <- function() {
  return(d_communities)  
}

# create vectors by continent to filter by using meta variables like americas, africa, etc

## Africa
countries_africa <- countries() %>% 
  filter(continent == "Africa") %>% 
  select(country_iso) %>% 
  pull()

## Americas
countries_americas <- countries() %>% 
  filter(continent == "Americas") %>% 
  select(country_iso) %>% 
  pull()

## Asia
countries_asia <- countries() %>% 
  filter(continent == "Asia") %>% 
  select(country_iso) %>% 
  pull()

## Europe
countries_europe <- countries() %>% 
  filter(continent == "Europe") %>% 
  select(country_iso) %>% 
  pull()

## Oceania
countries_oceania <- countries() %>% 
  filter(continent == "Oceania") %>% 
  select(country_iso) %>% 
  pull()

# Clean inputs ------------------------------------------------------------

clean_char_input <- function(x, i, j) {
  y <- iconv(x, to = "ASCII//TRANSLIT", sub = "")
  y <- gsub("[^[:alpha:]-]", "", y)
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
    return(paste0("year=", y))
  }
}

check_reporter <- function(r) {
  if (!nchar(r) <= 4 | !r %in% c(countries()$country_iso)) {
    return("The specified reporter is not a valid ISO code or alias. Read the documentation: tradestatistics.io")
  } else {
    return(paste0("reporter_iso=", r))
  }
}

check_partner <- function(p) {
  if (!nchar(p) <= 4 | !p %in% c(countries()$country_iso)) {
    return("The specified partner is not a valid ISO code or alias. Read the documentation: tradestatistics.io")
  } else {
    return(p)
  }
}

check_commodity <- function(c) {
  if (!nchar(c) <= 4 | !c %in% commodities()$commodity_code) {
    return("The specified commodity code is not a valid string. Read the documentation: tradestatistics.io")
  } else {
    return(c)
  }
}

# Format functions --------------------------------------------------------

remove_hive <- function(x) {
  gsub(".*=", "", x)
}

# Helpers -----------------------------------------------------------------

multiple_reporters <- function(r) {
  r2 <- switch(
    r,
    "reporter_iso=c-af" = countries_africa,
    "reporter_iso=c-am" = countries_americas,
    "reporter_iso=c-as" = countries_asia,
    "reporter_iso=c-eu" = countries_europe,
    "reporter_iso=c-oc" = countries_oceania
  )
  
  return(paste0("reporter_iso=", r2))
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

no_data <- function(table, y = NULL, r = NULL, p = NULL, c = NULL) {
  if (table == "yrpc") {
    d <- tibble(
      year = remove_hive(y),
      reporter_iso = remove_hive(r),
      partner_iso = p,
      commodity_code = c,
      observation = "No data available for these filtering parameters"
    )
  }
  
  if (table == "yrp") {
    d <- tibble(
      year = remove_hive(y),
      reporter_iso = remove_hive(r),
      partner_iso = p,
      observation = "No data available for these filtering parameters"
    )
  }
  
  if (table == "yrc") {
    d <- tibble(
      year = remove_hive(y),
      reporter_iso = remove_hive(r),
      commodity_code = c,
      observation = "No data available for these filtering parameters"
    )
  }
  
  if (table == "yr") {
    d <- tibble(
      year = remove_hive(y),
      reporter_iso = remove_hive(r),
      observation = "No data available for these filtering parameters"
    )
  }
  
  if (table == "yc") {
    d <- tibble(
      year = remove_hive(y),
      commodity_code = c,
      observation = "No data available for these filtering parameters"
    )
  }
  
  return(d)
}

# Available years in the DB -----------------------------------------------

min_year <- function() {
  return(1962)
}

max_year <- function() {
  return(2020)
}

# Title and description ---------------------------------------------------

#* @apiTitle Open Trade Statistics API
#* @apiDescription International trade data available with different levels of aggregation
#* @apiVersion 2.0.0
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

function(y = NULL) {
  y <- as.integer(y)
  
  y <- check_year(y)
  
  data <- d_yr %>% 
    filter(year == y) %>% 
    select(reporter_iso) %>% 
    collect() %>% 
    mutate(reporter_iso = remove_hive(reporter_iso)) %>% 
    arrange(reporter_iso)
  
  return(data)
}

# Partners ----------------------------------------------------------------

#* Echo back the result of a query on yrp table
#* @param y Year
#* @get /partners

function(y = NULL) {
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

# Commodities ----------------------------------------------------------------

#* Echo back the result of a query on commodities table
#* @get /commodities

function() { commodities() }

#* Echo back the result of a query on commodities_shortnames table
#* @get /commodities_shortnames

function() { commodities_shortnames() }

#* Echo back the result of a query on communities table
#* @get /commodities_communities

function() { commodities_communities() }

# YC ----------------------------------------------------------------------

#* Echo back the result of a query on yc table
#* @param y Year
#* @param c Commodity code
#* @get /yc

function(y = NULL, c = "all") {
  y <- as.integer(y)
  c <- clean_num_input(c, 1, 4)
  
  y <- check_year(y)
  c <- check_commodity(c)
  
  query <- d_yc %>% 
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
    collect() %>% 
    mutate(year = remove_hive(year)) %>% 
    select(year, everything())
  
  if (nrow(data) == 0) {
    data <- no_data("yc", y, c)
  }
  
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
  
  y <- check_year(y)
  r <- check_reporter(r)
  
  query <- d_yr %>% 
    filter(year == y)

  if (r != "reporter_iso=all" & nchar(remove_hive(r)) == 3) {
    query <- query %>% 
      filter(reporter_iso == r)
  }
  
  if (r != "reporter_iso=all" & nchar(remove_hive(r)) == 4) {
    r2 <- multiple_reporters(r)
    
    query <- query %>% 
      filter(reporter_iso %in% r2)
  }
  
  data <- query %>% 
    collect() %>% 
    mutate(
      year = remove_hive(year),
      reporter_iso = remove_hive(reporter_iso)
    ) %>% 
    select(year, reporter_iso, everything())
    
  
  if (nrow(data) == 0) {
    data <- no_data("yr", y, r)
  }
  
  return(data)
}

# YR-Communities ----------------------------------------------------------

#* Echo back the result of a query on yrc-communities table
#* @param y Year
#* @param r Reporter ISO
#* @get /yr-communities

function(y = NULL, r = NULL) {
  y <- as.integer(y)
  r <- clean_char_input(r, 1, 4)
  
  y <- check_year(y)
  r <- check_reporter(r)
  
  query <- d_yr_communities %>% 
    filter(year == y)
  
  if (r != "reporter_iso=all" & nchar(remove_hive(r)) == 3) {
    query <- query %>% 
      filter(reporter_iso == r)
  }
  
  if (r != "reporter_iso=all" & nchar(remove_hive(r)) == 4) {
    r2 <- multiple_reporters(r)
    
    query <- query %>% 
      filter(reporter_iso %in% r2)
  }
  
  data <- query %>% 
    collect() %>% 
    mutate(
      year = remove_hive(year),
      reporter_iso = remove_hive(reporter_iso)
    ) %>% 
    select(year, reporter_iso, everything())
  
  
  if (nrow(data) == 0) {
    data <- no_data("yr", y, r)
  }
  
  return(data)
}

# YR-Groups ---------------------------------------------------------------

#* Echo back the result of a query on yr-groups table
#* @param y Year
#* @param r Reporter ISO
#* @get /yr-groups

function(y = NULL, r = NULL) {
  y <- as.integer(y)
  r <- clean_char_input(r, 1, 4)
  
  y <- check_year(y)
  r <- check_reporter(r)
  
  query <- d_yr_groups %>% 
    filter(year == y)
  
  if (r != "reporter_iso=all" & nchar(remove_hive(r)) == 3) {
    query <- query %>% 
      filter(reporter_iso == r)
  }
  
  if (r != "reporter_iso=all" & nchar(remove_hive(r)) == 4) {
    r2 <- multiple_reporters(r)
    
    query <- query %>% 
      filter(reporter_iso %in% r2)
  }
  
  data <- query %>% 
    collect() %>% 
    mutate(
      year = remove_hive(year),
      reporter_iso = remove_hive(reporter_iso)
    ) %>% 
    select(year, reporter_iso, everything())
  
  
  if (nrow(data) == 0) {
    data <- no_data("yr", y, r)
  }
  
  return(data)
}

# YRC ---------------------------------------------------------------------

#* Echo back the result of a query on yrc table
#* @param y Year
#* @param r Reporter ISO
#* @param c Commodity code
#* @get /yrc

function(y = NULL, r = NULL, c = "all") {
  y <- as.integer(y)
  r <- clean_char_input(r, 1, 4)
  c <- clean_num_input(c, 1, 4)
  
  y <- check_year(y)
  r <- check_reporter(r)
  c <- check_commodity(c)
  
  if (all(c(remove_hive(r) , c) == "all")) {
    data <- tibble(
      year = remove_hive(y),
      reporter_iso = remove_hive(r),
      commodity_code = c,
      observation = "You are better off downloading the compressed datasets from docs.tradestatistics.io/accesing-the-data.html"
    )
    
    return(data)
  }
  
  query <- d_yrc %>% 
    filter(year == y)
  
  if (r != "reporter_iso=all" & nchar(remove_hive(r)) == 3) {
    query <- query %>% 
      filter(reporter_iso == r)
  }
  
  if (r != "reporter_iso=all" & nchar(remove_hive(r)) == 4) {
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
      year = remove_hive(year),
      reporter_iso = remove_hive(reporter_iso)
    ) %>% 
    select(year, reporter_iso, everything())
  
  if (nrow(data) == 0) {
    data <- no_data("yrc", y, r, c)
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
  
  y <- check_year(y)
  r <- check_reporter(r)
  p <- check_partner(p)
  
  # if (all(c(remove_hive(r) , p, c) == "all")) {
  #   data <- tibble(
  #     year = remove_hive(y),
  #     reporter_iso = remove_hive(r),
  #     partner_iso = p,
  #     observation = "You are better off downloading the compressed datasets from docs.tradestatistics.io/accesing-the-data.html"
  #   )
  #   
  #   return(data)
  # }
  
  query <- d_yrp %>% 
    filter(year == y)
  
  if (r != "reporter_iso=all" & nchar(remove_hive(r)) == 3) {
    query <- query %>% 
      filter(reporter_iso == r)
  }
  
  if (r != "reporter_iso=all" & nchar(remove_hive(r)) == 4) {
    r2 <- multiple_reporters(r)
    
    query <- query %>% 
      filter(reporter_iso %in% r2)
  }
  
  if (p != "all" & nchar(p) == 3) {
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
      year = remove_hive(year),
      reporter_iso = remove_hive(reporter_iso)
    ) %>% 
    select(year, reporter_iso, everything())
  
  if (nrow(data) == 0) {
    data <- no_data("yrp", y, r, p)
  }
  
  return(data)
}

# YRPC --------------------------------------------------------------------

#* Echo back the result of a query on yrpc table
#* @param y Year
#* @param r Reporter ISO
#* @param p Partner ISO
#* @param c Commodity code
#* @get /yrpc

function(y = NULL, r = NULL, p = NULL, c = "all") {
  y <- as.integer(y)
  r <- clean_char_input(r, 1, 4)
  p <- clean_char_input(p, 1, 4)
  c <- clean_num_input(c, 1, 4)
  
  y <- check_year(y)
  r <- check_reporter(r)
  p <- check_partner(p)
  c <- check_commodity(c)
  
  if (all(c(remove_hive(r), p , c) == "all")) {
    data <- tibble(
      year = remove_hive(y),
      reporter_iso = remove_hive(r),
      partner_iso = p,
      commodity_code = c,
      observation = "You are better off downloading the compressed datasets from docs.tradestatistics.io/accesing-the-data.html"
    )
    
    return(data)
  }
  
  query <- d_yrpc %>% 
    filter(year == y)
  
  if (r != "reporter_iso=all" & nchar(remove_hive(r)) == 3) {
    query <- query %>% 
      filter(reporter_iso == r)
  }
  
  if (r != "reporter_iso=all" & nchar(remove_hive(r)) == 4) {
    r2 <- multiple_reporters(r)
    
    query <- query %>% 
      filter(reporter_iso %in% r2)
  }
  
  if (p != "all" & nchar(p) == 3) {
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
      year = remove_hive(year),
      reporter_iso = remove_hive(reporter_iso)
    ) %>% 
    select(year, reporter_iso, everything())
  
  if (nrow(data) == 0) {
    data <- no_data("yrpc", y, r, p, c)
  }
  
  return(data)
}

# Year range --------------------------------------------------------------

#* Minimum and maximum years with available data
#* @get /years

function() {
  tibble(min_year = min_year(), max_year = max_year())
}

# Available tables --------------------------------------------------------

#* All the tables generated by this API
#* @get /tables

function() {
  tibble(
    table = c(
      "countries",
      "reporters",
      "partners",
      "commodities",
      "commodities_shortnames",
      "commodities_communities",
      "yrpc",
      "yrp",
      "yrc",
      "yr",
      "yr-communities",
      "yr-groups",
      "yc",
      "years"
    ),
    description = c(
      "Countries metadata",
      "Reporters for a given year",
      "Partners for a given year",
      "Commodities metadata",
      "Commodities short names",
      "Commodities communities",
      "Reporter-Partner trade at commodity level (Year, Reporter, Partner and Commodity Code)",
      "Reporter-Partner trade at aggregated level (Year, Reporter and Partner)",
      "Reporter trade at commodity level (Year, Reporter and Commodity Code)",
      "Reporter trade at aggregated level (Year and Reporter)",
      "Reporter trade at commodity community level (Year, Reporter and Commodity Community) (22 communities)",
      "Reporter trade at commodity group level (Year, Reporter and Commodity Group) (99 groups)",
      "Commodity trade at detailed level (Year and Commodity Code)",
      "Minimum and maximum years with available data"
    ),
    source = c(
      rep("UN Comtrade (with modifications)",3),
      rep("Center for International Development at Harvard University (with modifications)", 2),
      rep("Open Trade Statistics",9)
    )
  )
}
