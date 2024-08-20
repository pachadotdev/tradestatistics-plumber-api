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
  tbl(con, "countries") %>%
    collect()
}

commodities <- function() {
  tbl(con, "commodities") %>%
    collect()
}

commodities_short <- function() {
  tbl(con, "commodities_short") %>%
    collect()
}

distances <- function() {
  tbl(con, "distances") %>%
    collect()
}

sections <- function() {
  tbl(con, "sections") %>%
    collect()
}

sections_colors <- function() {
  tbl(con, "sections_colors") %>%
    collect()
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
  y <- tolower(iconv(x, to = "ASCII//TRANSLIT", sub = ""))
  y <- if (grepl("^e-", y)) {
    gsub("[^[:alpha:][:digit:]]-", "", y)
  } else {
    gsub("[^[:alpha:]]-", "", y)
  }
  substr(y, i, j)
}

clean_num_input <- function(x, i, j) {
  y <- tolower(iconv(x, to = "ASCII//TRANSLIT", sub = ""))
  if (y != "all") {
    y <- gsub("[^[:digit:]]", "", y)
  }
  substr(y, i, j)
}

# Checks ------------------------------------------------------------------

check_year <- function(y) {
  if (nchar(y) != 4 || !y >= min_year() || !y <= max_year()) {
    return("The specified year is not a valid integer value. Read the documentation: tradestatistics.io")
  }
  y
}

check_reporter <- function(r) {
  if (!nchar(r) <= 5 || !r %in% c(countries()$country_iso)) {
    return("The specified reporter is not a valid ISO code or alias. Read the documentation: tradestatistics.io")
  }
  r
}

check_partner <- function(p) {
  if (!nchar(p) <= 5 || !p %in% c(countries()$country_iso)) {
    return("The specified partner is not a valid ISO code or alias. Read the documentation: tradestatistics.io")
  }
  p
}

check_commodity <- function(c) {
  if (!nchar(c) <= 6 || !c %in% c(commodities()$commodity_code)) {
    return("The specified commodity code is not a valid string. Read the documentation: tradestatistics.io")
  }
  c
}

check_section <- function(s) {
  if (!nchar(s) <= 3 || !s %in% c(sections()$section_code)) {
    return("The specified section code is not a valid string. Read the documentation: tradestatistics.io")
  }
  s
}

# Helpers -----------------------------------------------------------------

multiple_reporters <- function(r) {
  switch(r,
    "c-af" = countries_africa,
    "c-am" = countries_americas,
    "c-as" = countries_asia,
    "c-eu" = countries_europe,
    "c-oc" = countries_oceania
  )
}

multiple_partners <- function(p) {
  switch(p,
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

  if (table == "yrc") {
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

  d
}

# Data functions ----------------------------------------------------------

yc <- function(y, c, d) {
  y <- check_year(as.integer(y))
  c <- check_commodity(clean_num_input(c, 1, 6))

  query <- d %>%
    filter(year == y)

  if (c != "all" && nchar(c) != 2) {
    query <- query %>%
      filter(commodity_code == c)
  }

  if (c != "all" && nchar(c) == 2) {
    query <- query %>%
      filter(substr(commodity_code, 1, 2) == c)
  }

  d2 <- query %>%
    collect()

  if (nrow(d2) == 0) {
    d2 <- no_data("yc", y = y, c = c)
  }

  d2
}

yr <- function(y, r, d) {
  y <- check_year(as.integer(y))
  r <- check_reporter(clean_char_input(r, 1, 5))

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

  d2 <- query %>%
    collect()

  if (nrow(d2) == 0) {
    d2 <- no_data("yr", y = y, r = r)
  }

  d2
}

yrc <- function(y, r, c, d) {
  y <- check_year(as.integer(y))
  r <- check_reporter(clean_char_input(r, 1, 5))
  c <- check_commodity(clean_num_input(c, 1, 6))

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

  d2 <- query %>%
    collect()

  if (nrow(d2) == 0) {
    d2 <- no_data("yrc", y = y, r = r, c = c)
  }

  d2
}

yrp <- function(y, r, p, d) {
  y <- check_year(as.integer(y))
  r <- check_reporter(clean_char_input(r, 1, 5))
  p <- check_partner(clean_char_input(p, 1, 5))

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

  d2 <- query %>%
    collect()

  if (nrow(d2) == 0) {
    d2 <- no_data("yrp", y = y, r = r, p = p)
  }

  d2
}

yrpc <- function(y, r, p, c, d) {
  y <- check_year(as.integer(y))
  r <- check_reporter(clean_char_input(r, 1, 5))
  p <- check_partner(clean_char_input(p, 1, 5))
  c <- check_commodity(clean_num_input(c, 1, 6))

  if (r == "all" & p == "all") {
    d2 <- tibble(
      year = y,
      reporter_iso = r,
      partner_iso = p,
      commodity_code = c,
      observation = "You are better off downloading the compressed SQL dumps."
    )

    return(d2)
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

  d2 <- query %>%
    collect()

  if (nrow(d2) == 0) {
    d2 <- no_data("yrpc", y = y, r = r, p = p, c = c)
  }

  d2
}

# Available years in the DB -----------------------------------------------

min_year <- function() {
  1980L
}

max_year <- function() {
  2021L
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
function() {
  countries()
}

# Distances ---------------------------------------------------------------

#* Echo back the result of a query on countries table
#* @get /distances
function() {
  distances()
}

# Reporters ---------------------------------------------------------------

#* Echo back the result of a query on yr table
#* @param y Year
#* @get /reporters
function(y = NA) {
  y <- check_year(as.integer(y))

  tbl(con, "yr") %>%
    filter(year == y) %>%
    select(reporter_iso) %>%
    distinct() %>%
    arrange(reporter_iso) %>%
    collect()
}

# Partners ----------------------------------------------------------------

#* Echo back the result of a query on yrp table
#* @param y Year
#* @get /partners
function(y = NA) {
  y <- check_year(as.integer(y))

  tbl(con, "yrp") %>%
    filter(year == y) %>%
    select(partner_iso) %>%
    distinct() %>%
    arrange(partner_iso) %>%
    collect()
}

# Sections ----------------------------------------------------------------

#* Echo back the result of a query on commodities table
#* @get /sections
function() {
  sections()
}

#* Echo back the result of a query on commodities table
#* @get /sections_colors
function() {
  sections_colors()
}

# Commodities ----------------------------------------------------------------

#* Echo back the result of a query on commodities table
#* @get /commodities
function() {
  commodities()
}

#* Echo back the result of a query on commodities table
#* @get /commodities_short
function() {
  commodities_short()
}

# YC ----------------------------------------------------------------------

#* Echo back the result of a query on yc table
#* @param y Year
#* @param c Commodity Code
#* @get /yc
function(y = NA, c = NA) {
  yc(y, c, tbl(con, "yc"))
}

# YR ----------------------------------------------------------------------

#* Echo back the result of a query on yr table
#* @param y Year
#* @param r Reporter ISO
#* @get /yr
function(y = NA, r = NA) {
  yr(y, r, tbl(con, "yr"))
}

# YRC ---------------------------------------------------------------------

#* Echo back the result of a query on yrc table
#* @param y Year
#* @param r Reporter ISO
#* @param c Commodity code
#* @get /yrc
function(y = NA, r = NA, c = "all") {
  yrc(y, r, c, tbl(con, "yrc"))
}

# YRP ---------------------------------------------------------------------

#* Echo back the result of a query on yrp table
#* @param y Year
#* @param r Reporter ISO
#* @param p Partner ISO
#* @get /yrp
function(y = NA, r = NA, p = NA) {
  yrp(y, r, p, tbl(con, "yrp"))
}

# YRPC --------------------------------------------------------------------

#* Echo back the result of a query on yrpc table
#* @param y Year
#* @param r Reporter ISO
#* @param p Partner ISO
#* @param c Commodity code
#* @get /yrpc
function(y = NA, r = NA, p = NA, c = "all") {
  yrpc(y, r, p, c, tbl(con, "yrpc"))
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
