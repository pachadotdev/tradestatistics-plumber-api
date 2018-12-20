# queries.R

# Packages ----------------------------------------------------------------

library(glue)
library(RPostgreSQL)

# DB connection parameters ------------------------------------------------

source("/api/credentials.R")
drv <- dbDriver("PostgreSQL")

con <- dbConnect(
  drv, 
  host = dbhost, 
  port = 5432,
  user = dbusr,
  password = dbpwd,
  dbname = dbname
)

# List of countries (to filter API parameters) ----------------------------

countries_query <- glue_sql("
                            SELECT *
                            FROM public.attributes_country_names
                            ", .con = con)

countries_data <- dbGetQuery(con, countries_query)

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
  query <- glue_sql("
                    SELECT *
                    FROM public.attributes_country_names
                    ", .con = con)
  
  data <- dbGetQuery(con, query)
  
  return(data)
}

# Products ----------------------------------------------------------------

#* Echo back the result of a query on attributes_countries table
#* @get /products
function() {
  query <- glue_sql("
                    SELECT *
                    FROM public.attributes_product_names
                    ", .con = con)
  
  data <- dbGetQuery(con, query)
  
  return(data)
}

# YC ----------------------------------------------------------------------

#* Echo back the result of a query on yc table
#* @param y Year
#* @get /yc
function(y = NULL) {
  y <- as.integer(y)
  
  if (nchar(y) != 4 & y <= 2016 & y >= 1962) {
    return(
      paste("The specified year is not a valid integer value.")
    )
  }
  
  stopifnot(
    is.numeric(y)
  )
  
  query <- glue_sql("
                    SELECT *
                    FROM public.hs07_yc
                    WHERE year = {y}
                    ", .con = con)
  
  data <- dbGetQuery(con, query)
  
  return(data)
}

# YP ----------------------------------------------------------------------

#* Echo back the result of a query on yrpc table
#* @param y Year
#* @param p Partner ISO
#* @get /yp
function(y = NULL, p = NULL) {
  y <- as.integer(y)
  p <- tolower(substr(as.character(p), 1, 3))
  
  if (nchar(y) != 4 & y <= 2016 & y >= 1962) {
    return(
      paste("The specified year is not a valid integer value.")
    )
  }
  
  if (nchar(p) != 3 & !p %in% c(countries_data$country_iso, "all")) {
    return(
      paste("The specified reporter is not a valid ISO code, please check /countries.")
    )
  }
  
  stopifnot(
    is.numeric(y), 
    is.character(p)
  )
  
  if (p == "all") {
    query <- glue_sql("
                    SELECT *
                    FROM public.hs07_yp
                    WHERE year = {y}
                    ", .con = con) 
  } else {
    query <- glue_sql("
                    SELECT *
                    FROM public.hs07_yp
                    WHERE year = {y}
                    AND partner_iso = {p}
                    ", .con = con)
  }
  
  data <- dbGetQuery(con, query)
  
  return(data)
}

# YPC ---------------------------------------------------------------------

#* Echo back the result of a query on ypc table
#* @param y Year
#* @param p Partner ISO
#* @get /ypc
function(y = NULL, p = NULL) {
  y <- as.integer(y)
  p <- tolower(substr(as.character(p), 1, 3))
  
  if (nchar(y) != 4 & y <= 2016 & y >= 1962) {
    return(
      paste("The specified year is not a valid integer value.")
    )
  }
  
  if (nchar(p) != 3 & !p %in% c(countries_data$country_iso, "all")) {
    return(
      paste("The specified reporter is not a valid ISO code, please check /countries.")
    )
  }
  
  stopifnot(
    is.numeric(y), 
    is.character(p)
  )
  
  if (p == "all") {
    query <- glue_sql("
                    SELECT *
                    FROM public.hs07_ypc
                    WHERE year = {y}
                    ", .con = con)
  } else {
    query <- glue_sql("
                    SELECT *
                    FROM public.hs07_ypc
                    WHERE year = {y}
                    AND partner_iso = {p}
                    ", .con = con)
  }
  
  data <- dbGetQuery(con, query)
  
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
  
  if (nchar(y) != 4 & y <= 2016 & y >= 1962) {
    return(
      paste("The specified year is not a valid integer value.")
    )
  }
  
  if (nchar(r) != 3 & !r %in% c(countries_data$country_iso, "all")) {
    return(
      paste("The specified reporter is not a valid ISO code, please check /countries.")
    )
  }
  
  stopifnot(
    is.integer(y), 
    is.character(r)
  )
  
  if (r == "all") {
    query <- glue_sql("
                    SELECT *
                    FROM public.hs07_yr
                    WHERE year = {y}
                    ", .con = con)
  } else {
    query <- glue_sql("
                    SELECT *
                    FROM public.hs07_yr
                    WHERE year = {y}
                    AND reporter_iso = {r}
                    ", .con = con)
  }
  
  data <- dbGetQuery(con, query)
  
  return(data)
}

# YRC ---------------------------------------------------------------------

#* Echo back the result of a query on yrc table
#* @param y Year
#* @param r Reporter ISO
#* @get /yrc
function(y = NULL, r = NULL) {
  y <- as.integer(y)
  r <- tolower(substr(as.character(r), 1, 3))
  
  if (nchar(y) != 4 & y <= 2016 & y >= 1962) {
    return(
      paste("The specified year is not a valid integer value.")
    )
  }
  
  if (nchar(r) != 3 & !r %in% c(countries_data$country_iso, "all")) {
    return(
      paste("The specified reporter is not a valid ISO code, please check /countries.")
    )
  }
  
  stopifnot(
    is.numeric(y), 
    is.character(r)
  )
  
  if (r == "all") {
    query <- glue_sql("
                    SELECT *
                    FROM public.hs07_yrc
                    WHERE year = {y}
                    ", .con = con)
  } else {
    query <- glue_sql("
                    SELECT *
                    FROM public.hs07_yrc
                    WHERE year = {y}
                    AND reporter_iso = {r}
                    ", .con = con)
  }
  
  data <- dbGetQuery(con, query)
  
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
  r <- tolower(substr(as.character(r), 1, 3))
  p <- tolower(substr(as.character(p), 1, 3))
  
  if (nchar(y) != 4 & y <= 2016 & y >= 1962) {
    return(
      paste("The specified year is not a valid integer value.")
    )
  }
  
  if (nchar(r) != 3 & !r %in% c(countries_data$country_iso, "all")) {
    return(
      paste("The specified reporter is not a valid ISO code, please check /countries.")
    )
  }
  
  if (nchar(p) != 3 & !p %in% c(countries_data$country_iso, "all")) {
    return(
      paste("The specified reporter is not a valid ISO code, please check /countries.")
    )
  }
  
  stopifnot(
    is.integer(y), 
    is.character(r),
    is.character(p)
  )
  
  if (r == "all" & p != "all") {
    query <- glue_sql("
                    SELECT *
                    FROM public.hs07_yrp
                    WHERE year = {y}
                    AND partner_iso = {p}
                    ", .con = con)
  }
  
  if (r != "all" & p == "all") {
    query <- glue_sql("
                    SELECT *
                    FROM public.hs07_yrp
                    WHERE year = {y}
                    AND reporter_iso = {r}
                    ", .con = con)
  }
  
  if (r != "all" & p != "all") {
    query <- glue_sql("
                    SELECT *
                    FROM public.hs07_yrp
                    WHERE year = {y}
                    AND reporter_iso = {r}
                    AND partner_iso = {p}
                    ", .con = con)
  }
  
  if (r == "all" & p == "all") {
    query <- glue_sql("
                    SELECT *
                    FROM public.hs07_yrp
                    WHERE year = {y}
                    ", .con = con)
  }
  
  data <- dbGetQuery(con, query)
  
  return(data)
}

# YRPC --------------------------------------------------------------------

#* Echo back the result of a query on yrpc table
#* @param y Year
#* @param r Reporter ISO
#* @param p Partner ISO
#* @get /yrpc
function(y = NULL, r = NULL, p = NULL) {
  y <- as.integer(y)
  r <- tolower(substr(as.character(r), 1, 3))
  p <- tolower(substr(as.character(p), 1, 3))
  
  if (nchar(y) != 4 & y <= 2016 & y >= 1962) {
    return(
      paste("The specified year is not a valid integer value.")
    )
  }
  
  if (nchar(r) != 3 & !r %in% c(countries_data$country_iso, "all")) {
    return(
      paste("The specified reporter is not a valid ISO code, please check /countries.")
    )
  }
  
  if (nchar(p) != 3 & !p %in% c(countries_data$country_iso, "all")) {
    return(
      paste("The specified reporter is not a valid ISO code, please check /countries.")
    )
  }
  
  stopifnot(
    is.integer(y), 
    is.character(r),
    is.character(p)
  )
  
  if (r == "all" & p != "all") {
    query <- glue_sql("
                      SELECT *
                      FROM public.hs07_yrpc
                      WHERE year = {y}
                      AND partner_iso = {p}
                      ", .con = con)
  }
  
  if (r != "all" & p == "all") {
    query <- glue_sql("
                      SELECT *
                      FROM public.hs07_yrpc
                      WHERE year = {y}
                      AND reporter_iso = {r}
                      ", .con = con)
  }
  
  if (r != "all" & p != "all") {
    query <- glue_sql("
                      SELECT *
                      FROM public.hs07_yrpc
                      WHERE year = {y}
                      AND reporter_iso = {r}
                      AND partner_iso = {p}
                      ", .con = con)
  }
  
  if (r == "all" & p == "all") {
    query <- glue_sql("
                      SELECT *
                      FROM public.hs07_yrpc
                      WHERE year = {y}
                      ", .con = con)
  }
  
  data <- dbGetQuery(con, query)
  
  return(data)
}
