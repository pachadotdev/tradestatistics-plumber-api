# queries.R

# Packages ----------------------------------------------------------------

library(dplyr)
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

# Test --------------------------------------------------------------------

#* Echo back API status
#* @get /friend
function() {
  paste("You can't make memes on an API REST")
}

#* Echo back API status
#* @get /me
function(res) {
  res$body <- paste(
    "
    ,*************,,*/(((((//,,*(#%%%%%%%%%%%%%%%#(*,,,****************************************************,*/(((((((((/((((////****/((##%%%%%%
    ,*************,,//((((((//,,*(%%%%%%%%%%%%%%%%%##/*****************************************************,,*/(///(//////****//((##%%%%%%%%%%%
    ,************,,*/(((((((//***/#%%%%%%%%%%%%%%%%%%%#(/***************************************************,*//////////*//((#%%%%%%%%%%%%%%%%%
    ,***********,,*////////////***/##%%%%%%%%%%%%%%%%%%%##(*,***********************************************,,*////////(###%%%%%%%%%%%%%%%%%%%%
    ,**********,,,*/*******//////**/(#%%%%%%%%%%%%%%%%%%%%%#(/**********************************************,,,***/(##%%%%%%%%%%%%%%%%%%%%%%%%%
    ,*********,,,,*************///***/(#%%%%%%%%%%%%%%%%%%%%%%#(/***********************************,****,****/((#%%%%%%%%%%%%%%%%%%%%%%%%%%%%#
    ,*********,,,***************//****/(##%%%%%%%%%%%%%%%%%%%%%%##//**************//////////////////////((#####%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%#(
    ,********,,,,***********************/(#%%%%%%%%%%%%%%%%%%%%%%%##################%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%##(/
    ,*******,..,***********************,,*/##%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%###((//
    ,*******,.,,***********************,,,,*(#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%##(//**//
    ,******,.,,,************************,,,,*/(#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%#(//*******
    ,*****,,,,,********,***,,,,,,,,,,,,*,,,,,,*/(######%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%##(/**********
    ,*****,..,*******,,,,,,,,,,,,,,,,,,,,,,*,,,,*///((#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%###(/************
    ,*****,,,*******,,,,,*,,,,,,,,,,,,,,,,,****,,,*/(#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%#######(//**************
    ,****,.,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,**,,,/(%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%#((//******************
    ,***,..,,,,,,,,,,,,,,,,,,,,,,,,,,,,,..,,,,,,,*(#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%#(/*******************
    ,**,,.,,,,,,,,,,,,,,,,,,,,,,,,,,.......,,,,,,/#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%#####%%%%%%%%%%%%%%%%#(/******************
    ,**,..,,,,,,,,,,,,,,,,,,,,,,,,,......,,,*,,,*(#%%%%%%%%##(((/(##%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%##(((/*/((#%%%%%%%%%%%%%%#(/*****************
    ,*,..,,,,,,,,,,,,,,,,,,,,,,,,,,,.....,,**,,*/#%%%%%%%##((((*,**/#%%%%%%%%%%%%%%%%%%%%%%%%%%%%##((##/,,,*(#%%%%%%%%%%%%%%#(*****************
    .*,.,,,**,,,,,,,,,,,,,,,,,,,,,,,,,,*****,,,/(%%%%%%%%#(//(#/,..*/#%%%%%%%%%%%%%%%%%%%%%%%%%%%#(//(#/,..,/(#%%%%%%%%%%%%%%#/*****///////////
    .,..,,,,,,,,,,,,,,,,,,,,,,,,,,*,,*******,,,(#%%%%%%%%#(*,,,....,/#%%%%%%%%%%%%%%%%%%%%%%%%%%%#(*,,,....,/(#%%%%%%%%%%%%%%#(*,**////////////
    .,..,,,,,,,,,...........,,,,,,*,********,,*(#%%%%%%%%%#(/*,,...,/#%%%%%%%%%%%%%%%%%%%%%%%%%%%%#(/*,,..,*/##%%%%%%%%%%%%%%%#(***////////////
    ...,,,,,,,................,,*,**********,,/#%%%%%%%%%%%%#((////((#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%##((///(#%%%%%%%%%%%%%%%%%%(/**////////////
    ..,,,,,,.................,,,**********,,*(#%%%%%%%%%%%%%%%%%%#%%%%%%%%#((///((#%%%%%%%%%%%%%%%%%%%%%#%%%%%%%%%%%%%%%%%%%%%#/**////////////
    .,,,,,,,,.................,,***********,,/(####%%%%%%%%%%%%%%%%%%%%%%%%#(/*,,,*(#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%#(/*////////////
    .,***,,,,,,..............,,,**********,..,***//((##%%%%%%%%%%%%%%%%%%%%%%%##((##%%%%%%%%%%%%%%%%%%%%%%%%%##(((((((((###%%%%%#/**///////////
    .*****,,,,,,,,,,,,,,,,,,,*************,..,*******/(#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%##///*//////((#%%%%%#(**///////////
    .****************/******/***////*****,.,*///////**/#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%#(////////////(#%%%%%#/**//////////
    .***********************/////*******,..,*//////////(#%%%%%%%%%%%%%%%%%%%%##########%%%%%%%%%%%%%%%%%%%%#(///////////*/(#%%%%%#(***/////////
    .************************///********,..,*//////////#%%%%%%%%%%%%%%%%%%#(//*****///(((##%%%%%%%%%%%%%%%%#(///////////**/##%%%%##/***////////
    .***********************************,.,,***///////(#%%%%%%%%%%%%%%%%#(/*,,,*//((((////(#%%%%%%%%%%%%%%%#((////////////(#%%%%%%#(*********//
    ,***********,,,*,,*,,**************,,,*//******//(#%%%%%%%%%%%%%%%%%#(*,,*/(((#####(((((#%%%%%%%%%%%%%%%##///////////(#%%%%%%%%#(***///////
    ,*************,,**,,,************,,,,,/(##((((####%%%%%%%%%%%%%%%%%%%(/**/(((#((((#((//(#%%%%%%%%%%%%%%%%%#(((((((((##%%%%%%%%%%#/**///////
    ,******************************,,,,,,,*(#%#%%%%%%%%%%%%%%%%%%%%%%%%%%#(**/((#(#(((#((//(#%%%%%%%%%%%%%%%%%%%%%%%#%#%%%%%%%%%%%%%#(**///////
    ,*************,**************,****,,,,,/(#%%%%%%%%%%%%%%%%%%%%%%%%%%%%#(/*/((((#((((///(#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%(/*///////
    ,*************************************,*/#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%##(////////////(#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%#/**/////*
    ,******////****///////////////////////***/#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%####(((((((###%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%#(********
    .,*,****///////////////////////////////***/#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%#(/*******
    .,,,,*****//////////////////////////*******(#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%##(*******
    .,,,,,,***********/////////////////********/(#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%(*******
    "
  )
  
  res
}

#* Echo back API status
#* @get /bowie
#* @html
function() {
  paste(
    '<meta http-equiv="refresh" content="0; url=https://en.wikipedia.org/wiki/David_Bowie">'
  )
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
#* @param l Commodity code length
#* @get /yc
function(y = NULL, l = 4) {
  y <- as.integer(y)
  l <- tolower(substr(as.character(l), 1, 3))
  
  if (nchar(y) != 4 | !y %in% 1962:2016) {
    return("The specified year is not a valid integer value.")
    stop()
  }
  
  if (!nchar(l) <= 3 | !l %in% c(4, 6, "all")) {
    return("The specified length is not a valid integer value.")
    stop()
  }
  
  if (l == "all") {
    query <- glue_sql("
                      SELECT *
                      FROM public.hs07_yc
                      WHERE year = {y}
                      ", .con = con)
  } else {
    query <- glue_sql("
                      SELECT *
                      FROM public.hs07_yc
                      WHERE year = {y}
                      AND commodity_code_length = {l}
                      ",
                      .con = con)
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
  
  query <- glue_sql("
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
  
  if (nchar(r) != 3 |
      !r %in% c(countries_data$country_iso, "all")) {
    return("The specified reporter is not a valid ISO code, please check /countries.")
    stop()
  }
  
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
                      ",
                      .con = con)
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
  
  query <- glue_sql("
                    SELECT reporter_iso
                    FROM public.hs07_yr
                    WHERE year = {y}
                    ",
                    .con = con)
  
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
  
  query <- glue_sql("
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
function(y = NULL, r = NULL, l = NULL) {
  y <- as.integer(y)
  r <- tolower(substr(as.character(r), 1, 3))
  l <- tolower(substr(as.character(l), 1, 3))
  
  if (nchar(y) != 4 | !y %in% 1962:2016) {
    return("The specified year is not a valid integer value.")
    stop()
  }
  
  if (nchar(r) != 3 |
      !r %in% c(countries_data$country_iso, "all")) {
    return("The specified reporter is not a valid ISO code, please check /countries.")
    stop()
  }
  
  if (!nchar(l) <= 3 | !l %in% c(4, 6, "all")) {
    return("The specified length is not a valid integer value.")
    stop()
  }
  
  if (r == "all" & l == "all") {
    query <- glue_sql("
                      SELECT *
                      FROM public.hs07_yrc
                      WHERE year = {y}
                      ", .con = con)
  }
  
  if (r == "all" & l != "all") {
    query <- glue_sql("
                      SELECT *
                      FROM public.hs07_yrc
                      WHERE year = {y}
                      AND commodity_code_length = {l}
                      ",
                      .con = con
    )
  }
  
  if (r != "all" & l == "all") {
    query <- glue_sql("
                      SELECT *
                      FROM public.hs07_yrc
                      WHERE year = {y}
                      AND reporter_iso = {r}
                      ",
                      .con = con)
  }
  
  if (r != "all" & l != "all") {
    query <- glue_sql("
                      SELECT *
                      FROM public.hs07_yrc
                      WHERE year = {y}
                      AND reporter_iso = {r}
                      AND commodity_code_length = {l}
                      ",
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
  
  if (nchar(r) != 3 |
      !r %in% c(countries_data$country_iso, "all")) {
    return("The specified reporter is not a valid ISO code, please check /countries.")
    stop()
  }
  
  if (nchar(p) != 3 |
      !p %in% c(countries_data$country_iso, "all")) {
    return("The specified reporter is not a valid ISO code, please check /countries.")
    stop()
  }
  
  if (r == "all" & p == "all") {
    query <- glue_sql("
                      SELECT *
                      FROM public.hs07_yrp
                      WHERE year = {y}
                      ", .con = con)
  }
  
  if (r != "all" & p == "all") {
    query <- glue_sql("
                      SELECT *
                      FROM public.hs07_yrp
                      WHERE year = {y}
                      AND reporter_iso = {r}
                      ",
                      .con = con)
  }
  
  if (r == "all" & p != "all") {
    query <- glue_sql("
                      SELECT *
                      FROM public.hs07_yrp
                      WHERE year = {y}
                      AND partner_iso = {p}
                      ",
                      .con = con)
  }
  
  if (r != "all" & p != "all") {
    query <- glue_sql("
                      SELECT *
                      FROM public.hs07_yrp
                      WHERE year = {y}
                      AND reporter_iso = {r}
                      AND partner_iso = {p}
                      ",
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
function(y = NULL, r = NULL, p = NULL, l = NULL) {
  y <- as.integer(y)
  r <- tolower(substr(as.character(r), 1, 3))
  p <- tolower(substr(as.character(p), 1, 3))
  l <- tolower(substr(as.character(l), 1, 3))
  
  if (nchar(y) != 4 | !y %in% 1962:2016) {
    return("The specified year is not a valid integer value.")
    stop()
  }
  
  if (nchar(r) != 3 |
      !r %in% c(countries_data$country_iso, "all")) {
    return("The specified reporter is not a valid ISO code, please check /countries.")
    stop()
  }
  
  if (nchar(p) != 3 |
      !p %in% c(countries_data$country_iso, "all")) {
    return("The specified reporter is not a valid ISO code, please check /countries.")
    stop()
  }
  
  if (!nchar(l) <= 3 | !l %in% c(4, 6, "all")) {
    return("The specified length is not a valid integer value.")
    stop()
  }
  
  if (r == "all" & p == "all" & l == "all") {
    query <- glue_sql("
                      SELECT *
                      FROM public.hs07_yrpc
                      WHERE year = {y}
                      ", .con = con)
  }
  
  if (r != "all" & p == "all" & l == "all") {
    query <- glue_sql("
                      SELECT *
                      FROM public.hs07_yrpc
                      WHERE year = {y}
                      AND reporter_iso = {r}
                      ",
                      .con = con)
  }
  
  if (r == "all" & p != "all" & l == "all") {
    query <- glue_sql("
                      SELECT *
                      FROM public.hs07_yrpc
                      WHERE year = {y}
                      AND partner_iso = {p}
                      ",
                      .con = con)
  }
  
  if (r == "all" & p == "all" & l != "all") {
    query <- glue_sql("
                      SELECT *
                      FROM public.hs07_yrpc
                      WHERE year = {y}
                      AND commodity_code_length = {l}
                      ",
                      .con = con
    )
  }
  
  if (r != "all" & p != "all" & l == "all") {
    query <- glue_sql("
                      SELECT *
                      FROM public.hs07_yrpc
                      WHERE year = {y}
                      AND reporter_iso = {r}
                      AND partner_iso = {p}
                      ",
                      .con = con
    )
  }
  
  if (r != "all" & p == "all" & l != "all") {
    query <- glue_sql("
                      SELECT *
                      FROM public.hs07_yrpc
                      WHERE year = {y}
                      AND reporter_iso = {r}
                      AND commodity_code_length = {l}
                      ",
                      .con = con
    )
  }
  
  if (r == "all" & p != "all" & l != "all") {
    query <- glue_sql("
                      SELECT *
                      FROM public.hs07_yrpc
                      WHERE year = {y}
                      AND partner_iso = {p}
                      AND commodity_code_length = {l}
                      ",
                      .con = con
    )
  }
  
  if (r != "all" & p != "all" & l != "all") {
    query <- glue_sql("
                      SELECT *
                      FROM public.hs07_yrpc
                      WHERE year = {y}
                      AND reporter_iso = {r}
                      AND partner_iso = {p}
                      AND commodity_code_length = {l}
                      ",
                      .con = con
    )
  }
  
  data <- dbGetQuery(con, query)
  
  return(data)
}
