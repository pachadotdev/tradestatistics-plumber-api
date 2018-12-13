# api.R

library(glue)
library(RPostgreSQL)
library(jsonlite)

drv <- dbDriver("PostgreSQL")
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

#* Echo back the input
#* @param msg The message to echo
#* @get /echo
function(msg=""){
  list(msg = paste0("The message is: '", msg, "'"))
}

#* Echo back the result of a query on yrpc table
#* @param y Year
#* @param r Reporter ISO
#* @param p Partner ISO
#* @param l Commodity code length
#* @get /yrpc

function(y = 2015, r = "chl", p = "chn", l = NULL) {
  if (is.null(l)) {
    query <- glue_sql("
                      SELECT *
                      FROM public.hs07_yrpc
                      WHERE year = {y}
                      AND reporter_iso = {r}
                      AND partner_iso = {p}
                      ", .con = con)
  } else {
    query <- glue_sql("
                      SELECT *
                      FROM public.hs07_yrpc
                      WHERE year = {y}
                      AND reporter_iso = {r}
                      AND partner_iso = {p}
                      AND commodity_code_length = {l}
                      ", .con = con)
  }
  
  data <- dbGetQuery(con, query)
  
  toJSON(data)
  }

#* Echo back the result of a query on attributes_countries table
#* @get /countries

function() {
  query <- glue_sql("
                    SELECT *
                    FROM public.attributes_country_names
                    ", .con = con)
  
  data <- dbGetQuery(con, query)
  
  toJSON(data)
}

#* Echo back the result of a query on attributes_countries table
#* @get /products

function() {
  query <- glue_sql("
                    SELECT *
                    FROM public.attributes_product_names
                    ", .con = con)
  
  data <- dbGetQuery(con, query)
  
  toJSON(data)
}
