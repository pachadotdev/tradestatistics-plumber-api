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

#* Echo back the input
#* @param y Year
#* @param r Reporter ISO
#* @param p Partner ISO
#* @param l Commodity code length
#* @get /yrpc
function(y = 2015, r = "chl", p = "chn", l = NULL) {
  query <- glue_sql("
                SELECT *
                FROM public.hs07_yrpc
                WHERE year = {y}
                AND reporter_iso = {r}
                AND partner_iso = {p}
                LIMIT 10
                ", .con = con)

  data <- dbGetQuery(con, query)
  
  toJSON(data)
}
