# api.R

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
  if (!is.null(l)) {
    d <- dbGetQuery(con, 
                    sprintf(
                      "
                      SELECT * FROM public.hs07_yrpc WHERE year = %s
                      AND reporter_iso = '%s'
                      AND partner_iso = '%s'
                      ",
                      y, r, p
                    )
    )
  } else {
    d <- dbGetQuery(con, 
                    sprintf(
                      "
                      SELECT * FROM public.hs07_yrpc WHERE year = %s
                      AND reporter_iso = '%s'
                      AND partner_iso = '%s'
                      AND commodity_code_length = '%s'
                      ",
                      y, r, p, l
                    )
    )   
  }
  
  toJSON(d)
}
