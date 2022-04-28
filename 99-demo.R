library(readr)
library(purrr)

countries = c("per", "bol", "bra", "chl")

map_df(
  countries,
  function(c) { 
    read_dev(
      sprintf("http://127.0.0.1:8080/yr?y=2019&r=%s", c)
    )
  }
)
