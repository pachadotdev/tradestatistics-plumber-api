pacman::p_load(plumber)

r <- plumb("queries.R")
r$run(port = 8080, host = "0.0.0.0")
