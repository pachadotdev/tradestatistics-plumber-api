# run API -----------------------------------------------------------------

api <- plumber::plumb("queries.R")
api$run(port = 4949, host = "0.0.0.0")
