FROM rocker/r-ver:3.5.1

MAINTAINER Mauricio Vargas S. <mvargas@dcc.uchile.cl>

# required packages
RUN apt-get update && apt-get -y install libssl-dev libcurl4-gnutls-dev libpq-dev
RUN R -e "install.packages('pacman')"
RUN R -e "pacman::p_load(RPostgreSQL, glue, jsonlite, plumber)"

# copy everything from the current directory into the container
COPY / /

# open port 8080 to traffic
EXPOSE 8080

# run API script
ENTRYPOINT ["Rscript", "main.R"]
