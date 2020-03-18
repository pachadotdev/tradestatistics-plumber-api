# queries.R

# Packages ----------------------------------------------------------------

library(pool)
library(RPostgres)
library(dplyr)
library(stringr)
library(glue)

# Read credentials from file excluded in .gitignore --------------------------------

readRenviron("/tradestatistics/plumber-api")

# DB connection parameters ------------------------------------------------

pool <- dbPool(
  drv = Postgres(),
  dbname = Sys.getenv("dbname"),
  host = Sys.getenv("dbhost"),
  user = Sys.getenv("dbusr"),
  password = Sys.getenv("dbpwd")
)

# Clean inputs ------------------------------------------------------------

clean_char_input <- function(x, i, j) {
  y <- iconv(x, to = "ASCII//TRANSLIT", sub = " ")
  y <- str_replace_all(y, "[^[:alpha:]-]", " ")
  y <- str_squish(y)
  y <- str_trim(y)
  y <- str_to_lower(y)
  y <- str_sub(y, i, j)
  
  return(y)
}

clean_num_input <- function(x, i, j) {
  y <- iconv(x, to = "ASCII//TRANSLIT", sub = " ")
  y <- ifelse(y == "all", y, str_replace_all(y, "[^[:digit:]]", " "))
  y <- str_squish(y)
  y <- str_trim(y)
  y <- str_to_lower(y)
  y <- str_sub(y, i, j)
  
  return(y)
}

# Available years in the DB -----------------------------------------------

min_year <- function() {
  return(
    dbGetQuery(pool, glue("SELECT MIN(year) FROM public.hs07_yr")) %>% 
      as.numeric()
  )
}

max_year <- function() {
  return(
    dbGetQuery(pool, glue("SELECT MAX(year) FROM public.hs07_yr")) %>% 
      as.numeric()
  )
}

# Title and description ---------------------------------------------------

#* @apiTitle Open Trade Statistics API
#* @apiDescription Sandbox to experiment with the available functions

# Hello World -------------------------------------------------------------

#* Echo back Hello World!
#* @get /

function() {
  paste("Hello World! Welcome to Open Trade Statistics API")
}

# Countries ---------------------------------------------------------------

countries <- function() {
  d <- dbGetQuery(pool, glue("SELECT * FROM public.attributes_countries"))
  d2 <- data.table::fread("aliases/countries.csv")
  d <- bind_rows(d, d2)
  return(d)
}

#* Echo back the result of a query on attributes_countries table
#* @get /countries

function() { countries() }

# Continents (to filter API parameters) -----------------------------------

# create vectors by continent to filter by using meta variables like americas, africa, etc

## Africa
countries_africa <- countries() %>%
  filter(continent == "Africa") %>%
  select(country_iso) %>%
  as.vector()

## Americas
countries_americas <- countries() %>%
  filter(continent == "Americas") %>%
  select(country_iso) %>%
  as.vector()

## Asia
countries_asia <- countries() %>%
  filter(continent == "Asia") %>%
  select(country_iso) %>%
  as.vector()

## Europe
countries_europe <- countries() %>%
  filter(continent == "Europe") %>%
  select(country_iso) %>%
  as.vector()

## Oceania
countries_oceania <- countries() %>%
  filter(continent == "Oceania") %>%
  select(country_iso) %>%
  as.vector()

# Products ----------------------------------------------------------------

products <- function() {
  d <- dbGetQuery(pool, glue("SELECT * FROM public.attributes_products"))
  d2 <- data.table::fread("aliases/products.csv")
  d <- bind_rows(d, d2)
  return(d)
}

#* Echo back the result of a query on attributes_products table
#* @get /products

function() { products() }

# Communities -------------------------------------------------------------

communities <- function() {
  return(
    dbGetQuery(pool, glue("SELECT * FROM public.attributes_communities"))
  )
}

#* Echo back the result of a query on attributes_communities table
#* @get /communities

function() { communities() }

# Product shortnames ------------------------------------------------------

products_shortnames <- function() {
  return(
    dbGetQuery(pool, glue("SELECT * FROM public.attributes_products_shortnames"))
  )
}

#* Echo back the result of a query on attributes_products_shortnames table
#* @get /product_shortnames

function() { products_shortnames() }

# YC ----------------------------------------------------------------------

#* Echo back the result of a query on yc table
#* @param y Year
#* @param c Product code
#* @get /yc

function(y = NULL, c = "all") {
  y <- as.integer(y)
  c <- clean_num_input(c, 1, 4)
  
  if (nchar(y) != 4 | !y >= min_year() | !y <= max_year()) {
    return("The specified year is not a valid integer value. Read the documentation: tradestatistics.io")
    stop()
  }
  
  if (!nchar(c) <= 4 | !c %in% products()$product_code) {
    return("The specified product code is not a valid string. Read the documentation: tradestatistics.io")
    stop()
  }
  
  query <- glue(
    "
    SELECT *
    FROM public.hs07_yc
    WHERE year = {y}
    "
  )
  
  if (c != "all" & nchar(c) != 2) {
    query <- glue(
      query,
      " AND product_code = '{c}'",
    )
  }
  
  if (c != "all" & nchar(c) == 2) {
    query <- glue(
      query,
      " AND LEFT(product_code, 2) = '{c}'"
    )
  }
  
  data <- dbGetQuery(pool, query)
  
  if (nrow(data) == 0) {
    data <- tibble(
      year = y,
      product_code = c,
      observation = "No data available for these filtering parameters"
    )
  }
  
  return(data)
}

# Product rankings --------------------------------------------------------

#* Echo back the result of a query on yr table
#* @param y Year
#* @get /product_rankings

function(y = 2017) {
  y <- as.integer(y)
  
  if (nchar(y) != 4 | !y >= min_year() | !y <= max_year()) {
    return("The specified year is not a valid integer value.")
    stop()
  }
  
  stopifnot(is.integer(y))
  
  query <- glue(
    "
    SELECT year, product_code, pci_fitness_method, pci_rank_fitness_method
    FROM public.hs07_yc
    WHERE year = {y}
    "
  )
  
  data <- dbGetQuery(pool, query)
  
  data <- data %>%
    filter(pci_rank_fitness_method > 0) %>% 
    arrange(pci_rank_fitness_method)
  
  return(data)
}

# YR ----------------------------------------------------------------------

#* Echo back the result of a query on yr table
#* @param y Year
#* @param r Reporter ISO
#* @get /yr

function(y = NULL, r = NULL) {
  y <- as.integer(y)
  r <- clean_char_input(r, 1, 4)
  
  if (nchar(y) != 4 | !y >= min_year() | !y <= max_year()) {
    return("The specified year is not a valid integer value. Read the documentation: tradestatistics.io")
    stop()
  }
  
  if (!nchar(r) <= 4 | !r %in% c(countries()$country_iso)) {
    return("The specified reporter is not a valid ISO code or alias. Read the documentation: tradestatistics.io")
    stop()
  }
  
  query <- glue(
    "
    SELECT * 
    FROM public.hs07_yr 
    WHERE year = {y}
    "
  )
  
  if (r != "all" & nchar(r) == 3) {
    query <- glue(
      query,
      " AND reporter_iso = '{r}'"
    )
  }
  
  if (r != "all" & nchar(r) == 4) {
    r2 <- switch(
      r,
      "c-af" = countries_africa,
      "c-am" = countries_americas,
      "c-as" = countries_asia,
      "c-eu" = countries_europe,
      "c-oc" = countries_oceania
    )
    
    r2 <- sprintf("'%s'", r2$country_iso)
    
    query <- glue(
      query,
      " AND reporter_iso IN ({glue_collapse(r2,  sep = ', ')})"
    )
  }
  
  data <- dbGetQuery(pool, query)
  
  if (nrow(data) == 0) {
    data <- tibble(
      year = y,
      reporter_iso = r,
      observation = "No data available for these filtering parameters"
    )
  }
  
  return(data)
}

# YR short ----------------------------------------------------------------

#* Echo back the result of a query on yr table
#* @param y Year
#* @param r Reporter ISO
#* @get /yr-short

function(y = NULL, r = NULL) {
  y <- as.integer(y)
  r <- clean_char_input(r, 1, 4)
  
  if (nchar(y) != 4 | !y >= min_year() | !y <= max_year()) {
    return("The specified year is not a valid integer value. Read the documentation: tradestatistics.io")
    stop()
  }
  
  if (!nchar(r) <= 4 | !r %in% c(countries()$country_iso)) {
    return("The specified reporter is not a valid ISO code or alias. Read the documentation: tradestatistics.io")
    stop()
  }
  
  query <- glue(
    "
    SELECT year, reporter_iso, export_value_usd, import_value_usd,
      top_export_product_code, top_export_trade_value_usd,
      top_import_product_code, top_import_trade_value_usd
    FROM public.hs07_yr 
    WHERE year = {y}
    "
  )
  
  if (r != "all" & nchar(r) == 3) {
    query <- glue(
      query,
      " AND reporter_iso = '{r}'"
    )
  }
  
  if (r != "all" & nchar(r) == 4) {
    r2 <- switch(
      r,
      "c-af" = countries_africa,
      "c-am" = countries_americas,
      "c-as" = countries_asia,
      "c-eu" = countries_europe,
      "c-oc" = countries_oceania
    )
    
    r2 <- sprintf("'%s'", r2$country_iso)
    
    query <- glue(
      query,
      " AND reporter_iso IN ({glue_collapse(r2,  sep = ', ')})"
    )
  }
  
  data <- dbGetQuery(pool, query)
  
  if (nrow(data) == 0) {
    data <- tibble(
      year = y,
      reporter_iso = r,
      observation = "No data available for these filtering parameters"
    )
  }
  
  return(data)
}

# YR-GA ----------------------------------------------------------------------

#* Echo back the result of a query on yrc table
#* @param y Year
#* @param r Reporter ISO
#* @get /yr-ga

function(y = NULL, r = NULL) {
  y <- as.integer(y)
  r <- clean_char_input(r, 1, 4)
  
  if (nchar(y) != 4 | !y >= min_year() | !y <= max_year()) {
    return("The specified year is not a valid integer value. Read the documentation: tradestatistics.io")
    stop()
  }
  
  if (!nchar(r) <= 4 | !r %in% c(countries()$country_iso)) {
    return("The specified reporter is not a valid ISO code or alias. Read the documentation: tradestatistics.io")
    stop()
  }
  
  query <- glue(
    "
    SELECT year, reporter_iso, export_value_usd, import_value_usd,
	   top_export_group_code, top_export_trade_value_usd,
	   top_import_group_code, top_import_trade_value_usd
    FROM(
    	SELECT lhs.year as year, 
    	       lhs.reporter_iso as reporter_iso, 
    	       lhs.export_value_usd as export_value_usd,
    	       lhs.import_value_usd as import_value_usd,
    	       rhsexp.top_export_group_code as top_export_group_code,
    	       rhsexp.top_export_trade_value_usd as top_export_trade_value_usd,
    	       rhsimp.top_import_group_code as top_import_group_code,
    		   rhsimp.top_import_trade_value_usd as top_import_trade_value_usd
    	FROM (
    		SELECT year, reporter_iso, export_value_usd, import_value_usd
    		FROM public.hs07_yr
        WHERE year = {y}
    "
  )
  
  if (r != "all" & nchar(r) == 3) {
    query <- glue(
      query,
      " AND reporter_iso = '{r}'"
    )
  }
  
  if (r != "all" & nchar(r) == 4) {
    r2 <- switch(
      r,
      "c-af" = countries_africa,
      "c-am" = countries_americas,
      "c-as" = countries_asia,
      "c-eu" = countries_europe,
      "c-oc" = countries_oceania
    )
    
    r2 <- sprintf("'%s'", r2$country_iso)
    
    query <- glue(
      query,
      " AND reporter_iso IN ({glue_collapse(r2,  sep = ', ')})"
    )
  }
  
  query <- glue(
    query,
    "
    ) AS lhs
    LEFT JOIN (
		SELECT reporter_iso,
			   group_code AS top_export_group_code,
			   export_value_usd AS top_export_trade_value_usd
		FROM (
			SELECT reporter_iso, group_code, export_value_usd, MAX(export_value_usd) OVER () AS mev
			FROM (
				SELECT rhsexp12.reporter_iso AS reporter_iso,
					     rhsexp22.group_code AS group_code,
					     SUM(export_value_usd) AS export_value_usd
				FROM (
					SELECT reporter_iso, product_code, export_value_usd
					FROM public.hs07_yrc
					WHERE year = {y}
    "
  )
  
  if (r != "all" & nchar(r) == 3) {
    query <- glue(
      query,
      " AND reporter_iso = '{r}'"
    )
  }
  
  if (r != "all" & nchar(r) == 4) {
    r2 <- switch(
      r,
      "c-af" = countries_africa,
      "c-am" = countries_americas,
      "c-as" = countries_asia,
      "c-eu" = countries_europe,
      "c-oc" = countries_oceania
    )
    
    r2 <- sprintf("'%s'", r2$country_iso)
    
    query <- glue(
      query,
      " AND reporter_iso IN ({glue_collapse(r2,  sep = ', ')})"
    )
  }
  
  query <- glue(
    query,
    "
  				) AS rhsexp12
  				INNER JOIN (
  					SELECT product_code, group_code FROM public.attributes_products
  				) AS rhsexp22
  				ON (rhsexp12.product_code = rhsexp22.product_code)
  				GROUP BY rhsexp12.reporter_iso, rhsexp22.group_code
  			) rhsexpnf
  		) AS rhsexpf
  		WHERE export_value_usd = mev
  	) AS rhsexp
  	ON (lhs.reporter_iso = rhsexp.reporter_iso)
    "
  )
  
  query <- glue(
    query,
    "
    LEFT JOIN (
		SELECT reporter_iso,
			   group_code AS top_import_group_code,
			   import_value_usd AS top_import_trade_value_usd
		FROM (
			SELECT reporter_iso, group_code, import_value_usd, MAX(import_value_usd) OVER () AS miv
			FROM (
				SELECT rhsimp12.reporter_iso AS reporter_iso,
					   rhsimp22.group_code AS group_code,
					   SUM(import_value_usd) AS import_value_usd
				FROM (
					SELECT reporter_iso,
						   product_code,
						   import_value_usd
					FROM public.hs07_yrc
					WHERE year = {y}
    "
  )
  
  if (r != "all" & nchar(r) == 3) {
    query <- glue(
      query,
      " AND reporter_iso = '{r}'"
    )
  }
  
  if (r != "all" & nchar(r) == 4) {
    r2 <- switch(
      r,
      "c-af" = countries_africa,
      "c-am" = countries_americas,
      "c-as" = countries_asia,
      "c-eu" = countries_europe,
      "c-oc" = countries_oceania
    )
    
    r2 <- sprintf("'%s'", r2$country_iso)
    
    query <- glue(
      query,
      " AND reporter_iso IN ({glue_collapse(r2,  sep = ', ')})"
    )
  }
  
  query <- glue(
    query,
    "
    				) as rhsimp12
    				inner join (
    					SELECT product_code, group_code FROM public.attributes_products
    				) as rhsimp22
    				on (rhsimp12.product_code = rhsimp22.product_code)
    				GROUP BY rhsimp12.reporter_iso, rhsimp22.group_code
    			) rhsimpnf
    		) AS rhsimpf
    		WHERE import_value_usd = miv
    	) as rhsimp
    	ON (lhs.reporter_iso = rhsimp.reporter_iso)
    ) AS res
    "
  )
  
  data <- dbGetQuery(pool, query)
  
  if (nrow(data) == 0) {
    data <- tibble(
      year = y,
      reporter_iso = r,
      observation = "No data available for these filtering parameters"
    )
  }
  
  return(data)
}

# YR-CA ----------------------------------------------------------------------

#* Echo back the result of a query on yrc table
#* @param y Year
#* @param r Reporter ISO
#* @get /yr-ca

function(y = NULL, r = NULL) {
  y <- as.integer(y)
  r <- clean_char_input(r, 1, 4)
  
  if (nchar(y) != 4 | !y >= min_year() | !y <= max_year()) {
    return("The specified year is not a valid integer value. Read the documentation: tradestatistics.io")
    stop()
  }
  
  if (!nchar(r) <= 4 | !r %in% c(countries()$country_iso)) {
    return("The specified reporter is not a valid ISO code or alias. Read the documentation: tradestatistics.io")
    stop()
  }
  
  query <- glue(
    "
    SELECT year, reporter_iso, export_value_usd, import_value_usd,
	   top_export_community_code, top_export_trade_value_usd,
	   top_import_community_code, top_import_trade_value_usd
    FROM(
    	SELECT lhs.year as year, 
    	       lhs.reporter_iso as reporter_iso, 
    	       lhs.export_value_usd as export_value_usd,
    	       lhs.import_value_usd as import_value_usd,
    	       rhsexp.top_export_community_code as top_export_community_code,
    	       rhsexp.top_export_trade_value_usd as top_export_trade_value_usd,
    	       rhsimp.top_import_community_code as top_import_community_code,
    		   rhsimp.top_import_trade_value_usd as top_import_trade_value_usd
    	FROM (
    		SELECT year, reporter_iso, export_value_usd, import_value_usd
    		FROM public.hs07_yr
        WHERE year = {y}
    "
  )
  
  if (r != "all" & nchar(r) == 3) {
    query <- glue(
      query,
      " AND reporter_iso = '{r}'"
    )
  }
  
  if (r != "all" & nchar(r) == 4) {
    r2 <- switch(
      r,
      "c-af" = countries_africa,
      "c-am" = countries_americas,
      "c-as" = countries_asia,
      "c-eu" = countries_europe,
      "c-oc" = countries_oceania
    )
    
    r2 <- sprintf("'%s'", r2$country_iso)
    
    query <- glue(
      query,
      " AND reporter_iso IN ({glue_collapse(r2,  sep = ', ')})"
    )
  }
  
  query <- glue(
    query,
    "
    ) AS lhs
    LEFT JOIN (
		SELECT reporter_iso,
			   community_code AS top_export_community_code,
			   export_value_usd AS top_export_trade_value_usd
		FROM (
			SELECT reporter_iso, community_code, export_value_usd, MAX(export_value_usd) OVER () AS mev
			FROM (
				SELECT rhsexp12.reporter_iso AS reporter_iso,
					     rhsexp22.community_code AS community_code,
					     SUM(export_value_usd) AS export_value_usd
				FROM (
					SELECT reporter_iso, product_code, export_value_usd
					FROM public.hs07_yrc
					WHERE year = {y}
    "
  )
  
  if (r != "all" & nchar(r) == 3) {
    query <- glue(
      query,
      " AND reporter_iso = '{r}'"
    )
  }
  
  if (r != "all" & nchar(r) == 4) {
    r2 <- switch(
      r,
      "c-af" = countries_africa,
      "c-am" = countries_americas,
      "c-as" = countries_asia,
      "c-eu" = countries_europe,
      "c-oc" = countries_oceania
    )
    
    r2 <- sprintf("'%s'", r2$country_iso)
    
    query <- glue(
      query,
      " AND reporter_iso IN ({glue_collapse(r2,  sep = ', ')})"
    )
  }
  
  query <- glue(
    query,
    "
  				) AS rhsexp12
  				INNER JOIN (
  					SELECT product_code, community_code FROM public.attributes_communities
  				) AS rhsexp22
  				ON (rhsexp12.product_code = rhsexp22.product_code)
  				GROUP BY rhsexp12.reporter_iso, rhsexp22.community_code
  			) rhsexpnf
  		) AS rhsexpf
  		WHERE export_value_usd = mev
  	) AS rhsexp
  	ON (lhs.reporter_iso = rhsexp.reporter_iso)
    "
  )
  
  query <- glue(
    query,
    "
    LEFT JOIN (
		SELECT reporter_iso,
			   community_code AS top_import_community_code,
			   import_value_usd AS top_import_trade_value_usd
		FROM (
			SELECT reporter_iso, community_code, import_value_usd, MAX(import_value_usd) OVER () AS miv
			FROM (
				SELECT rhsimp12.reporter_iso AS reporter_iso,
					   rhsimp22.community_code AS community_code,
					   SUM(import_value_usd) AS import_value_usd
				FROM (
					SELECT reporter_iso,
						   product_code,
						   import_value_usd
					FROM public.hs07_yrc
					WHERE year = {y}
    "
  )
  
  if (r != "all" & nchar(r) == 3) {
    query <- glue(
      query,
      " AND reporter_iso = '{r}'"
    )
  }
  
  if (r != "all" & nchar(r) == 4) {
    r2 <- switch(
      r,
      "c-af" = countries_africa,
      "c-am" = countries_americas,
      "c-as" = countries_asia,
      "c-eu" = countries_europe,
      "c-oc" = countries_oceania
    )
    
    r2 <- sprintf("'%s'", r2$country_iso)
    
    query <- glue(
      query,
      " AND reporter_iso IN ({glue_collapse(r2,  sep = ', ')})"
    )
  }
  
  query <- glue(
    query,
    "
    				) as rhsimp12
    				inner join (
    					SELECT product_code, community_code FROM public.attributes_communities
    				) as rhsimp22
    				on (rhsimp12.product_code = rhsimp22.product_code)
    				GROUP BY rhsimp12.reporter_iso, rhsimp22.community_code
    			) rhsimpnf
    		) AS rhsimpf
    		WHERE import_value_usd = miv
    	) as rhsimp
    	ON (lhs.reporter_iso = rhsimp.reporter_iso)
    ) AS res
    "
  )
  
  data <- dbGetQuery(pool, query)
  
  if (nrow(data) == 0) {
    data <- tibble(
      year = y,
      reporter_iso = r,
      observation = "No data available for these filtering parameters"
    )
  }
  
  return(data)
}

# Reporters ---------------------------------------------------------------

#* Echo back the result of a query on yr table
#* @get /reporters

function(y = 2017) {
  y <- as.integer(y)
  
  if (nchar(y) != 4 | !y >= min_year() | !y <= max_year()) {
    return("The specified year is not a valid integer value.")
    stop()
  }
  
  query <- glue(
    "
    SELECT reporter_iso
    FROM public.hs07_yr
    WHERE year = {y}
    "
  )
  
  data <- dbGetQuery(pool, query)
  
  return(data)
}

# Country rankings --------------------------------------------------------

#* Echo back the result of a query on yr table
#* @param y Year
#* @get /country_rankings

function(y = 2017) {
  y <- as.integer(y)
  
  if (nchar(y) != 4 | !y >= min_year() | !y <= max_year()) {
    return("The specified year is not a valid integer value.")
    stop()
  }
  
  stopifnot(is.integer(y))
  
  query <- glue(
    "
    SELECT year, reporter_iso, eci_fitness_method, eci_rank_fitness_method
    FROM public.hs07_yr
    WHERE year = {y}
    "
  )
  
  data <- dbGetQuery(pool, query)
  
  data <- data %>%
    filter(eci_rank_fitness_method > 0) %>% 
    arrange(eci_rank_fitness_method)
  
  return(data)
}

# YRC ---------------------------------------------------------------------

#* Echo back the result of a query on yrc table
#* @param y Year
#* @param r Reporter ISO
#* @param c Product code
#* @get /yrc

function(y = NULL, r = NULL, c = "all") {
  y <- as.integer(y)
  r <- clean_char_input(r, 1, 4)
  c <- clean_num_input(c, 1, 4)
  
  if (nchar(y) != 4 | !y >= min_year() | !y <= max_year()) {
    return("The specified year is not a valid integer value. Read the documentation: tradestatistics.io")
    stop()
  }
  
  if (!nchar(r) <= 4 | !r %in% c(countries()$country_iso)) {
    return("The specified reporter is not a valid ISO code or alias. Read the documentation: tradestatistics.io")
    stop()
  }
  
  if (!nchar(c) <= 4 | !c %in% products()$product_code) {
    return("The specified product code is not a valid string. Read the documentation: tradestatistics.io")
    stop()
  }
  
  query <- glue(
    "
    SELECT *
    FROM public.hs07_yrc
    WHERE year = {y}
    "
  )
  
  if (r != "all" & nchar(r) == 3) {
    query <- glue(
      query,
      " AND reporter_iso = '{r}'"
    )
  }
  
  if (r != "all" & nchar(r) == 4) {
    r2 <- switch(
      r,
      "c-af" = countries_africa,
      "c-am" = countries_americas,
      "c-as" = countries_asia,
      "c-eu" = countries_europe,
      "c-oc" = countries_oceania
    )
    
    r2 <- sprintf("'%s'", r2$country_iso)
    
    query <- glue(
      query,
      " AND reporter_iso IN ({glue_collapse(r2,  sep = ', ')})"
    )
  }
  
  if (c != "all" & nchar(c) != 2) {
    query <- glue(
      query,
      " AND product_code = '{c}'"
    )
  }
  
  if (c != "all" & nchar(c) == 2) {
    query <- glue(
      query,
      " AND LEFT(product_code, 2) = '{c}'"
    )
  }
  
  data <- dbGetQuery(pool, query)
  
  if (nrow(data) == 0) {
    data <- tibble(
      year = y,
      reporter_iso = r,
      product_code = c,
      observation = "No data available for these filtering parameters"
    )
  }
  
  return(data)
}

# YRC-GA --------------------------------------------------------------------

#* Echo back the result of a query on yrc table
#* @param y Year
#* @param r Reporter ISO
#* @param g Product group
#* @get /yrc-ga

function(y = NULL, r = NULL, g = "all") {
  y <- as.integer(y)
  r <- clean_char_input(r, 1, 4)
  g <- clean_num_input(g, 1, 3)
  
  if (nchar(y) != 4 | !y >= min_year() | !y <= max_year()) {
    return("The specified year is not a valid integer value. Read the documentation: tradestatistics.io")
    stop()
  }
  
  if (!nchar(r) <= 4 | !r %in% c(countries()$country_iso)) {
    return("The specified reporter is not a valid ISO code or alias. Read the documentation: tradestatistics.io")
    stop()
  }
  
  product_group <- dbGetQuery(pool, "SELECT DISTINCT(group_code) FROM public.attributes_products") %>% 
    pull()
  
  if (!nchar(g) <= 3 | !g %in% c(product_group, "all")) {
    return("The specified product group is not a valid string. Read the documentation: tradestatistics.io")
    stop()
  }
  
  query <- glue(
    "
    SELECT year, reporter_iso, group_code, 
           SUM(export_value_usd) AS export_value_usd,
           SUM(import_value_usd) AS import_value_usd
    FROM(
    SELECT lhs.year as year, 
           lhs.reporter_iso as reporter_iso, 
           lhs.product_code as product_code, 
           lhs.export_value_usd as export_value_usd,
           lhs.import_value_usd as import_value_usd,
           rhs.group_code as group_code
    FROM (
    SELECT *
    FROM public.hs07_yrc
    WHERE year = {y}
    "
  )
  
  if (r != "all" & nchar(r) == 3) {
    query <- glue(
      query,
      " AND reporter_iso = '{r}'"
    )
  }
  
  if (r != "all" & nchar(r) == 4) {
    r2 <- switch(
      r,
      "c-af" = countries_africa,
      "c-am" = countries_americas,
      "c-as" = countries_asia,
      "c-eu" = countries_europe,
      "c-oc" = countries_oceania
    )
    
    r2 <- sprintf("'%s'", r2$country_iso)
    
    query <- glue(
      query,
      " AND reporter_iso IN ({glue_collapse(r2,  sep = ', ')})"
    )
  }
  
  if (nchar(g) == 2) {
    query <- glue(
      query,
      " AND LEFT(product_code, 2) = '{g}'"
    )
  }
  
  query <- glue(
    query,
    "
    ) AS lhs
    INNER JOIN (SELECT product_code, group_code FROM public.attributes_products) AS rhs
    ON (lhs.product_code = rhs.product_code)
    ) AS res
    GROUP BY year, reporter_iso, group_code
    ORDER BY group_code
    "
  )
  
  data <- dbGetQuery(pool, query)
  
  if (nrow(data) == 0) {
    data <- tibble(
      year = y,
      reporter_iso = r,
      group_code = g,
      observation = "No data available for these filtering parameters"
    )
  }
  
  return(data)
}

# YRC-CA --------------------------------------------------------------------

#* Echo back the result of a query on yrc table
#* @param y Year
#* @param r Reporter ISO
#* @param o Product community
#* @get /yrc-ca

function(y = NULL, r = NULL, o = "all") {
  y <- as.integer(y)
  r <- clean_char_input(r, 1, 4)
  o <- clean_num_input(o, 1, 3)
  
  if (nchar(y) != 4 | !y >= min_year() | !y <= max_year()) {
    return("The specified year is not a valid integer value. Read the documentation: tradestatistics.io")
    stop()
  }
  
  if (!nchar(r) <= 4 | !r %in% c(countries()$country_iso)) {
    return("The specified reporter is not a valid ISO code or alias. Read the documentation: tradestatistics.io")
    stop()
  }
  
  product_community <- dbGetQuery(pool, glue("SELECT DISTINCT(community_code) FROM public.attributes_communities")) %>% 
    pull()
  
  if (!nchar(o) <= 3 | !o %in% c(product_community, "all")) {
    return("The specified product community is not a valid string. Read the documentation: tradestatistics.io")
    stop()
  }
  
  query <- glue(
    "
    SELECT year, reporter_iso, community_code, 
           SUM(export_value_usd) AS export_value_usd,
           SUM(import_value_usd) AS import_value_usd
    FROM(
    SELECT lhs.year as year, 
           lhs.reporter_iso as reporter_iso, 
           lhs.product_code as product_code, 
           lhs.export_value_usd as export_value_usd,
           lhs.import_value_usd as import_value_usd,
           rhs.community_code as community_code
    FROM (
    SELECT *
    FROM public.hs07_yrc
    WHERE year = {y}
    "
  )
  
  if (r != "all" & nchar(r) == 3) {
    query <- glue(
      query,
      " AND reporter_iso = '{r}'"
    )
  }
  
  if (r != "all" & nchar(r) == 4) {
    r2 <- switch(
      r,
      "c-af" = countries_africa,
      "c-am" = countries_americas,
      "c-as" = countries_asia,
      "c-eu" = countries_europe,
      "c-oc" = countries_oceania
    )
    
    r2 <- sprintf("'%s'", r2$country_iso)
    
    query <- glue(
      query,
      " AND reporter_iso IN ({glue_collapse(r2,  sep = ', ')})"
    )
  }
  
  if (nchar(o) == 2) {
    query <- glue(
      query,
      " AND LEFT(product_code, 2) = '{o}'"
    )
  }
  
  query <- glue(
    query,
    "
    ) AS lhs
    INNER JOIN (SELECT product_code, community_code FROM public.attributes_communities) AS rhs
    ON (lhs.product_code = rhs.product_code)
    ) AS res
    GROUP BY year, reporter_iso, community_code
    ORDER BY community_code
    "
  )
  
  data <- dbGetQuery(pool, query)
  
  if (nrow(data) == 0) {
    data <- tibble(
      year = y,
      reporter_iso = r,
      community_code = o,
      observation = "No data available for these filtering parameters"
    )
  }
  
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
  r <- clean_char_input(r, 1, 4)
  p <- clean_char_input(p, 1, 4)
  
  if (nchar(y) != 4 | !y >= min_year() | !y <= max_year()) {
    return("The specified year is not a valid integer value. Read the documentation: tradestatistics.io")
    stop()
  }
  
  if (!nchar(r) <= 4 | !r %in% c(countries()$country_iso)) {
    return("The specified reporter is not a valid ISO code or alias. Read the documentation: tradestatistics.io")
    stop()
  }
  
  if (!nchar(p) <= 4 | !p %in% c(countries()$country_iso)) {
    return("The specified partner is not a valid ISO code or alias. Read the documentation: tradestatistics.io")
    stop()
  }
  
  query <- glue("SELECT * FROM public.hs07_yrp WHERE year = {y}")
  
  if (r != "all" & nchar(r) == 3) {
    query <- glue(
      query,
      " AND reporter_iso = '{r}'"
    )
  }
  
  if (r != "all" & nchar(r) == 4) {
    r2 <- switch(
      r,
      "c-af" = countries_africa,
      "c-am" = countries_americas,
      "c-as" = countries_asia,
      "c-eu" = countries_europe,
      "c-oc" = countries_oceania
    )
    
    r2 <- sprintf("'%s'", r2$country_iso)
    
    query <- glue(
      query,
      " AND reporter_iso IN ({glue_collapse(r2,  sep = ', ')})"
    )
  }
  
  if (p != "all" & nchar(p) == 3) {
    query <- glue(
      query,
      " AND partner_iso = '{p}'"
    )
  }
  
  if (p != "all" & nchar(p) == 4) {
    p2 <- switch(
      p,
      "c-af" = countries_africa,
      "c-am" = countries_americas,
      "c-as" = countries_asia,
      "c-eu" = countries_europe,
      "c-oc" = countries_oceania
    )
    
    p2 <- sprintf("'%s'", p2$country_iso)
    
    query <- glue(
      query,
      " AND reporter_iso IN ({glue_collapse(p2,  sep = ', ')})"
    )
  }
  
  data <- dbGetQuery(pool, query)
  
  if (nrow(data) == 0) {
    data <- tibble(
      year = y,
      reporter_iso = r,
      partner_iso = p,
      observation = "No data available for these filtering parameters"
    )
  }
  
  return(data)
}

# YRPC --------------------------------------------------------------------

#* Echo back the result of a query on yrpc table
#* @param y Year
#* @param r Reporter ISO
#* @param p Partner ISO
#* @param c Product code
#* @get /yrpc

function(y = NULL, r = NULL, p = NULL, c = "all") {
  y <- as.integer(y)
  r <- clean_char_input(r, 1, 4)
  p <- clean_char_input(p, 1, 4)
  c <- clean_num_input(c, 1, 4)
  
  if (nchar(y) != 4 | !y >= min_year() | !y <= max_year()) {
    return("The specified year is not a valid integer value. Read the documentation: tradestatistics.io")
    stop()
  }
  
  if (!nchar(r) <= 4 | !r %in% c(countries()$country_iso)) {
    return("The specified reporter is not a valid ISO code or alias. Read the documentation: tradestatistics.io")
    stop()
  }
  
  if (!nchar(p) <= 4 | !p %in% c(countries()$country_iso)) {
    return("The specified partner is not a valid ISO code or alias. Read the documentation: tradestatistics.io")
    stop()
  }
  
  if (!nchar(c) <= 4 | !c %in% products()$product_code) {
    return("The specified product code is not a valid string. Read the documentation: tradestatistics.io")
    stop()
  }
  
  if (all(c(r, p , c) == "all")) {
    data <- tibble(
      year = y,
      reporter_iso = r,
      partner_iso = p,
      product_code = c,
      observation = "You are better off downloading the compressed datasets from docs.tradestatistics.io/accesing-the-data.html"
    )
    
    return(data)
  }
  
  query <- glue(
    "
    SELECT *
    FROM public.hs07_yrpc
    WHERE year = {y}
    "
  )
  
  if (r != "all" & nchar(r) == 3) {
    query <- glue(
      query,
      " AND reporter_iso = '{r}'"
    )
  }
  
  if (r != "all" & nchar(r) == 4) {
    r2 <- switch(
      r,
      "c-af" = countries_africa,
      "c-am" = countries_americas,
      "c-as" = countries_asia,
      "c-eu" = countries_europe,
      "c-oc" = countries_oceania
    )
    
    r2 <- sprintf("'%s'", r2$country_iso)
    
    query <- glue(
      query,
      " AND reporter_iso IN ({glue_collapse(r2,  sep = ', ')})"
    )
  }
  
  if (p != "all" & nchar(p) == 3) {
    query <- glue(
      query,
      " AND partner_iso = '{p}'"
    )
  }
  
  if (p != "all" & nchar(p) == 4) {
    p2 <- switch(
      p,
      "c-af" = countries_africa,
      "c-am" = countries_americas,
      "c-as" = countries_asia,
      "c-eu" = countries_europe,
      "c-oc" = countries_oceania
    )
    
    p2 <- sprintf("'%s'", p2$country_iso)
    
    query <- glue(
      query,
      " AND reporter_iso IN ({glue_collapse(p2,  sep = ', ')})"
    )
  }
  
  if (c != "all" & nchar(c) != 2) {
    query <- glue(
      query,
      " AND product_code = '{c}'"
    )
  }
  
  if (c != "all" & nchar(c) == 2) {
    query <- glue(
      query,
      " AND LEFT(product_code, 2) = '{c}'"
    )
  }
  
  data <- dbGetQuery(pool, query)
  
  if (nrow(data) == 0) {
    data <- tibble(
      year = y,
      reporter_iso = r,
      partner_iso = p,
      product_code = c,
      observation = "No data available for these filtering parameters"
    )
  }
  
  return(data)
}

# YRPC-GA --------------------------------------------------------------------

#* Echo back the result of a query on yrpc table
#* @param y Year
#* @param r Reporter ISO
#* @param p Partner ISO
#* @param g Product group
#* @get /yrpc-ga

function(y = NULL, r = NULL, p = NULL, g = "all") {
  y <- as.integer(y)
  r <- clean_char_input(r, 1, 4)
  p <- clean_char_input(p, 1, 4)
  g <- clean_num_input(g, 1, 3)
  
  if (nchar(y) != 4 | !y >= min_year() | !y <= max_year()) {
    return("The specified year is not a valid integer value. Read the documentation: tradestatistics.io")
    stop()
  }
  
  if (!nchar(r) <= 4 | !r %in% c(countries()$country_iso)) {
    return("The specified reporter is not a valid ISO code or alias. Read the documentation: tradestatistics.io")
    stop()
  }
  
  if (!nchar(p) <= 4 | !p %in% c(countries()$country_iso)) {
    return("The specified partner is not a valid ISO code or alias. Read the documentation: tradestatistics.io")
    stop()
  }
  
  product_group <- dbGetQuery(pool, "SELECT DISTINCT(group_code) FROM public.attributes_products") %>% 
    pull()
  
  if (!nchar(g) <= 3 | !g %in% c(product_group, "all")) {
    return("The specified product group is not a valid string. Read the documentation: tradestatistics.io")
    stop()
  }
  
  query <- glue(
    "
    SELECT year, reporter_iso, partner_iso, group_code, 
           SUM(export_value_usd) AS export_value_usd,
           SUM(import_value_usd) AS import_value_usd
    FROM(
    SELECT lhs.year as year, 
           lhs.reporter_iso as reporter_iso, 
           lhs.partner_iso as partner_iso,
           lhs.product_code as product_code, 
           lhs.export_value_usd as export_value_usd,
           lhs.import_value_usd as import_value_usd,
           rhs.group_code as group_code
    FROM (
    SELECT *
    FROM public.hs07_yrpc
    WHERE year = {y}
    "
  )
  
  if (r != "all" & nchar(r) == 3) {
    query <- glue(
      query,
      " AND reporter_iso = '{r}'"
    )
  }
  
  if (r != "all" & nchar(r) == 4) {
    r2 <- switch(
      r,
      "c-af" = countries_africa,
      "c-am" = countries_americas,
      "c-as" = countries_asia,
      "c-eu" = countries_europe,
      "c-oc" = countries_oceania
    )
    
    r2 <- sprintf("'%s'", r2$country_iso)
    
    query <- glue(
      query,
      " AND reporter_iso IN ({glue_collapse(r2,  sep = ', ')})"
    )
  }
  
  if (p != "all" & nchar(p) == 3) {
    query <- glue(
      query,
      " AND partner_iso = '{p}'"
    )
  }
  
  if (p != "all" & nchar(p) == 4) {
    p2 <- switch(
      p,
      "c-af" = countries_africa,
      "c-am" = countries_americas,
      "c-as" = countries_asia,
      "c-eu" = countries_europe,
      "c-oc" = countries_oceania
    )
    
    p2 <- sprintf("'%s'", p2$country_iso)
    
    query <- glue(
      query,
      " AND reporter_iso IN ({glue_collapse(p2,  sep = ', ')})"
    )
  }
  
  if (nchar(g) == 2) {
    query <- glue(
      query,
      " AND LEFT(product_code, 2) = '{g}'"
    )
  }
  
  query <- glue(
    query,
    "
    ) AS lhs
    INNER JOIN (SELECT product_code, group_code FROM public.attributes_products) AS rhs
    ON (lhs.product_code = rhs.product_code)
    ) AS res
    GROUP BY year, reporter_iso, partner_iso, group_code
    ORDER BY group_code
    "
  )
  
  data <- dbGetQuery(pool, query)
  
  if (nrow(data) == 0) {
    data <- tibble(
      year = y,
      reporter_iso = r,
      partner_iso = p,
      group_code = g,
      observation = "No data available for these filtering parameters"
    )
  }
  
  return(data)
}

# YRPC-CA --------------------------------------------------------------------

#* Echo back the result of a query on yrpc table
#* @param y Year
#* @param r Reporter ISO
#* @param p Partner ISO
#* @param o Product community
#* @get /yrpc-ca

function(y = NULL, r = NULL, p = NULL, o = "all") {
  y <- as.integer(y)
  r <- clean_char_input(r, 1, 4)
  p <- clean_char_input(p, 1, 4)
  o <- clean_num_input(o, 1, 3)
  
  if (nchar(y) != 4 | !y >= min_year() | !y <= max_year()) {
    return("The specified year is not a valid integer value. Read the documentation: tradestatistics.io")
    stop()
  }
  
  if (!nchar(r) <= 4 | !r %in% c(countries()$country_iso)) {
    return("The specified reporter is not a valid ISO code or alias. Read the documentation: tradestatistics.io")
    stop()
  }
  
  if (!nchar(p) <= 4 | !p %in% c(countries()$country_iso)) {
    return("The specified partner is not a valid ISO code or alias. Read the documentation: tradestatistics.io")
    stop()
  }
  
  product_community <- dbGetQuery(pool, glue("SELECT DISTINCT(community_code) FROM public.attributes_communities")) %>% 
    pull()
  
  if (!nchar(o) <= 3 | !o %in% c(product_community, "all")) {
    return("The specified product community is not a valid string. Read the documentation: tradestatistics.io")
    stop()
  }
  
  query <- glue(
    "
    SELECT year, reporter_iso, partner_iso, community_code, 
           SUM(export_value_usd) AS export_value_usd,
           SUM(import_value_usd) AS import_value_usd
    FROM(
    SELECT lhs.year as year, 
           lhs.reporter_iso as reporter_iso, 
           lhs.partner_iso as partner_iso,
           lhs.product_code as product_code, 
           lhs.export_value_usd as export_value_usd,
           lhs.import_value_usd as import_value_usd,
           rhs.community_code as community_code
    FROM (
    SELECT *
    FROM public.hs07_yrpc
    WHERE year = {y}
    "
  )
  
  if (r != "all" & nchar(r) == 3) {
    query <- glue(
      query,
      " AND reporter_iso = '{r}'"
    )
  }
  
  if (r != "all" & nchar(r) == 4) {
    r2 <- switch(
      r,
      "c-af" = countries_africa,
      "c-am" = countries_americas,
      "c-as" = countries_asia,
      "c-eu" = countries_europe,
      "c-oc" = countries_oceania
    )
    
    r2 <- sprintf("'%s'", r2$country_iso)
    
    query <- glue(
      query,
      " AND reporter_iso IN ({glue_collapse(r2,  sep = ', ')})"
    )
  }
  
  if (p != "all" & nchar(p) == 3) {
    query <- glue(
      query,
      " AND partner_iso = '{p}'"
    )
  }
  
  if (p != "all" & nchar(p) == 4) {
    p2 <- switch(
      p,
      "c-af" = countries_africa,
      "c-am" = countries_americas,
      "c-as" = countries_asia,
      "c-eu" = countries_europe,
      "c-oc" = countries_oceania
    )
    
    p2 <- sprintf("'%s'", p2$country_iso)
    
    query <- glue(
      query,
      " AND reporter_iso IN ({glue_collapse(p2,  sep = ', ')})"
    )
  }
  
  if (nchar(o) == 2) {
    query <- glue(
      query,
      " AND LEFT(product_code, 2) = '{o}'"
    )
  }
  
  query <- glue(
    query,
    "
    ) AS lhs
    INNER JOIN (SELECT product_code, community_code FROM public.attributes_communities) AS rhs
    ON (lhs.product_code = rhs.product_code)
    ) AS res
    GROUP BY year, reporter_iso, partner_iso, community_code
    ORDER BY community_code
    "
  )
  
  data <- dbGetQuery(pool, query)
  
  if (nrow(data) == 0) {
    data <- tibble(
      year = y,
      reporter_iso = r,
      partner_iso = p,
      community_code = o,
      observation = "No data available for these filtering parameters"
    )
  }
  
  return(data)
}

# Available tables --------------------------------------------------------

#* All the tables generated by this API
#* @get /tables

function() {
  tibble(
    table = c(
      "countries",
      "products",
      "reporters",
      "communities",
      "product_shortnames",
      "country_rankings",
      "product_rankings",
      "yrpc",
      "yrpc-ga",
      "yrpc-ca",
      "yrp",
      "yrc",
      "yrc-ga",
      "yrc-ca",
      "yr",
      "yr-short",
      "yc"
    ),
    description = c(
      "Countries metadata",
      "Product metadata",
      "Reporting countries",
      "Product communities",
      "Product short names",
      "Ranking of countries",
      "Ranking of products",
      "Bilateral trade at product level (Year, Reporter, Partner and Product Code)",
      "Bilateral trade at group level (Year, Reporter, Partner and Product Group)",
      "Bilateral trade at community level (Year, Reporter, Partner and Product Community)",
      "Bilateral trade at aggregated level (Year, Reporter and Partner)",
      "Multilateral trade at product level (Year, Reporter and Product Code)",
      "Multilateral trade at group level (Year, Reporter and Product Group)",
      "Multilateral trade at community level (Year, Reporter and Product Community)",
      "Multilateral trade at aggregated level (Year and Reporter)",
      "Multilateral trade at aggregated level (Year and Reporter, Abridged)",
      "Product trade at detailed level (Year and Product Code)"
    ),
    source = c(
      rep("UN Comtrade (with modifications)",3),
      "Center for International Development at Harvard University (with modifications)",
      "The Observatory of Economic Complexity (with modifications)",
      rep("Open Trade Statistics",12)
    )
  )
}

# Year range --------------------------------------------------------------

#* All the tables generated by this API
#* @get /year_range

function() {
  tibble(year = c(min_year(), max_year()))
}
