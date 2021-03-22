# queries.R

# Packages ----------------------------------------------------------------

library(pool)
library(RPostgreSQL)
library(data.table)
library(glue)

# Read credentials from file excluded in .gitignore --------------------------------

readRenviron("/tradestatistics/plumber-api")

# DB connection parameters ------------------------------------------------

pool <- dbPool(
  drv = dbDriver("PostgreSQL"),
  dbname = Sys.getenv("dbname"),
  host = Sys.getenv("dbhost"),
  user = Sys.getenv("dbusr"),
  password = Sys.getenv("dbpwd")
)

# Clean inputs ------------------------------------------------------------

clean_char_input <- function(x, i, j) {
  y <- iconv(x, to = "ASCII//TRANSLIT", sub = "")
  y <- gsub("[^[:alpha:]-]", "", y)
  y <- tolower(y)
  y <- substr(y, i, j)
  return(y)
}

clean_num_input <- function(x, i, j) {
  y <- iconv(x, to = "ASCII//TRANSLIT", sub = "")
  y <- ifelse(y == "all", y, gsub("[^[:digit:]]", "", y))
  y <- tolower(y)
  y <- substr(y, i, j)
  return(y)
}

# Available years in the DB -----------------------------------------------

min_year <- function() {
  return(
    as.numeric(dbGetQuery(pool, glue("SELECT MIN(year) FROM public.hs07_yr")))
  )
}

max_year <- function() {
  return(
    as.numeric(dbGetQuery(pool, glue("SELECT MAX(year) FROM public.hs07_yr")))
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
  d2 <- fread("aliases/countries.csv")
  d <- rbind(d, d2, fill = TRUE)
  return(d)
}

#* Echo back the result of a query on attributes_countries table
#* @get /countries

function() { countries() }

# Continents (to filter API parameters) -----------------------------------

# create vectors by continent to filter by using meta variables like americas, africa, etc

## Africa
countries_africa <- as.vector(unlist(countries()[continent == "Africa", .(country_iso)]))

## Americas
countries_americas <- as.vector(unlist(countries()[continent == "Americas", .(country_iso)]))

## Asia
countries_asia <- as.vector(unlist(countries()[continent == "Asia", .(country_iso)]))

## Europe
countries_europe <- as.vector(unlist(countries()[continent == "Europe", .(country_iso)]))

## Oceania
countries_oceania <- as.vector(unlist(countries()[continent == "Oceania", .(country_iso)]))

# Products ----------------------------------------------------------------

products <- function() {
  d <- dbGetQuery(pool, glue("SELECT product_code, product_fullname_english FROM public.attributes_products"))
  d2 <- fread("aliases/products.csv")
  d <- rbind(d, d2)
  return(d)
}

#* Echo back the result of a query on attributes_products table
#* @get /products

function() { products() }

# Sections -------------------------------------------------------------

sections <- function() {
  return(
    dbGetQuery(pool, glue("SELECT * FROM public.attributes_sections"))
  )
}

sections_names <- function() {
  return(
    dbGetQuery(pool, glue("SELECT * FROM public.attributes_sections_names"))
  )
}

sections_shortnames <- function() {
  return(
    dbGetQuery(pool, glue("SELECT * FROM public.attributes_sections_shortnames"))
  )
}

sections_colors <- function() {
  return(
    dbGetQuery(pool, glue("SELECT * FROM public.attributes_sections_colors"))
  )
}

#* Echo back the result of a query on attributes_sections table
#* @get /sections

function() { sections() }

#* Echo back the result of a query on attributes_sections table
#* @get /sections_names

function() { sections_names() }

#* Echo back the result of a query on attributes_sections table
#* @get /sections_shortnames

function() { sections_shortnames() }

#* Echo back the result of a query on attributes_sections table
#* @get /sections_colors

function() { sections_colors() }

# Groups -------------------------------------------------------------

groups <- function() {
  return(
    dbGetQuery(pool, glue("SELECT * FROM public.attributes_groups"))
  )
}

#* Echo back the result of a query on attributes_groups table
#* @get /groups

function() { groups() }

# Product shortnames ------------------------------------------------------

products_shortnames <- function() {
  return(
    dbGetQuery(pool, glue("SELECT * FROM public.attributes_products_shortnames"))
  )
}

#* Echo back the result of a query on attributes_products_shortnames table
#* @get /products_shortnames

function() { products_shortnames() }

# sections shortnames -----------------------------------------------------

sections_shortnames <- function() {
  return(
    dbGetQuery(pool, glue("SELECT * FROM public.attributes_sections_shortnames"))
  )
}

#* Echo back the result of a query on attributes_sections_shortnames table
#* @get /sections_shortnames

function() { sections_shortnames() }

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
    data <- data.table(
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

function(y = NULL) {
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
  
  data <- as.data.table(dbGetQuery(pool, query))
  data <- data[pci_rank_fitness_method > 0][order(pci_rank_fitness_method)]
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
    data <- data.table(
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
    data <- data.table(
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
  					SELECT product_code, LEFT(product_code,2) as group_code FROM public.attributes_products
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
    					SELECT product_code, LEFT(product_code,2) as group_code FROM public.attributes_products
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
    data <- data.table(
      year = y,
      reporter_iso = r,
      observation = "No data available for these filtering parameters"
    )
  }
  
  return(data)
}

# YR-SA ----------------------------------------------------------------------

#* Echo back the result of a query on yrc table
#* @param y Year
#* @param r Reporter ISO
#* @get /yr-sa

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
	   top_export_section_code, top_export_trade_value_usd,
	   top_import_section_code, top_import_trade_value_usd
    FROM(
    	SELECT lhs.year as year, 
    	       lhs.reporter_iso as reporter_iso, 
    	       lhs.export_value_usd as export_value_usd,
    	       lhs.import_value_usd as import_value_usd,
    	       rhsexp.top_export_section_code as top_export_section_code,
    	       rhsexp.top_export_trade_value_usd as top_export_trade_value_usd,
    	       rhsimp.top_import_section_code as top_import_section_code,
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
			   section_code AS top_export_section_code,
			   export_value_usd AS top_export_trade_value_usd
		FROM (
			SELECT reporter_iso, section_code, export_value_usd, MAX(export_value_usd) OVER () AS mev
			FROM (
				SELECT rhsexp12.reporter_iso AS reporter_iso,
					     rhsexp22.section_code AS section_code,
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
  					SELECT product_code, section_code FROM public.attributes_sections
  				) AS rhsexp22
  				ON (rhsexp12.product_code = rhsexp22.product_code)
  				GROUP BY rhsexp12.reporter_iso, rhsexp22.section_code
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
			   section_code AS top_import_section_code,
			   import_value_usd AS top_import_trade_value_usd
		FROM (
			SELECT reporter_iso, section_code, import_value_usd, MAX(import_value_usd) OVER () AS miv
			FROM (
				SELECT rhsimp12.reporter_iso AS reporter_iso,
					   rhsimp22.section_code AS section_code,
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
    					SELECT product_code, section_code FROM public.attributes_sections
    				) as rhsimp22
    				on (rhsimp12.product_code = rhsimp22.product_code)
    				GROUP BY rhsimp12.reporter_iso, rhsimp22.section_code
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
    data <- data.table(
      year = y,
      reporter_iso = r,
      observation = "No data available for these filtering parameters"
    )
  }
  
  return(data)
}

# Reporters ---------------------------------------------------------------

#* Echo back the result of a query on yr table
#* @param y Year
#* @get /reporters

function(y = NULL) {
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

function(y = NULL) {
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
  
  data <- as.data.table(dbGetQuery(pool, query))
  data <- data[eci_rank_fitness_method > 0][order(eci_rank_fitness_method)]
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
    data <- data.table(
      year = y,
      reporter_iso = r,
      product_code = c,
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
    data <- data.table(
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
    data <- data.table(
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
    data <- data.table(
      year = y,
      reporter_iso = r,
      partner_iso = p,
      product_code = c,
      observation = "No data available for these filtering parameters"
    )
  }
  
  return(data)
}

# Available tables --------------------------------------------------------

#* All the tables generated by this API
#* @get /tables

function() {
  data.table(
    table = c(
      "countries",
      "products",
      "reporters",
      "sections",
      "product_shortnames",
      "country_rankings",
      "product_rankings",
      "yrpc",
      "yrp",
      "yrc",
      "yr",
      "yr-short",
      "yr-ga",
      "yr-sa",
      "yc"
    ),
    description = c(
      "Countries metadata",
      "Product metadata",
      "Reporting countries",
      "Product sections",
      "Product short names",
      "Ranking of countries",
      "Ranking of products",
      "Reporter-Partner trade at product level (Year, Reporter, Partner and Product Code)",
      "Reporter-Partner trade at aggregated level (Year, Reporter and Partner)",
      "Reporter trade at product level (Year, Reporter and Product Code)",
      "Reporter trade at aggregated level (Year and Reporter, with top exported/imported Product)",
      "Reporter trade at aggregated level (Year and Reporter, Abridged)",
      "Reporter trade at aggregated level (Year and Reporter, with top exported/imported Group)",
      "Reporter trade at aggregated level (Year and Reporter, with top exported/imported Section)",
      "Product trade at detailed level (Year and Product Code)"
    ),
    source = c(
      rep("UN Comtrade (with modifications)",3),
      "Center for International Development at Harvard University (with modifications)",
      "The Observatory of Economic Complexity (with modifications)",
      rep("Open Trade Statistics",10)
    )
  )
}

# Year range --------------------------------------------------------------

#* All the tables generated by this API
#* @get /year_range

function() {
  data.table(year = c(min_year(), max_year()))
}
