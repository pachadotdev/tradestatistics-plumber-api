# static.R

# Packages ----------------------------------------------------------------

library(dplyr)
library(glue)
library(RPostgreSQL)

# Read credentials from file in .gitignore --------------------------------

readRenviron("/api")

# DB connection parameters ------------------------------------------------

drv <- dbDriver("PostgreSQL") # choose the driver

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

# Countries ---------------------------------------------------------------

countries_query <- glue_sql(
  "
  SELECT *
  FROM public.attributes_countries
  ",
  .con = con
)

countries_data <- dbGetQuery(con, countries_query)

continents_data <- tibble(
  country_iso = c(paste0("c-", c("af", "am", "as", "eu", "oc")), "all"),
  country_name_english = c(
    "Alias for all valid ISO codes in Africa",
    "Alias for all valid ISO codes in the Americas",
    "Alias for all valid ISO codes in Asia",
    "Alias for all valid ISO codes in Europe",
    "Alias for all valid ISO codes in Oceania",
    "Alias for all valid ISO codes in the World"
  )
)

ots_attributes_countries <- countries_data %>%
  bind_rows(continents_data)

# Products ----------------------------------------------------------------

products_query <- glue_sql(
  "
  SELECT *
  FROM public.attributes_products
  ",
  .con = con
)

products_data <- dbGetQuery(con, products_query)

groups_data <- products_data %>%
  select(group_code, group_name) %>%
  distinct() %>%
  mutate(group_name = paste("Alias for all codes in the group", group_name)) %>%
  add_row(group_code = "all", group_name = "Alias for all codes") %>%
  rename(
    product_code = group_code,
    product_fullname_english = group_name
  ) %>%
  arrange(product_code)

ots_attributes_products <- products_data %>%
  bind_rows(groups_data)

# Communities -------------------------------------------------------------

communities_query <- glue_sql(
  "
  SELECT *
  FROM public.attributes_communities
  ",
  .con = con
)

ots_attributes_communities <- dbGetQuery(con, communities_query)

# Product shortnames ------------------------------------------------------

products_shotnames_query <- glue_sql(
  "
  SELECT *
  FROM public.attributes_products_shortnames
  ",
  .con = con
)

ots_attributes_product_shortnames <- dbGetQuery(con, products_shotnames_query)

# Tables ------------------------------------------------------------------

ots_attributes_tables <- tibble(
  table = c(
    "countries",
    "products",
    "reporters",
    "communities",
    "product_shortnames",
    "country_rankings",
    "product_rankings",
    "yrpc",
    "yrp",
    "yrc",
    "yrc_exports",
    "yrc_imports",
    "yr",
    "yr_short",
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
    "Bilateral trade at aggregated level (Year, Reporter and Partner)",
    "Bilateral trade at aggregated level (Year, Reporter and Partner), exports only",
    "Bilateral trade at aggregated level (Year, Reporter and Partner), imports only",
    "Reporter trade at product level (Year, Reporter and Product Code)",
    "Reporter trade at aggregated level (Year and Reporter)",
    "Reporter trade at aggregated level (Year and Reporter), imports and exports only",
    "Product trade at aggregated level (Year and Product Code)"
  ),
  source = c(
    rep("UN Comtrade", 3),
    "Center for International Development at Harvard University",
    "The Observatory of Economic Complexity (with modifications)",
    "UN Comtrade",
    rep("Open Trade Statistics", 9)
  )
)
