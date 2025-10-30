library(dplyr)
library(purrr)
library(tidyr)
library(sf)
library(ham)

d <- qs::qread("/Users/carsonhartlage/Documents/GitHub/parcel_matching_evaluation/geomatched_NAD_to_parcels.qs") |>
  select(-c(degauss_which_intersects_parcel, degauss_which_closest_parcel, nad_which_intersects_parcel, nad_which_closest_parcel))

d <- d |>
  mutate(nad_intersects_parcel_id = lapply(nad_intersects_parcel_id, function(x) {
    if (length(x) == 0) NA_character_ else x}),
    degauss_intersects_parcel_id = lapply(degauss_intersects_parcel_id, function(x) {
      if (length(x) == 0) NA_character_ else x}),
    nad_intersects_parcel_land_use = lapply(nad_intersects_parcel_land_use, function(x) {
      if (length(x) == 0) NA_character_ else x}),
    degauss_intersects_parcel_land_use = lapply(degauss_intersects_parcel_land_use, function(x) {
      if (length(x) == 0) NA_character_ else x}))

d <- d |>
  mutate(
    cagis_addr_data = map(cagis_addr_data, ~ if (is.null(.x) || nrow(.x) == 0) tibble(cagis_parcel_id = NA_character_) else .x["cagis_parcel_id"])
  ) |>
  unnest(cagis_addr_data, keep_empty = TRUE) |>
  rename(addr_matched_parcel_id = cagis_parcel_id)
#####
# join data for addr matches

frank <- filter(d, county == "Franklin, OH")
ham <- filter(d, county == "Hamilton, OH")
ham <- ham |>
  mutate(
    addr_matched_parcel_id = paste0(sub("^0", "", addr_matched_parcel_id), "00")
  )

rm(d)

hamilton_parcels <-
  dpkg::stow("gh://geomarker-io/parcel/cagis_parcels-v1.1.1") |>
  arrow::read_parquet() |>
  select(parcel_id, land_use, market_total_value)

ham <- ham |>
  left_join(hamilton_parcels, join_by(addr_matched_parcel_id == parcel_id)) |>
  rename(addr_matched_parcel_land_use = land_use,
         addr_matched_parcel_value = market_total_value) |>
  left_join(hamilton_parcels, join_by(nad_closest_parcel_id == parcel_id)) |>
  rename(nad_closest_parcel_value = market_total_value) |>
  left_join(hamilton_parcels, join_by(degauss_closest_parcel_id == parcel_id)) |>
  rename(degauss_closest_parcel_value = market_total_value) |>
  select(-c(land_use.x, land_use.y)) |>
  mutate(addr_matched_parcel_id = ifelse(is.na(addr_matched_parcel_land_use), NA, addr_matched_parcel_id))

lu_keepers <-
  c(
    "single family dwelling" = "510",
    "two family dwelling" = "520",
    "three family dwelling" = "530",
    "condominium unit" = "550",
    "apartment, 4-19 units" = "401",
    "apartment, 20-39 units" = "402",
    "apartment, 40+ units" = "403",
    "mobile home / trailer park" = "415",
    "other commercial housing" = "419",
    "office / apartment over" = "431",
    "boataminium" = "551",
    "landominium" = "555",
    "manufactured home" = "560",
    "other residential structure" = "599",
    "condo or pud garage" = "552",
    "metropolitan housing authority" = "645",
    "lihtc res" = "569"
  )
lu_map <- setNames(names(lu_keepers), lu_keepers)
franklin_parcels <-
  paste0(
    "/vsizip/",
    dpkg::stow(
      "https://apps.franklincountyauditor.com/GIS_Shapefiles/CurrentExtracts/20251008_Parcel_Polygons.zip"
    )
  ) |>
  read_sf() |>
  st_drop_geometry() |>
  filter(CLASSCD %in% lu_keepers) |>
  select(parcel_id = PARCELID, land_use = CLASSCD) |>
  mutate(land_use = recode(land_use, !!!lu_map))

f2 <- readr::read_csv("https://apps.franklincountyauditor.com/Parcel_CSV/2025/07/Parcel.csv")
f2 <- f2 |>
  select(parcel_id = "PARCEL ID",
         appraised_total_value = APPRTOT) |>
  mutate(appraised_total_value = as.numeric(appraised_total_value))

frank <- frank |>
  left_join(franklin_parcels, join_by(addr_matched_parcel_id == parcel_id)) |>
  rename(addr_matched_parcel_land_use = land_use) |>
  left_join(f2, join_by(addr_matched_parcel_id == parcel_id)) |>
  rename(addr_matched_parcel_value = appraised_total_value) |>
  left_join(f2, join_by(nad_closest_parcel_id == parcel_id)) |>
  rename(nad_closest_parcel_value = appraised_total_value) |>
  left_join(f2, join_by(degauss_closest_parcel_id == parcel_id)) |>
  rename(degauss_closest_parcel_value = appraised_total_value) |>
  mutate(addr_matched_parcel_id = ifelse(is.na(addr_matched_parcel_land_use), NA, addr_matched_parcel_id))

# join back together
# all <- rbind(ham, frank)

getMode <- function(x) {
  ux <- unique(x)
  tab <- tabulate(match(x, ux))
  max_tab <- max(tab)
  modes <- ux[tab == max_tab]
  sample(modes, 1)
}
#####
ham <- ham %>%
  group_by(nad_address) %>%
  summarise(
    nad_addr = first(nad_addr),
    nad_lat = first(nad_lat),
    nad_lon = first(nad_lon),
    county = first(county),
    
    n_addr_matches = sum(!is.na(addr_matched_parcel_id) & !is.na(addr_matched_parcel_land_use)),
    
    addr_matched_parcel_list = list(addr_matched_parcel_id),
    addr_matched_parcel_value = mean(addr_matched_parcel_value, na.rm = TRUE),
    addr_matched_parcel_land_use = getMode(addr_matched_parcel_land_use),
    
    nad_closest_parcel_id = first(nad_closest_parcel_id),
    nad_closest_parcel_land_use = first(nad_closest_parcel_land_use),
    nad_closest_parcel_value = first(nad_closest_parcel_value),
    nad_dist_to_closest_parcel = first(nad_dist_to_closest_parcel),
    
    degauss_closest_parcel_id = first(degauss_closest_parcel_id),
    degauss_closest_parcel_land_use = first(degauss_closest_parcel_land_use),
    degauss_closest_parcel_value = first(degauss_closest_parcel_value),
    degauss_dist_to_closest_parcel = first(degauss_dist_to_closest_parcel),
    
    nad_intersects_parcel_id = list(nad_intersects_parcel_id),
    nad_intersects_parcel_land_use = list(nad_intersects_parcel_land_use),
    degauss_intersects_parcel_id = list(degauss_intersects_parcel_id),
    degauss_intersects_parcel_land_use = list(degauss_intersects_parcel_land_use),
    
    nad_intersects_parcel_id = list(nad_intersects_parcel_id),
    degauss_intersects_parcel_id = list(degauss_intersects_parcel_id),
    
    .groups = "drop"
  )

frank <- frank %>%
  group_by(nad_address) %>%
  summarise(
    nad_addr = first(nad_addr),
    nad_lat = first(nad_lat),
    nad_lon = first(nad_lon),
    county = first(county),
    
    n_addr_matches = sum(!is.na(addr_matched_parcel_id) & !is.na(addr_matched_parcel_land_use)),
    
    addr_matched_parcel_list = list(addr_matched_parcel_id),
    addr_matched_parcel_value = mean(addr_matched_parcel_value, na.rm = TRUE),
    addr_matched_parcel_land_use = getMode(addr_matched_parcel_land_use),
    
    nad_closest_parcel_id = first(nad_closest_parcel_id),
    nad_closest_parcel_land_use = first(nad_closest_parcel_land_use),
    nad_closest_parcel_value = first(nad_closest_parcel_value),
    nad_dist_to_closest_parcel = first(nad_dist_to_closest_parcel),
    
    degauss_closest_parcel_id = first(degauss_closest_parcel_id),
    degauss_closest_parcel_land_use = first(degauss_closest_parcel_land_use),
    degauss_closest_parcel_value = first(degauss_closest_parcel_value),
    degauss_dist_to_closest_parcel = first(degauss_dist_to_closest_parcel),
    
    nad_intersects_parcel_id = list(nad_intersects_parcel_id),
    nad_intersects_parcel_land_use = list(nad_intersects_parcel_land_use),
    degauss_intersects_parcel_id = list(degauss_intersects_parcel_id),
    degauss_intersects_parcel_land_use = list(degauss_intersects_parcel_land_use),
    
    .groups = "drop"
  )

####
get_mode_chr <- function(x) {
  x <- as.character(x)
  x <- x[!is.na(x)]
  if (length(x) == 0) return(NA_character_)
  ux <- unique(x)
  tab <- tabulate(match(x, ux))
  max_tab <- max(tab)
  modes <- ux[tab == max_tab]
  sample(modes, 1)
}

parcel_values <- hamilton_parcels |>
  select(c("parcel_id", "market_total_value"))
parcel_values_vec <- setNames(hamilton_parcels$market_total_value, hamilton_parcels$parcel_id)

ham <- ham |> 
  mutate(
    nad_intersects_value = map_dbl(nad_intersects_parcel_id, ~ mean(parcel_values_vec[unlist(.x)], na.rm = TRUE)),
    nad_intersects_land_use = map_chr(nad_intersects_parcel_land_use, ~ get_mode_chr(unlist(.x))),
    n_parcels_nad_intersect = map_int(nad_intersects_parcel_id, ~ length(unlist(.x))),
    degauss_intersects_value = map_dbl(degauss_intersects_parcel_id, ~ mean(parcel_values_vec[unlist(.x)], na.rm = TRUE)),
    degauss_intersects_land_use = map_chr(degauss_intersects_parcel_land_use, ~ get_mode_chr(unlist(.x))),
    n_parcels_degauss_intersect = map_int(degauss_intersects_parcel_id, ~ length(unlist(.x)))
  )



parcel_values <- f2 |>
  select(c("parcel_id", "appraised_total_value"))
parcel_values_vec <- setNames(f2$appraised_total_value, f2$parcel_id)

frank <- frank |> 
  mutate(
    nad_intersects_value = map_dbl(nad_intersects_parcel_id, ~ mean(parcel_values_vec[unlist(.x)], na.rm = TRUE)),
    nad_intersects_land_use = map_chr(nad_intersects_parcel_land_use, ~ get_mode_chr(unlist(.x))),
    n_parcels_nad_intersect = map_int(nad_intersects_parcel_id, ~ length(unlist(.x))),
    degauss_intersects_value = map_dbl(degauss_intersects_parcel_id, ~ mean(parcel_values_vec[unlist(.x)], na.rm = TRUE)),
    degauss_intersects_land_use = map_chr(degauss_intersects_parcel_land_use, ~ get_mode_chr(unlist(.x))),
    n_parcels_degauss_intersect = map_int(degauss_intersects_parcel_id, ~ length(unlist(.x)))
  )

# combine and export
all <- bind_rows(ham, frank)
saveRDS(all, "~/Documents/GitHub/parcel_matching_evaluation/NAD_matching_data_10.30.25.rds")

### regenerate frank - problem with degauss_intersect_land_use

#
saveRDS(ham, "~/Documents/GitHub/parcel_matching_evaluation/NAD_matching_data_ham_10.30.25.rds")
saveRDS(frank, "~/Documents/GitHub/parcel_matching_evaluation/NAD_matching_data_frank_10.30.25.rds")





#saveRDS(all, "~/Documents/GitHub/parcel_matching_evaluation/NAD_matching_data_10.13.25.rds")


dep_index <- 'https://github.com/geomarker-io/dep_index/raw/master/2023/data/ACS_deprivation_index_by_census_tracts.rds' %>% 
  url() %>% 
  gzcon() %>% 
  readRDS() %>% 
  as_tibble()

tracts <- dep_index |>
  filter(substr(census_tract_id_2020, 1, 2) == "39",
         substr(census_tract_id_2020, 3, 5) %in% c("049", "061"))

options(tigris_use_cache = TRUE)
franklin_tracts <- tracts(state = "39", county = "049", year = 2020, class = "sf")
hamilton_tracts <- tracts(state = "39", county = "061", year = 2020, class = "sf")
both <- rbind(franklin_tracts, hamilton_tracts)

tracts <- tracts |>
  left_join(both, join_by("census_tract_id_2020" == "GEOID")) |>
  st_as_sf() |>
  select(c(census_tract_id_2020, dep_index))
  
all_geo <- all_summary |>
  st_as_sf(coords = c("nad_lon", "nad_lat"), crs = 4269) |>
  st_transform(st_crs(tracts)) |>
  st_join(tracts) #|>
  #st_drop_geometry()


####
# all_geo <- all_summary |>
#   st_as_sf(coords = c("nad_lon", "nad_lat"), crs = 4269)
all_geo <- st_transform(all_geo, 26916)

chunk_size <- 50000
n <- nrow(all_geo)
n_within_100m <- numeric(n)

for (i in seq(1, n, by = chunk_size)) {
  message("Processing rows ", i, " to ", min(i + chunk_size - 1, n))
  end_i <- min(i + chunk_size - 1, n)
  sub <- all_geo[i:end_i, ]
  
  nearby <- st_is_within_distance(sub, all_geo, dist = 100)
  n_within_100m[i:end_i] <- lengths(nearby) - 1
}

all_geo$addresses_within_100m <- n_within_100m

all_geo <- all_geo |>
  st_drop_geometry()

saveRDS(all_geo, "~/Documents/GitHub/parcel_matching_evaluation/NAD_matching_data_10.21.25.rds")

####
# hcv_ham <- readRDS("/Users/carsonhartlage/Desktop/PhD/Parcel Matching/property_code_enforcements_matched_addr.rds")
# class(hcv_ham) <- "data.frame"
# hcv_ham <- hcv_ham |>
#   mutate(date = as.Date(date)) |>
#   filter(date >= as.Date("2019-01-02") & date <= as.Date("2025-08-03"))

hcv_frank <- read.csv("/Users/carsonhartlage/Desktop/PhD/Parcel Matching/Columbus_Code_Enforcement_Cases.csv") |>
  filter(B1_PER_SUB_TYPE == "Residential") |>
  mutate(B1_FILE_DD = as_date(ymd_hms(B1_FILE_DD, tz = "UTC")))


### from parcel package
code_enforcement_url <- "https://data.cincinnati-oh.gov/api/views/cncm-znd6/rows.csv?accessType=DOWNLOAD"

raw_data <-
  readr::read_csv(
    code_enforcement_url,
    col_types = readr::cols_only(
      SUB_TYPE_DESC = "character",
      NUMBER_KEY = "character",
      ENTERED_DATE = readr::col_datetime(format = "%m/%d/%Y %I:%M:%S %p"),
      FULL_ADDRESS = "character",
      LATITUDE = "numeric",
      LONGITUDE = "numeric",
      DATA_STATUS_DISPLAY = "character"
    )
  ) |>
  filter(
    !DATA_STATUS_DISPLAY %in% c(
      "Closed - No Violation",
      "Closed - No Violations Found",
      "Duplicate Case",
      "Closed - Duplicate Complaint"
    )
  ) |>
  mutate(
    description = stringr::str_to_lower(SUB_TYPE_DESC), 
    lat_jittered = LATITUDE,
    lon_jittered = LONGITUDE
  ) |> 
  select(
    id = NUMBER_KEY, 
    date = ENTERED_DATE,
    description,
    status = DATA_STATUS_DISPLAY,
    address = FULL_ADDRESS,
    lat_jittered,
    LATITUDE,
    lon_jittered,
    LONGITUDE,
  ) |>
  filter(address != "")

ham_hcv_addr <- 
  raw_data |>
  filter(!is.na(lat_jittered), !is.na(lon_jittered)) |>
  sf::st_as_sf(coords = c("LONGITUDE", "LATITUDE"), crs = 4326) |>
  sf::st_transform(sf::st_crs(cincy::zcta_tigris_2020)) |>
  sf::st_join(cincy::zcta_tigris_2020, largest = TRUE) |>
  sf::st_drop_geometry() |>
  mutate(addr = addr::addr(glue::glue("{address} Anytown XX {zcta_2020}"))) 

ham_hcv_addr <- ham_hcv_addr |>
  mutate(date = as.Date(date)) |>
  filter(date > "2020-10-20" & date <= "2025-10-20") |>
  group_by(addr) |>
  summarise(n_violations = n()) |>
  ungroup()

saveRDS(ham_hcv_addr, "~/Documents/GitHub/parcel_matching_evaluation/ham_HCV_10.21.25.rds")
