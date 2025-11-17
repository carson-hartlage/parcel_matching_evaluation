library(dplyr)
library(sf)
library(addr)
library(lubridate)

#### Franklin County
# https://opendata.columbus.gov/datasets/columbus::code-enforcement-cases/about
hcv_frank <- st_read("/Users/carsonhartlage/Documents/GitHub/parcel_matching_evaluation/Code_Enforcement_Cases/Code_Enforcement_Cases.shp") |>
  filter(B1_PER_TYP == "Housing Code Inspection") |>
  filter(B1_APPL_ST %in% c("PACE - Closed", "Closed")) |>
  filter(!INSP_1ST_R %in% c("Cancelled", "In Compliance", "Void", "Void-See Comment")) |>
  mutate(B1_FILE_DD = as_date(B1_FILE_DD))

frank_zips <- st_read("/Users/carsonhartlage/Documents/GitHub/parcel_matching_evaluation/20251106_Zipcode/ZIPCODES.shp") |>
  select(c(ZIPCODE, geometry))

hcv_frank <- hcv_frank |>
  st_transform(st_crs(frank_zips)) |>
  st_join(frank_zips) |>
  st_drop_geometry() |>
  filter(!is.na(ZIPCODE) & !is.na(SITE_ADDRE) & !is.na(B1_FILE_DD)) |>
  filter(B1_FILE_DD >= "2019-01-01" & B1_FILE_DD <= "2025-10-15") |>
  mutate(addr = addr::addr(glue::glue("{SITE_ADDRE} Anytown XX {ZIPCODE}")))

hcv_frank <- hcv_frank |>
  group_by(addr) |>
  summarise(n_violations = n()) |>
  ungroup() #|>
  #mutate(parcel_id = sub("^(\\d{3})(\\d+)$", "\\1-\\2", B1_PARCEL_))


saveRDS(hcv_frank, "/Users/carsonhartlage/Documents/GitHub/parcel_matching_evaluation/franklin_hcv_by_address.rds")

### Hamilton County
# from parcel package
# ham_hcv <- readRDS("/Users/carsonhartlage/Documents/GitHub/parcel_matching_evaluation/property_code_enforcements_matched_addr.rds") #|>
# ham_hcv <- ham_hcv |>
#   as_tibble() |>
#   filter(date > "2019-01-01") |>
#   filter(!is.na(cagis_parcel_id)) |>
#   group_by(cagis_parcel_id) |>
#   summarise(n_violations = n()) |>
#   ungroup()
# # hcv by parcel, 2019 to 2025
# 
# saveRDS(ham_hcv, "/Users/carsonhartlage/Documents/GitHub/parcel_matching_evaluation/hamilton_hcv_by_parcel.rds")


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

d_addr <- 
  raw_data |>
  filter(date >= "2019-01-01 UTC" & date <= "2025-10-25 UTC") |>
  filter(!is.na(lat_jittered), !is.na(lon_jittered)) |>
  sf::st_as_sf(coords = c("LONGITUDE", "LATITUDE"), crs = 4326) |>
  sf::st_transform(sf::st_crs(cincy::zcta_tigris_2020)) |>
  sf::st_join(cincy::zcta_tigris_2020, largest = TRUE) |>
  sf::st_drop_geometry() |>
  mutate(addr = addr::addr(glue::glue("{address} Anytown XX {zcta_2020}"))) 


hcv_ham <- d_addr |>
  group_by(addr) |>
  summarise(n_violations = n()) |>
  ungroup()

saveRDS(hcv_ham, "/Users/carsonhartlage/Documents/GitHub/parcel_matching_evaluation/hamilton_hcv_by_address.rds")


#####
d <- readRDS("~/Documents/GitHub/parcel_matching_evaluation/NAD_matching_data_11.4.25.rds")
dd <- as.data.frame(d$nad_addr)
dd <- dd |>
  mutate(nad_addr_to_merge = addr::addr(glue::glue("{street_number} {street_name} {street_type} Anytown XX {zip_code}"))) 
d <- d |>
  mutate(nad_addr_to_merge = dd$nad_addr_to_merge)


