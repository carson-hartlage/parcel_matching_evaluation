library(dplyr)
library(sf)
library(addr)

#### Franklin County
# https://opendata.columbus.gov/datasets/columbus::code-enforcement-cases/about
hcv_frank <- st_read("/Users/carsonhartlage/Documents/GitHub/parcel_matching_evaluation/Code_Enforcement_Cases/Code_Enforcement_Cases.shp") |>
  filter(B1_PER_TYP == "Housing Code Inspection") |>
  filter(B1_APPL_ST %in% c("PACE - Closed", "Closed")) |>
  filter(!INSP_1ST_R %in% c("Cancelled", "In Compliance", "Void", "Void-See Comment")) |>
  mutate(B1_FILE_DD = as_date(ymd_hms(B1_FILE_DD, tz = "UTC")))

frank_zips <- st_read("/Users/carsonhartlage/Documents/GitHub/parcel_matching_evaluation/20251106_Zipcode/ZIPCODES.shp") |>
  select(c(ZIPCODE, geometry))

hcv_frank <- hcv_frank |>
  st_transform(st_crs(frank_zips)) |>
  st_join(frank_zips) |>
  st_drop_geometry() |>
  filter(!is.na(ZIPCODE) & !is.na(SITE_ADDRE)) |>
  mutate(addr = addr::addr(glue::glue("{SITE_ADDRE} Columbus OH {ZIPCODE}")))


saveRDS(hcv_frank, "/Users/carsonhartlage/Documents/GitHub/parcel_matching_evaluation/franklin_hcv.rds")

### Hamilton County
# from parcel package
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
  filter(!is.na(lat_jittered), !is.na(lon_jittered)) |>
  sf::st_as_sf(coords = c("LONGITUDE", "LATITUDE"), crs = 4326) |>
  sf::st_transform(sf::st_crs(cincy::zcta_tigris_2020)) |>
  sf::st_join(cincy::zcta_tigris_2020, largest = TRUE) |>
  sf::st_drop_geometry() |>
  mutate(addr = addr::addr(glue::glue("{address} Anytown XX {zcta_2020}"))) 

# match with addr::cagis_addr reference addresses included in the package
d_addr$cagis_addr <- addr_match(
  x = d_addr$addr, 
  ref_addr = cagis_addr()$cagis_addr,
  stringdist_match = "osa_lt_1", 
  match_street_type = TRUE, 
  simplify = TRUE
)

# if not matched using zcta, match using addr_match_street_name_and_number()
unmatched <- 
  d_addr |>
  filter(is.na(cagis_addr)) |>
  select(addr) |>
  distinct()

unmatched_cagis_addr <- 
  purrr::map2(
    seq(from = 1, to = nrow(unmatched), by = 1000),
    c(seq(from = 1000, to = nrow(unmatched), by = 1000), nrow(unmatched)), 
    \(x,y)
    addr_match_street_name_and_number(
      x = unmatched$addr[x:y], 
      ref_addr = cagis_addr()$cagis_addr, 
      stringdist_match = "osa_lt_1", 
      match_street_type = TRUE, 
      simplify = TRUE
    ), 
    .progress = TRUE
  )

unmatched$cagis_addr <- purrr::list_c(unmatched_cagis_addr, ptype = addr())

# row bind addrs matched using both methods
d_addr_rematch <- 
  d_addr |>
  filter(is.na(cagis_addr)) |>
  select(-cagis_addr) |>
  left_join(unmatched, by = "addr") 

d_addr <-
  d_addr |>
  filter(!is.na(cagis_addr)) |>
  bind_rows(d_addr_rematch) |>
  arrange(date)

# join to cagis_addr_data by and randomly select parcel id
d <- 
  d_addr |> 
  left_join(cagis_addr(), by = "cagis_addr") |>
  mutate(cagis_parcel_id = purrr::map(cagis_addr_data, "cagis_parcel_id") |>
           purrr::modify_if(\(.) length(.) > 1, sample, size = 1) |>
           purrr::modify_if(\(.) length(.) == 0, \(.) NA) ) |>
  tidyr::unnest(cols = c(cagis_parcel_id)) |>
  select(-cagis_addr_data)

d |>
  group_by(is.na(cagis_parcel_id)) |>
  tally() |>
  mutate(pct = n/sum(n)*100)

d_dpkg <-
  d |>
  dpkg::as_dpkg(
    name = "property_code_enforcements",
    version = "1.1.1",
    title = "Property Code Enforcements",
    homepage = "https://github.com/geomarker-io/parcel",
    description = paste(readLines(fs::path("property_code_enforcements", "README", ext = "md")), collapse = "\n")
  )

# dpkg::dpkg_gh_release(d_dpkg, draft = FALSE)