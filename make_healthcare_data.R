library(readr)

d <-
  readr::read_csv(
    "~/Downloads/DR1767_r2 with admissions.csv",
    na = c("NA", "-", "NULL", "null"),
    col_types = readr::cols_only(
      MRN = readr::col_character(),
      PAT_ENC_CSN_ID = readr::col_character(),
      ADMIT_DATE_TIME = col_datetime(format = "%m/%d/%Y %I:%M:%S %p"),
      ADDRESS = readr::col_character(),
      CITY = readr::col_character(),
      STATE = readr::col_character(),
      ZIP = readr::col_character(),
      STAY_TYPE = readr::col_character(),
      ADMITTED = readr::col_character()
    )
  ) |>
  filter(ADMITTED == 1 | STAY_TYPE == "ED") |>
  tidyr::unite("raw_address", c(ADDRESS, CITY, STATE, ZIP), sep = " ", na.rm = TRUE) |>
  mutate(clean_address = clean_address_text(raw_address)) |>
  filter(!clean_address == "") |>
  mutate(ADMIT_DATE = as.Date(ADMIT_DATE_TIME)) |>
  filter(!duplicated(PAT_ENC_CSN_ID)) |>
  filter(ADMIT_DATE >= as.Date("2016-07-01") & ADMIT_DATE <= as.Date("2022-06-30"))

d$addr <- addr(d$clean_address)

d <- d |>
  group_by(addr) |> 
  summarise(n_visit = n()) |>
  ungroup()

d <- d |>
  mutate(inst_address = dht::address_is_institutional(as.character(addr))) |>
  filter(inst_address==FALSE) |>
  select(-c(inst_address)) 

d_cagis <-
  codec::cincy_addr_geo() |>
  mutate(cagis_addr = as_addr(cagis_address)) |>
  select(cagis_parcel_id, cagis_addr, cagis_address_type) |>
  sf::st_drop_geometry() |>
  unique() |>
  nest_by(cagis_addr, .key = "cagis_addr_data")
d$cagis_addr <- d$addr |>
  addr_match(d_cagis$cagis_addr)

#
d <- d |>
  filter(
    map_lgl(cagis_addr, ~ {
      if (is.null(.x) || length(.x) == 0) return(FALSE)
      val <- as.character(.x)
      val <- val[!is.na(val) & val != ""]
      length(val) > 0
    })
  )
# what to do about multi-matches?

saveRDS(d, )

# PEAS
#d <- readRDS("/Users/carsonhartlage/Desktop/PhD/PEAS_pe_encounters_v0.3.rds")
