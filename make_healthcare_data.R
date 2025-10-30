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
# multi-matches?
# marge parcel ids
d <- d |>
  mutate(cagis_addr = as_addr(cagis_addr)) |>
  left_join(d_cagis, by = "cagis_addr") 
d2 <- d |>
  unnest(cagis_addr_data)

saveRDS(d, "~/Documents/GitHub/parcel_matching_evaluation/RISEUP_matched_data_10.13.25.rds")

# PEAS
#d <- readRDS("/Users/carsonhartlage/Desktop/PhD/PEAS_pe_encounters_v0.3.rds")


# OMOP data
o <- readRDS("/Users/carsonhartlage/Desktop/PhD/Parcel Matching/omop_testing-n_people_per_addr.rds")
o <- o |>
  filter(zip_code %in% cincy::zcta_tigris_2020$zcta_2020) |>
  mutate(inst_address = dht::address_is_institutional(as.character(addr))) |>
  filter(inst_address==FALSE) |>
  select(-c(inst_address)) 

o$cagis_addr <- o$addr |>
  addr_match(d_cagis$cagis_addr)

o <- o |>
  filter(
    map_lgl(cagis_addr, ~ {
      if (is.null(.x) || length(.x) == 0) return(FALSE)
      val <- as.character(.x)
      val <- val[!is.na(val) & val != ""]
      length(val) > 0
    })
  )

o <- o |>
  unnest(cagis_addr) |>
  mutate(cagis_addr = as_addr(cagis_addr)) |>
  left_join(d_cagis, by = "cagis_addr") |>
  unnest(cagis_addr_data)

o_summary <- o |>
  group_by(cagis_parcel_id) |>
  summarise(n_pts = mean(n_people)) |>
  ungroup()

saveRDS(o, "~/Documents/GitHub/parcel_matching_evaluation/OMOP_matched_data_10.15.25.rds")
saveRDS(o_summary, "~/Documents/GitHub/parcel_matching_evaluation/OMOP_summarized_data_10.15.25.rds")



######

library(yardstick)

metric_set(accuracy, sens, spec) %>%
  map(~ .x(
    truth = eval_data$true_parcel_id,
    estimate = eval_data$matched_parcel_id,
    case_weights = eval_data$freq
  ))

eval_data <- eval_data %>%
  mutate(correct_match = if_else(true_parcel_id == matched_parcel_id, 1, 0))
weighted_agreement <- sum(eval_data$correct_match * eval_data$freq) /
  sum(eval_data$freq)

weighted_agreement

