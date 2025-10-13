library(addr)
library(dplyr)
library(purrr)
library(sf)
library(tidyr)

# need to deal with addr multimatches 
# unnest cagis_addr_data

d <- qs::qread("~/Documents/GitHub/parcel_matching_methods_evaluation/data/geomatched_NAD_to_parcels.qs")
# d <- d |>
#   mutate(
#     addr_matched_parcel_id = map_chr(
#       cagis_addr_data,
#       ~ if (is.null(.x) || nrow(.x) == 0) {
#         NA_character_
#       } else {
#         sample(.x$cagis_parcel_id, 1)
#       }
#     )
#   )

d <- d %>%
  mutate(
    n_addr_matches = map_int(cagis_addr_data, ~ if (is.null(.x) || nrow(.x) == 0) 0 else nrow(.x)),
    cagis_addr_data = map(cagis_addr_data, ~ if (is.null(.x) || nrow(.x) == 0) tibble(cagis_parcel_id = NA_character_) else .x["cagis_parcel_id"])
  ) %>%
  unnest(cagis_addr_data, keep_empty = TRUE) |>
  rename(addr_matched_parcel_id = cagis_parcel_id)

###### 
# mm <- d |>
#   filter(n_addr_matches > 1)
# d_nested <- d_long %>%
#   group_by(nad_address) %>%
#   summarise(
#     cagis_parcel_list = list(cagis_parcel_id),
#     n_parcels = first(n_parcels),
#     .groups = "drop"
#   )
######

frank <- filter(d, county == "Franklin, OH")
ham <- filter(d, county == "Hamilton, OH")
ham <- ham |>
  mutate(
    addr_matched_parcel_id = paste0(sub("^0", "", addr_matched_parcel_id), "00")
  )

hamilton_parcels <-
  dpkg::stow("gh://geomarker-io/parcel/cagis_parcels-v1.1.1") |>
  arrow::read_parquet() |>
  select(parcel_id, land_use, market_total_value)

# ham_hcv <- read_csv("https://raw.githubusercontent.com/geomarker-io/curated_violations/main/curated_violations/curated_violations.csv") |>
#   distinct() |> 
#   filter(date >= as.Date("2016-01-01") & date <= as.Date("2024-12-31")) |>
#   group_by(parcel_number) |>
#   summarize(n_violation = n())

ham <- ham |>
  left_join(hamilton_parcels, join_by(addr_matched_parcel_id == parcel_id)) |>
  rename(addr_matched_parcel_land_use = land_use,
         addr_matched_parcel_value = market_total_value) |>
  left_join(hamilton_parcels, join_by(nad_closest_parcel_id == parcel_id)) |>
  rename(nad_closest_parcel_value = market_total_value) |>
  left_join(hamilton_parcels, join_by(degauss_closest_parcel_id == parcel_id)) |>
  rename(degauss_closest_parcel_value = market_total_value) |>
  # left_join(ham_hcv, join_by(nad_closest_parcel_id == parcel_number)) |>
  # rename(nad_parcel_violations = n_violations) |>
  # left_join(ham_hcv, join_by(degauss_closest_parcel_id == parcel_number)) |>
  # rename(degauss_parcel_violations = n_violations) |>
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

frank <- frank |>
  mutate(county = rep("Franklin", nrow(frank)))
ham <- ham |>
  mutate(county = rep("Hamilton", nrow(ham)))
all <- bind_rows(frank, ham)

######
single_matched <- filter(all, n_addr_matches <= 1)

getMode <- function(x) {
  ux <- unique(x)
  tab <- tabulate(match(x, ux))
  max_tab <- max(tab)
  modes <- ux[tab == max_tab]
  sample(modes, 1) 
}
mm_summary <- all |>
  filter(n_addr_matches > 1) |>
  group_by(nad_address) %>%
  summarise(
    addr_parcel_list = list(addr_matched_parcel_id),
    n_addr_matches = first(n_addr_matches),
    addr_matched_parcel_value = mean(addr_matched_parcel_value, na.rm = TRUE),
    addr_matched_parcel_land_use = getMode(addr_matched_parcel_land_use),
    .groups = "drop"
    )
# almost 1/3 NA?
######

#saveRDS(all, "~/Documents/GitHub/parcel_matching_evaluation/NAD_matching_data_10.13.25.rds")
