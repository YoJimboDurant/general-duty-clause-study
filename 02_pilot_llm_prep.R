# =========================================================
# pilot_llm_prep.R
# Prepare pilot datasets for LLM-assisted OSHA citation classification
# =========================================================

rm(list = ls())
gc()

options(stringsAsFactors = FALSE)

library(dplyr)
library(readr)

# -----------------------------
# paths
# -----------------------------
data_dir_x <- "."
input_rds_x <- file.path(data_dir_x, "data_clean.rds")
output_dir_x <- file.path(data_dir_x, "pilot_outputs")

if (!dir.exists(output_dir_x)) {
  dir.create(output_dir_x, recursive = TRUE)
}

# -----------------------------
# pilot sample sizes
# -----------------------------
pilot_probable_n_x <- 250
pilot_possible_n_x <- 250
pilot_unlikely_n_x <- 250

# -----------------------------
# load saved objects
# -----------------------------
if (!file.exists(input_rds_x)) {
  stop("Could not find file: ", input_rds_x)
}

data_clean_lx <- readRDS(input_rds_x)

if (!is.list(data_clean_lx)) {
  stop("data_clean.rds did not load as a list.")
}

message("Objects found in RDS:")
print(names(data_clean_lx))

# -----------------------------
# check required objects
# -----------------------------
required_objects_x <- c(
  "gdc_probable_dfx",
  "gdc_possible_dfx",
  "gdc_unlikely_dfx"
)

missing_objects_x <- setdiff(required_objects_x, names(data_clean_lx))

if (length(missing_objects_x) > 0) {
  stop(
    "Missing required objects in data_clean.rds: ",
    paste(missing_objects_x, collapse = ", ")
  )
}

# -----------------------------
# pull objects from list by name
# -----------------------------
gdc_probable_dfx <- data_clean_lx[["gdc_probable_dfx"]]
gdc_possible_dfx <- data_clean_lx[["gdc_possible_dfx"]]
gdc_unlikely_dfx <- data_clean_lx[["gdc_unlikely_dfx"]]

# -----------------------------
# basic validation
# -----------------------------
required_cols_x <- c("citation_text", "text_norm")

probable_missing_cols_x <- setdiff(required_cols_x, names(gdc_probable_dfx))
possible_missing_cols_x <- setdiff(required_cols_x, names(gdc_possible_dfx))
unlikely_missing_cols_x <- setdiff(required_cols_x, names(gdc_unlikely_dfx))

if (length(probable_missing_cols_x) > 0) {
  stop(
    "gdc_probable_dfx is missing required columns: ",
    paste(probable_missing_cols_x, collapse = ", ")
  )
}

if (length(possible_missing_cols_x) > 0) {
  stop(
    "gdc_possible_dfx is missing required columns: ",
    paste(possible_missing_cols_x, collapse = ", ")
  )
}

if (length(unlikely_missing_cols_x) > 0) {
  stop(
    "gdc_unlikely_dfx is missing required columns: ",
    paste(unlikely_missing_cols_x, collapse = ", ")
  )
}

message("Dimensions:")
message("  gdc_probable_dfx: ", paste(dim(gdc_probable_dfx), collapse = " x "))
message("  gdc_possible_dfx: ", paste(dim(gdc_possible_dfx), collapse = " x "))
message("  gdc_unlikely_dfx: ", paste(dim(gdc_unlikely_dfx), collapse = " x "))

# -----------------------------
# set seed
# -----------------------------
set.seed(20524)

# -----------------------------
# build pilot datasets
# -----------------------------
pilot_probable_n_use_x <- min(pilot_probable_n_x, nrow(gdc_probable_dfx))
pilot_possible_n_use_x <- min(pilot_possible_n_x, nrow(gdc_possible_dfx))
pilot_unlikely_n_use_x <- min(pilot_unlikely_n_x, nrow(gdc_unlikely_dfx))

pilot_probable_dfx <- gdc_probable_dfx %>%
  slice_sample(n = pilot_probable_n_use_x) %>%
  mutate(pilot_source = "probable_sample")

pilot_possible_dfx <- gdc_possible_dfx %>%
  slice_sample(n = pilot_possible_n_use_x) %>%
  mutate(pilot_source = "possible_sample")

pilot_unlikely_dfx <- gdc_unlikely_dfx %>%
  slice_sample(n = pilot_unlikely_n_use_x) %>%
  mutate(pilot_source = "unlikely_sample")

pilot_llm_dfx <- bind_rows(
  pilot_probable_dfx,
  pilot_possible_dfx,
  pilot_unlikely_dfx
) %>%
  mutate(
    row_id = row_number()
  ) %>%
  relocate(row_id, pilot_source)

# -----------------------------
# quick summaries
# -----------------------------
message("Pilot dataset sizes:")

pilot_summary_dfx <- tibble(
  dataset = c(
    "pilot_probable_dfx",
    "pilot_possible_dfx",
    "pilot_unlikely_dfx",
    "pilot_llm_dfx"
  ),
  n = c(
    nrow(pilot_probable_dfx),
    nrow(pilot_possible_dfx),
    nrow(pilot_unlikely_dfx),
    nrow(pilot_llm_dfx)
  )
)

print(pilot_summary_dfx)

pilot_source_summary_dfx <- pilot_llm_dfx %>%
  count(pilot_source, name = "n")

print(pilot_source_summary_dfx)

pilot_bucket_summary_dfx <- tibble(
  bucket = c("probable", "possible", "unlikely"),
  available_n = c(
    nrow(gdc_probable_dfx),
    nrow(gdc_possible_dfx),
    nrow(gdc_unlikely_dfx)
  ),
  sampled_n = c(
    pilot_probable_n_use_x,
    pilot_possible_n_use_x,
    pilot_unlikely_n_use_x
  )
)

print(pilot_bucket_summary_dfx)

# -----------------------------
# save outputs
# -----------------------------
pilot_objects_lx <- list(
  pilot_probable_dfx = pilot_probable_dfx,
  pilot_possible_dfx = pilot_possible_dfx,
  pilot_unlikely_dfx = pilot_unlikely_dfx,
  pilot_llm_dfx = pilot_llm_dfx,
  pilot_summary_dfx = pilot_summary_dfx,
  pilot_source_summary_dfx = pilot_source_summary_dfx,
  pilot_bucket_summary_dfx = pilot_bucket_summary_dfx
)

saveRDS(
  pilot_objects_lx,
  file = file.path(output_dir_x, "pilot_llm_objects.rds")
)

write_csv(
  pilot_llm_dfx,
  file = file.path(output_dir_x, "pilot_llm_dfx.csv")
)

write_csv(
  pilot_summary_dfx,
  file = file.path(output_dir_x, "pilot_summary_dfx.csv")
)

write_csv(
  pilot_source_summary_dfx,
  file = file.path(output_dir_x, "pilot_source_summary_dfx.csv")
)

write_csv(
  pilot_bucket_summary_dfx,
  file = file.path(output_dir_x, "pilot_bucket_summary_dfx.csv")
)

message("Pilot outputs written to: ", output_dir_x)

gc()