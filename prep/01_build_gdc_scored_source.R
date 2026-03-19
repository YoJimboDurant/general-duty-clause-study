###############################################################
# OSHA GENERAL DUTY CLAUSE DATA EXTRACTION FROM DOL CSV/ZIP FILES
#
# This script:
# 1. Downloads bulk CSV or ZIP files from DOL/OSHA
# 2. Captures reproducibility metadata and SHA-256 hashes
# 3. Extracts ZIP archives into a local directory
# 4. Hashes extracted CSVs
# 5. Loads the CSV file(s)
# 6. Continues with the existing GDC text collapse + scoring workflow
###############################################################

library(httr2)
library(dplyr)
library(tibble)
library(stringr)
library(purrr)
library(readr)
library(fs)
library(tools)
library(digest)
library(janitor)

`%||%` <- function(a, b) if (!is.null(a)) a else b

###############################################################
# Configuration
###############################################################

dir_create("data_raw")
dir_create("data_raw/dol_downloads")
dir_create("data_raw/dol_extract")
dir_create("data_clean")

source_files_dfx <- tibble::tribble(
  ~dataset_name,            ~url,
  "violation_gen_duty_std", "https://data.dol.gov/data-catalog/OSHA/violation_gen_duty_std/OSHA_violation_gen_duty_std.zip"
)

provenance_file_x <- "data_raw/source_file_manifest.csv"

###############################################################
# Helpers
###############################################################

compute_sha256_x <- function(path_x) {
  if (!file.exists(path_x)) {
    return(NA_character_)
  }
  digest::digest(file = path_x, algo = "sha256")
}


get_git_sha_x <- function() {
  git_sha_x <- tryCatch(
    system("git rev-parse HEAD", intern = TRUE),
    error = function(e_x) character(0)
  )
  
  if (length(git_sha_x) == 0) {
    return(NA_character_)
  }
  
  git_sha_x[[1]]
}


get_http_metadata_x <- function(url_x) {
  resp_x <- request(url_x) |>
    req_user_agent("Jim-Durant-OSHA-General-Duty-Research/0.3") |>
    req_method("HEAD") |>
    req_perform()
  
  headers_lx <- resp_headers(resp_x)
  
  tibble(
    source_url = url_x,
    etag = headers_lx[["etag"]] %||% NA_character_,
    last_modified = headers_lx[["last-modified"]] %||% NA_character_,
    content_length = suppressWarnings(as.numeric(headers_lx[["content-length"]] %||% NA_character_)),
    content_type = headers_lx[["content-type"]] %||% NA_character_,
    http_status = resp_status(resp_x)
  )
}


safe_download_file_x <- function(url_x, dest_path_x, overwrite_x = FALSE) {
  if (file.exists(dest_path_x) && !overwrite_x) {
    message("File already exists, skipping download: ", dest_path_x)
    return(dest_path_x)
  }
  
  message("Downloading: ", url_x)
  
  request(url_x) |>
    req_user_agent("Jim-Durant-OSHA-General-Duty-Research/0.3") |>
    req_perform(path = dest_path_x)
  
  if (!file.exists(dest_path_x)) {
    stop("Download failed for: ", url_x)
  }
  
  dest_path_x
}


download_bulk_files_x <- function(source_files_dfx,
                                  download_dir_x = "data_raw/dol_downloads",
                                  overwrite_x = FALSE) {
  
  manifest_dfx <- source_files_dfx |>
    mutate(
      file_name = basename(url),
      dest_path = file.path(download_dir_x, file_name)
    ) |>
    mutate(
      http_meta = map(url, get_http_metadata_x),
      downloaded_path = map2_chr(
        url,
        dest_path,
        ~ safe_download_file_x(
          url_x = .x,
          dest_path_x = .y,
          overwrite_x = overwrite_x
        )
      ),
      download_timestamp_utc = format(Sys.time(), tz = "UTC", usetz = TRUE),
      file_size_bytes = file.size(downloaded_path),
      sha256 = map_chr(downloaded_path, compute_sha256_x)
    ) |>
    tidyr::unnest(http_meta) |>
    mutate(
      git_sha = get_git_sha_x()
    ) |>
    select(
      dataset_name,
      source_url = url,
      file_name,
      downloaded_path,
      download_timestamp_utc,
      file_size_bytes,
      sha256,
      etag,
      last_modified,
      content_length,
      content_type,
      http_status,
      git_sha
    )
  
  manifest_dfx
}


extract_archives_x <- function(download_manifest_dfx,
                               extract_dir_x = "data_raw/dol_extract",
                               overwrite_x = FALSE) {
  
  extracted_dfx <- purrr::pmap_dfr(
    list(
      dataset_name = download_manifest_dfx$dataset_name,
      downloaded_path = download_manifest_dfx$downloaded_path,
      archive_sha256 = download_manifest_dfx$sha256
    ),
    function(dataset_name, downloaded_path, archive_sha256) {
      
      ext_x <- tolower(tools::file_ext(downloaded_path))
      dataset_extract_dir_x <- file.path(extract_dir_x, dataset_name)
      fs::dir_create(dataset_extract_dir_x)
      
      if (ext_x == "zip") {
        message("Extracting ZIP: ", downloaded_path)
        
        utils::unzip(
          zipfile = downloaded_path,
          exdir = dataset_extract_dir_x,
          overwrite = overwrite_x
        )
        
        extracted_files_x <- list.files(
          dataset_extract_dir_x,
          full.names = TRUE,
          recursive = TRUE
        )
        
      } else if (ext_x == "csv") {
        target_path_x <- file.path(
          dataset_extract_dir_x,
          basename(downloaded_path)
        )
        
        if (!file.exists(target_path_x) || overwrite_x) {
          file.copy(downloaded_path, target_path_x, overwrite = overwrite_x)
        }
        
        extracted_files_x <- target_path_x
        
      } else {
        warning("Unsupported file type for: ", downloaded_path)
        extracted_files_x <- character(0)
      }
      
      tibble(
        dataset_name = dataset_name,
        archive_path = downloaded_path,
        archive_sha256 = archive_sha256,
        extracted_path = extracted_files_x,
        extracted_file_name = basename(extracted_files_x),
        extracted_file_size_bytes = ifelse(
          file.exists(extracted_files_x),
          file.size(extracted_files_x),
          NA_real_
        ),
        extracted_sha256 = map_chr(extracted_files_x, compute_sha256_x)
      )
    }
  )
  
  extracted_dfx
}


write_provenance_manifest_x <- function(download_manifest_dfx,
                                        extracted_manifest_dfx,
                                        provenance_file_x = "data_raw/source_file_manifest.csv") {
  
  archive_manifest_dfx <- download_manifest_dfx |>
    mutate(record_type = "downloaded_archive") |>
    transmute(
      record_type,
      dataset_name,
      source_url,
      local_path = downloaded_path,
      file_name,
      file_size_bytes,
      sha256,
      download_timestamp_utc,
      etag,
      last_modified,
      content_length,
      content_type,
      http_status,
      parent_archive_sha256 = NA_character_,
      git_sha
    )
  
  extracted_file_manifest_dfx <- extracted_manifest_dfx |>
    mutate(record_type = "extracted_file") |>
    transmute(
      record_type,
      dataset_name,
      source_url = NA_character_,
      local_path = extracted_path,
      file_name = extracted_file_name,
      file_size_bytes = extracted_file_size_bytes,
      sha256 = extracted_sha256,
      download_timestamp_utc = NA_character_,
      etag = NA_character_,
      last_modified = NA_character_,
      content_length = NA_real_,
      content_type = NA_character_,
      http_status = NA_real_,
      parent_archive_sha256 = archive_sha256,
      git_sha = get_git_sha_x()
    )
  
  provenance_dfx <- bind_rows(
    archive_manifest_dfx,
    extracted_file_manifest_dfx
  )
  
  write_csv(provenance_dfx, provenance_file_x)
  provenance_dfx
}


find_csv_files_x <- function(extracted_manifest_dfx,
                             dataset_name_x = NULL,
                             pattern_x = "\\.csv$") {
  
  out_dfx <- extracted_manifest_dfx |>
    filter(str_detect(extracted_path, regex(pattern_x, ignore_case = TRUE)))
  
  if (!is.null(dataset_name_x)) {
    out_dfx <- out_dfx |>
      filter(dataset_name == dataset_name_x)
  }
  
  out_dfx
}


read_bulk_csvs_x <- function(csv_manifest_dfx,
                             guess_max_x = 100000) {
  
  if (nrow(csv_manifest_dfx) == 0) {
    stop("No CSV files found to load.")
  }
  
  purrr::map_dfr(
    csv_manifest_dfx$extracted_path,
    function(path_x) {
      
      message("Reading: ", path_x)
      
      readr::read_csv(
        path_x,
        guess_max = guess_max_x,
        show_col_types = FALSE
      ) %>%
        janitor::clean_names()
    }
  )
}

###############################################################
# Load GDC lines from bulk file(s)
###############################################################

load_gdc_lines_from_bulk_x <- function(extracted_manifest_dfx) {
  
  gdc_csvs_dfx <- find_csv_files_x(
    extracted_manifest_dfx = extracted_manifest_dfx,
    dataset_name_x = "violation_gen_duty_std"
  )
  
  if (nrow(gdc_csvs_dfx) == 0) {
    stop("No CSV files found for dataset_name == 'violation_gen_duty_std'")
  }
  
  gdc_lines_dfx <- read_bulk_csvs_x(gdc_csvs_dfx)
  
  required_cols_x <- c("activity_nr", "citation_id", "line_nr", "line_text")
  missing_cols_x <- setdiff(required_cols_x, names(gdc_lines_dfx))
  
  if (length(missing_cols_x) > 0) {
    stop(
      "Missing expected columns in GDC file(s): ",
      paste(missing_cols_x, collapse = ", ")
    )
  }
  
  gdc_lines_dfx
}

###############################################################
# Collapse line-based data into one narrative per citation
###############################################################

collapse_gdc_lines_x <- function(gdc_lines_dfx) {
  gdc_lines_dfx |>
    mutate(
      line_nr = suppressWarnings(as.integer(line_nr)),
      line_text = stringr::str_squish(as.character(line_text)),
      load_dt = dplyr::coalesce(as.character(load_dt), NA_character_)
    ) |>
    arrange(activity_nr, citation_id, line_nr) |>
    group_by(activity_nr, citation_id) |>
    summarise(
      citation_text = stringr::str_c(line_text, collapse = " "),
      n_lines = n(),
      load_dt = dplyr::first(na.omit(load_dt)) %||% NA_character_,
      .groups = "drop"
    ) %>%
    mutate(
      row_id = dplyr::row_number(),
      citation_key = paste(activity_nr, citation_id, sep = "_")
    ) %>%
    relocate(row_id, citation_key, .before = activity_nr)
}

###############################################################
# Regex patterns
###############################################################

gdc_ref_rx <- "(?i)\\b(?:section\\s*)?5\\s*\\(?a\\)?\\s*[-–—:/;, ]*\\(?1\\)?\\b|\\b5a1\\b|\\b29\\s*(?:u\\.?s\\.?c\\.?|usc)\\s*654\\s*\\(?a\\)?\\s*\\(?1\\)?\\b|\\b654\\s*\\(?a\\)?\\s*\\(?1\\)?\\b"

osha_std_rx <- "(?i)\\b29\\s*cfr\\s*(?:1910|1926|1915|1917|1918|1904|1903)\\.[0-9]"

###############################################################
# Text normalization
###############################################################

normalize_gdc_text_fx <- function(text_x) {
  text_x %>%
    dplyr::coalesce("") %>%
    stringr::str_replace_all("[\r\n\t]+", " ") %>%
    stringr::str_replace_all("[[:punct:]]+", " ") %>%
    stringr::str_squish() %>%
    stringr::str_to_lower()
}

###############################################################
# Scoring function
###############################################################

score_gdc_fx <- function(text_x) {
  
  text_norm_x <- normalize_gdc_text_fx(text_x)
  
  signal_ref_x   <- stringr::str_detect(text_norm_x, stringr::regex(gdc_ref_rx))
  signal_rec_x   <- stringr::str_detect(text_norm_x, "recogni|reconi|recognis")
  signal_haz_x   <- stringr::str_detect(text_norm_x, "hazard|hazrd|hazzard")
  signal_emp_x   <- stringr::str_detect(text_norm_x, "employ")
  signal_place_x <- stringr::str_detect(text_norm_x, "\\bplace\\b")
  signal_free_x  <- stringr::str_detect(text_norm_x, "\\bfree\\b")
  
  signal_std_x   <- stringr::str_detect(text_norm_x, stringr::regex(osha_std_rx))
  
  score_x <- 0
  score_x <- score_x + ifelse(signal_ref_x,   5, 0)
  score_x <- score_x + ifelse(signal_rec_x,   2, 0)
  score_x <- score_x + ifelse(signal_haz_x,   2, 0)
  score_x <- score_x + ifelse(signal_emp_x,   1, 0)
  score_x <- score_x + ifelse(signal_place_x, 1, 0)
  score_x <- score_x + ifelse(signal_free_x,  1, 0)
  score_x <- score_x - ifelse(signal_std_x,   2, 0)
  
  score_x
}

###############################################################
# Main workflow
###############################################################

download_manifest_dfx <- download_bulk_files_x(
  source_files_dfx = source_files_dfx,
  overwrite_x = FALSE
)

extracted_manifest_dfx <- extract_archives_x(
  download_manifest_dfx = download_manifest_dfx,
  overwrite_x = FALSE
)

provenance_dfx <- write_provenance_manifest_x(
  download_manifest_dfx = download_manifest_dfx,
  extracted_manifest_dfx = extracted_manifest_dfx,
  provenance_file_x = provenance_file_x
)

gdc_lines_dfx <- load_gdc_lines_from_bulk_x(extracted_manifest_dfx)

write_csv(gdc_lines_dfx, "data_raw/gdc_lines_raw.csv")

gdc_citations_dfx <- collapse_gdc_lines_x(gdc_lines_dfx)

gdc_scored_dfx <- gdc_citations_dfx %>%
  mutate(
    text_norm = normalize_gdc_text_fx(citation_text),
    gdc_score = purrr::map_dbl(citation_text, score_gdc_fx),
    gdc_bucket = case_when(
      gdc_score >= 6 ~ "probable_gdc",
      gdc_score >= 4 ~ "possible_gdc",
      TRUE ~ "unlikely_gdc"
    ),
    is_probable_gdc = gdc_bucket == "probable_gdc",
    is_possible_gdc = gdc_bucket == "possible_gdc",
    is_gdc_candidate = gdc_bucket %in% c("probable_gdc", "possible_gdc")
  )

gdc_probable_dfx <- gdc_scored_dfx %>%
  filter(is_probable_gdc)

gdc_possible_dfx <- gdc_scored_dfx %>%
  filter(is_possible_gdc)

gdc_unlikely_dfx <- gdc_scored_dfx %>%
  filter(gdc_bucket == "unlikely_gdc")

write_csv(gdc_citations_dfx, "data_clean/gdc_citations_text.csv")
write_csv(gdc_scored_dfx, "data_clean/gdc_scored.csv")
write_csv(provenance_dfx, "data_raw/source_file_manifest.csv")

saveRDS(gdc_citations_dfx, file = "data_clean/gdc_citations_dfx.rds")
saveRDS(gdc_scored_dfx, file = "data_clean/gdc_scored_dfx.rds")

saveRDS(
  list(
    provenance_dfx = provenance_dfx,
    gdc_lines_dfx = gdc_lines_dfx,
    gdc_citations_dfx = gdc_citations_dfx,
    gdc_probable_dfx = gdc_probable_dfx,
    gdc_possible_dfx = gdc_possible_dfx,
    gdc_unlikely_dfx = gdc_unlikely_dfx
  ),
  file = "data_clean/gdc_data_clean.rds"
)

