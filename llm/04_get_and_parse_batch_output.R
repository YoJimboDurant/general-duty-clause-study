#
# GET AND PARSE OPENAI BATCH OUTPUT
#
# PURPOSE
# Retrieve batch job status from OpenAI, download output/error files when
# available, and parse batch output JSONL into a flat CSV for downstream use.
#
# WHEN TO RUN
# Run this after llm/03_submit_openai_batch.R has submitted a batch.
#
# INPUTS
# - outputs/llm/batch_submissions/gdc_batch_submission_manifest.csv
# - OPENAI_API_KEY in environment
#
# OUTPUTS
# - outputs/llm/batch_results/raw/<batch_id>_output.jsonl
# - outputs/llm/batch_results/raw/<batch_id>_error.jsonl
# - outputs/llm/batch_results/parsed/<batch_id>_parsed.csv
# - outputs/llm/batch_results/parsed/<batch_id>_parsed.rds
# - updated submission manifest with latest status info
#
# NOTES
# - This script targets one batch_id at a time
# - It is safe to rerun
# - Parsing preserves custom_id for merge-back
#

# Setup -------------------------------------------------------------------

source("R/config.R")

library(readr)
library(dplyr)
library(httr2)
library(jsonlite)
library(stringr)
library(tibble)
library(purrr)

`%||%` <- function(a, b) if (!is.null(a)) a else b


# Configuration -----------------------------------------------------------

openai_api_key_x      <- Sys.getenv("OPENAI_API_KEY")
openai_base_url_x     <- "https://api.openai.com/v1"

submission_dir_x      <- file.path(dir_outputs_x, "llm", "batch_submissions")
submission_manifest_x <- file.path(submission_dir_x, "gdc_batch_submission_manifest.csv")

results_dir_x         <- file.path(dir_outputs_x, "llm", "batch_results")
results_raw_dir_x     <- file.path(results_dir_x, "raw")
results_parsed_dir_x  <- file.path(results_dir_x, "parsed")

# Set this to the batch you want to retrieve/parse.
# Easiest workflow: paste the batch_id from the submission manifest.
target_batch_id_x     <- NA_character_


# Initialize --------------------------------------------------------------

dir.create(results_dir_x, recursive = TRUE, showWarnings = FALSE)
dir.create(results_raw_dir_x, recursive = TRUE, showWarnings = FALSE)
dir.create(results_parsed_dir_x, recursive = TRUE, showWarnings = FALSE)

if (!nzchar(openai_api_key_x)) {
  stop("OPENAI_API_KEY is not set in the environment.")
}

if (!file.exists(submission_manifest_x)) {
  stop("Submission manifest not found: ", submission_manifest_x)
}


# Read Submission Manifest ------------------------------------------------

submission_manifest_dfx <- readr::read_csv(
  submission_manifest_x,
  show_col_types = FALSE
)

if (nrow(submission_manifest_dfx) == 0) {
  stop("Submission manifest is empty.")
}

if (is.na(target_batch_id_x) || !nzchar(target_batch_id_x)) {
  target_batch_id_x <- submission_manifest_dfx$batch_id[[nrow(submission_manifest_dfx)]]
  message("No target_batch_id_x provided; using most recent batch_id: ", target_batch_id_x)
}

target_row_idx_x <- which(submission_manifest_dfx$batch_id == target_batch_id_x)

if (length(target_row_idx_x) != 1) {
  stop("Expected exactly one matching batch_id in submission manifest: ", target_batch_id_x)
}


# Helpers -----------------------------------------------------------------

extract_custom_id_field_x <- function(custom_id_x, field_name_x) {
  pattern_x <- paste0("(^|\\|)", field_name_x, "=([^|]+)")
  match_x <- stringr::str_match(custom_id_x %||% "", pattern_x)
  out_x <- match_x[, 3]
  out_x[is.na(out_x)] <- NA_character_
  out_x
}


safe_json_parse_x <- function(text_x) {
  tryCatch(
    jsonlite::fromJSON(text_x, simplifyVector = FALSE),
    error = function(e_x) NULL
  )
}


extract_output_text_x <- function(response_body_lx) {
  if (is.null(response_body_lx)) {
    return(NA_character_)
  }
  
  output_lx <- response_body_lx$output %||% NULL
  
  if (is.null(output_lx) || length(output_lx) == 0) {
    return(NA_character_)
  }
  
  text_candidates_x <- c()
  
  for (item_x in output_lx) {
    content_lx <- item_x$content %||% NULL
    
    if (is.null(content_lx) || length(content_lx) == 0) {
      next
    }
    
    for (content_item_x in content_lx) {
      item_text_x <- content_item_x$text %||% NULL
      
      if (!is.null(item_text_x) && nzchar(item_text_x)) {
        text_candidates_x <- c(text_candidates_x, item_text_x)
      }
    }
  }
  
  if (length(text_candidates_x) == 0) {
    return(NA_character_)
  }
  
  paste(text_candidates_x, collapse = "\n")
}


parse_model_json_x <- function(raw_text_x) {
  
  if (is.na(raw_text_x) || !nzchar(raw_text_x)) {
    return(NULL)
  }
  
  raw_text_x <- trimws(raw_text_x)
  
  ############################################
  # FIX 1 — missing closing brace repair
  ############################################
  
  n_open_x  <- stringr::str_count(raw_text_x, "\\{")
  n_close_x <- stringr::str_count(raw_text_x, "\\}")
  
  if (n_open_x == (n_close_x + 1)) {
    raw_text_x <- paste0(raw_text_x, "}")
  }
  
  ############################################
  # FIX 2 — attempt full parse
  ############################################
  
  parsed_lx <- safe_json_parse_x(raw_text_x)
  
  if (!is.null(parsed_lx)) {
    return(parsed_lx)
  }
  
  ############################################
  # FIX 3 — fallback extraction
  ############################################
  
  json_match_x <- stringr::str_extract(raw_text_x, "\\{.*\\}")
  
  if (!is.na(json_match_x) && nzchar(json_match_x)) {
    
    # apply same brace repair again just in case
    n_open_x  <- stringr::str_count(json_match_x, "\\{")
    n_close_x <- stringr::str_count(json_match_x, "\\}")
    
    if (n_open_x == (n_close_x + 1)) {
      json_match_x <- paste0(json_match_x, "}")
    }
    
    return(safe_json_parse_x(json_match_x))
  }
  
  ############################################
  # FAIL
  ############################################
  
  NULL
}

flatten_result_record_x <- function(line_lx) {
  custom_id_x   <- line_lx$custom_id %||% NA_character_
  response_lx   <- line_lx$response %||% NULL
  error_lx      <- line_lx$error %||% NULL
  
  response_id_x     <- response_lx$body$id %||% NA_character_
  response_status_x <- response_lx$status_code %||% NA_real_
  
  model_raw_text_x <- extract_output_text_x(response_lx$body %||% NULL)
  parsed_json_lx   <- parse_model_json_x(model_raw_text_x)
  
  tibble(
    custom_id = custom_id_x,
    citation_key = extract_custom_id_field_x(custom_id_x, "citation_key"),
    row_id = suppressWarnings(as.integer(extract_custom_id_field_x(custom_id_x, "row_id"))),
    chunk_id = suppressWarnings(as.integer(extract_custom_id_field_x(custom_id_x, "chunk_id"))),
    llm_queue_id = suppressWarnings(as.integer(extract_custom_id_field_x(custom_id_x, "llm_queue_id"))),
    response_id = response_id_x,
    response_status_code = response_status_x,
    parse_status = case_when(
      !is.null(error_lx) ~ "request_error",
      is.na(model_raw_text_x) ~ "no_output_text",
      is.null(parsed_json_lx) ~ "json_parse_error",
      TRUE ~ "ok"
    ),
    classification = parsed_json_lx$classification %||% NA_character_,
    is_gdc = parsed_json_lx$is_gdc %||% NA,
    state_reference = parsed_json_lx$state_reference %||% NA,
    state_name = parsed_json_lx$state_name %||% NA_character_,
    standard_guess = parsed_json_lx$standard_guess %||% NA_character_,
    hazard_text = parsed_json_lx$hazard_text %||% NA_character_,
    hazard_short = parsed_json_lx$hazard_short %||% NA_character_,
    hazard_category = parsed_json_lx$hazard_category %||% NA_character_,
    failure_mode = parsed_json_lx$failure_mode %||% NA_character_,
    hazard_source_text = parsed_json_lx$hazard_source_text %||% NA_character_,
    hazard_source_list = if (!is.null(parsed_json_lx$hazard_source_list)) {
      paste(unlist(parsed_json_lx$hazard_source_list), collapse = "; ")
    } else {
      NA_character_
    },
    reason_short = parsed_json_lx$reason_short %||% NA_character_,
    raw_output_text = model_raw_text_x,
    request_error_code = error_lx$code %||% NA_character_,
    request_error_message = error_lx$message %||% NA_character_
  )
}


# Retrieve Batch Status ---------------------------------------------------

message("Retrieving batch status for: ", target_batch_id_x)

batch_resp_x <- request(paste0(openai_base_url_x, "/batches/", target_batch_id_x)) |>
  req_headers(
    Authorization = paste("Bearer", openai_api_key_x)
  ) |>
  req_perform()

batch_out_lx <- resp_body_json(batch_resp_x)

batch_status_x      <- batch_out_lx$status %||% NA_character_
output_file_id_x    <- batch_out_lx$output_file_id %||% NA_character_
error_file_id_x     <- batch_out_lx$error_file_id %||% NA_character_
completed_at_x      <- batch_out_lx$completed_at %||% NA
failed_at_x         <- batch_out_lx$failed_at %||% NA
expired_at_x        <- batch_out_lx$expired_at %||% NA
canceled_at_x       <- batch_out_lx$canceled_at %||% NA
retrieved_at_x      <- as.character(Sys.time())

message("Batch status: ", batch_status_x)


# Update Submission Manifest ----------------------------------------------

submission_manifest_dfx$batch_status[target_row_idx_x]   <- batch_status_x
submission_manifest_dfx$output_file_id[target_row_idx_x] <- output_file_id_x
submission_manifest_dfx$error_file_id[target_row_idx_x]  <- error_file_id_x

if (!"retrieved_at" %in% names(submission_manifest_dfx)) {
  submission_manifest_dfx$retrieved_at <- NA_character_
}
submission_manifest_dfx$retrieved_at[target_row_idx_x] <- retrieved_at_x

readr::write_csv(submission_manifest_dfx, submission_manifest_x)


# Download Output/Error Files ---------------------------------------------

output_jsonl_path_x <- file.path(results_raw_dir_x, paste0(target_batch_id_x, "_output.jsonl"))
error_jsonl_path_x  <- file.path(results_raw_dir_x, paste0(target_batch_id_x, "_error.jsonl"))

if (!is.na(output_file_id_x) && nzchar(output_file_id_x)) {
  message("Downloading output file: ", output_file_id_x)
  
  output_resp_x <- request(paste0(openai_base_url_x, "/files/", output_file_id_x, "/content")) |>
    req_headers(
      Authorization = paste("Bearer", openai_api_key_x)
    ) |>
    req_perform()
  
  writeBin(resp_body_raw(output_resp_x), output_jsonl_path_x)
} else {
  message("No output_file_id available yet.")
}

if (!is.na(error_file_id_x) && nzchar(error_file_id_x)) {
  message("Downloading error file: ", error_file_id_x)
  
  error_resp_x <- request(paste0(openai_base_url_x, "/files/", error_file_id_x, "/content")) |>
    req_headers(
      Authorization = paste("Bearer", openai_api_key_x)
    ) |>
    req_perform()
  
  writeBin(resp_body_raw(error_resp_x), error_jsonl_path_x)
} else {
  message("No error_file_id available.")
}


# Parse Output File -------------------------------------------------------

if (file.exists(output_jsonl_path_x)) {
  
  message("Parsing output file: ", output_jsonl_path_x)
  
  output_lines_x <- readLines(output_jsonl_path_x, warn = FALSE, encoding = "UTF-8")
  output_lines_x <- output_lines_x[nzchar(trimws(output_lines_x))]
  
  if (length(output_lines_x) == 0) {
    stop("Downloaded output file is empty: ", output_jsonl_path_x)
  }
  
  output_records_lx <- purrr::map(output_lines_x, safe_json_parse_x)
  
  parsed_dfx <- purrr::map_dfr(output_records_lx, flatten_result_record_x)
  
  parsed_csv_path_x <- file.path(results_parsed_dir_x, paste0(target_batch_id_x, "_parsed.csv"))
  parsed_rds_path_x <- file.path(results_parsed_dir_x, paste0(target_batch_id_x, "_parsed.rds"))
  
  readr::write_csv(parsed_dfx, parsed_csv_path_x)
  saveRDS(parsed_dfx, parsed_rds_path_x)
  
  message("Parsed CSV written: ", parsed_csv_path_x)
  message("Parsed RDS written: ", parsed_rds_path_x)
  
  print(parsed_dfx |> count(parse_status, sort = TRUE))
  
} else {
  message("No output JSONL file available yet, so parsing was skipped.")
}


# Console Summary ---------------------------------------------------------

message("Updated submission manifest: ", submission_manifest_x)
message("Done.")