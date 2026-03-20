#
# SUBMIT OPENAI BATCH
#
# PURPOSE
# Upload one selected JSONL batch-input file to OpenAI and create a Batch job.
#
# WHEN TO RUN
# Run this after:
# - llm/02_build_openai_batch_input.R
# - confirming the selected JSONL file looks correct
#
# INPUTS
# - outputs/llm/batch_input/gdc_batch_input_manifest.csv
# - one JSONL batch input file
# - OPENAI_API_KEY in environment
#
# OUTPUTS
# - outputs/llm/batch_submissions/gdc_batch_submission_manifest.csv
#
# NOTES
# - This script submits ONE selected batch file at a time
# - It uploads the file with purpose = "batch"
# - It creates a batch against endpoint = "/v1/responses"
# - completion_window is set to "24h"
#

# Setup -------------------------------------------------------------------

source("R/config.R")

library(readr)
library(dplyr)
library(httr)
library(httr2)
library(jsonlite)

`%||%` <- function(a, b) if (!is.null(a)) a else b


# Configuration -----------------------------------------------------------

batch_input_dir_x      <- file.path(dir_outputs_x, "llm", "batch_input")
batch_manifest_path_x  <- file.path(batch_input_dir_x, "gdc_batch_input_manifest.csv")

submission_dir_x       <- file.path(dir_outputs_x, "llm", "batch_submissions")
submission_manifest_x  <- file.path(submission_dir_x, "gdc_batch_submission_manifest.csv")

openai_api_key_x       <- Sys.getenv("OPENAI_API_KEY")
openai_base_url_x      <- "https://api.openai.com/v1"

endpoint_x             <- "/v1/responses"
completion_window_x    <- "24h"

# Set this to the batch file you want to submit.
# For a canary test, point this at a tiny JSONL file.
selected_batch_file_x  <- file.path(
  batch_input_dir_x,
  "gdc_batch_input_chunk_001.jsonl"
)


# Initialize --------------------------------------------------------------

dir.create(submission_dir_x, recursive = TRUE, showWarnings = FALSE)

if (!nzchar(openai_api_key_x)) {
  stop("OPENAI_API_KEY is not set in the environment.")
}

if (!file.exists(batch_manifest_path_x)) {
  stop("Batch input manifest not found: ", batch_manifest_path_x)
}

if (!file.exists(selected_batch_file_x)) {
  stop("Selected batch file not found: ", selected_batch_file_x)
}


# Read Input Manifest -----------------------------------------------------

batch_manifest_dfx <- readr::read_csv(
  batch_manifest_path_x,
  show_col_types = FALSE
)

selected_row_dfx <- batch_manifest_dfx |>
  filter(normalizePath(batch_input_file, winslash = "/", mustWork = FALSE) ==
           normalizePath(selected_batch_file_x, winslash = "/", mustWork = FALSE))

if (nrow(selected_row_dfx) != 1) {
  stop(
    paste0(
      "Expected exactly one matching row in batch manifest for file: ",
      selected_batch_file_x
    )
  )
}


# Upload File -------------------------------------------------------------

message("Uploading batch input file: ", selected_batch_file_x)

upload_resp_x <- request(paste0(openai_base_url_x, "/files")) |>
  req_headers(
    Authorization = paste("Bearer", openai_api_key_x)
  ) |>
  req_body_multipart(
    purpose = "batch",
    file = upload_file(selected_batch_file_x)
  ) |>
  req_perform()

upload_out_lx <- resp_body_json(upload_resp_x)

input_file_id_x <- upload_out_lx$id %||% NA_character_

if (!nzchar(input_file_id_x) || is.na(input_file_id_x)) {
  stop("File upload succeeded but no input file id was returned.")
}

message("Uploaded file id: ", input_file_id_x)


# Create Batch ------------------------------------------------------------

message("Creating batch job...")

batch_body_lx <- list(
  input_file_id = input_file_id_x,
  endpoint = endpoint_x,
  completion_window = completion_window_x
)

batch_resp_x <- request(paste0(openai_base_url_x, "/batches")) |>
  req_headers(
    Authorization = paste("Bearer", openai_api_key_x),
    `Content-Type` = "application/json"
  ) |>
  req_body_json(batch_body_lx, auto_unbox = TRUE) |>
  req_perform()

batch_out_lx <- resp_body_json(batch_resp_x)

batch_id_x            <- batch_out_lx$id %||% NA_character_
batch_status_x        <- batch_out_lx$status %||% NA_character_
output_file_id_x      <- batch_out_lx$output_file_id %||% NA_character_
error_file_id_x       <- batch_out_lx$error_file_id %||% NA_character_
submitted_at_x        <- as.character(Sys.time())

if (!nzchar(batch_id_x) || is.na(batch_id_x)) {
  stop("Batch creation succeeded but no batch id was returned.")
}

message("Created batch id: ", batch_id_x)
message("Batch status: ", batch_status_x)


# Write Submission Manifest -----------------------------------------------

submission_row_dfx <- tibble::tibble(
  submitted_at = submitted_at_x,
  prompt_version = selected_row_dfx$prompt_version[[1]],
  model = selected_row_dfx$model[[1]],
  source_batch_input_file = selected_batch_file_x,
  n_rows = selected_row_dfx$n_rows[[1]],
  min_llm_queue_id = selected_row_dfx$min_llm_queue_id[[1]],
  max_llm_queue_id = selected_row_dfx$max_llm_queue_id[[1]],
  input_file_id = input_file_id_x,
  batch_id = batch_id_x,
  batch_status = batch_status_x,
  endpoint = endpoint_x,
  completion_window = completion_window_x,
  output_file_id = output_file_id_x,
  error_file_id = error_file_id_x
)

if (file.exists(submission_manifest_x)) {
  existing_submission_dfx <- readr::read_csv(
    submission_manifest_x,
    show_col_types = FALSE
  )
  
  submission_manifest_dfx <- bind_rows(
    existing_submission_dfx,
    submission_row_dfx
  )
} else {
  submission_manifest_dfx <- submission_row_dfx
}

readr::write_csv(submission_manifest_dfx, submission_manifest_x)


# Console Summary ---------------------------------------------------------

message("Submission manifest written: ", submission_manifest_x)
print(submission_row_dfx)