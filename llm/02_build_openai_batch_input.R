#
# BUILD OPENAI BATCH INPUT
#
# PURPOSE
# Convert chunked GDC LLM queue files into OpenAI batch-ready JSONL files.
#
# WHEN TO RUN
# Run this after:
# - llm/01_prepare_gdc_llm_queue.R
# - saving the prompt file to llm/prompts/gdc_extraction_v2.txt
#
# INPUTS
# - outputs/llm/gdc_llm_chunk_manifest.csv
# - outputs/llm/chunks/gdc_llm_queue_chunk_*.csv
# - llm/prompts/gdc_extraction_v2.txt
#
# OUTPUTS
# - outputs/llm/batch_input/gdc_batch_input_chunk_*.jsonl
# - outputs/llm/batch_input/gdc_batch_input_manifest.csv
#
# NOTES
# - One JSON object is written per citation
# - Each object includes a stable custom_id for later merge-back
# - This script only builds batch input files; it does NOT submit them
# - This version is designed for chunk-wise processing to avoid oversized jobs
#

# Setup -------------------------------------------------------------------

source("R/config.R")

library(readr)
library(dplyr)
library(stringr)
library(purrr)
library(jsonlite)


# Configuration -----------------------------------------------------------

prompt_version_x <- "gdc_extraction_v2"
prompt_path_x    <- file.path(dir_llm_prompts_x, paste0(prompt_version_x, ".txt"))

queue_dir_x           <- file.path(dir_outputs_x, "llm")
chunk_dir_x           <- file.path(queue_dir_x, "chunks")
chunk_manifest_path_x <- file.path(queue_dir_x, "gdc_llm_chunk_manifest.csv")

batch_input_dir_x     <- file.path(queue_dir_x, "batch_input")
batch_manifest_path_x <- file.path(batch_input_dir_x, "gdc_batch_input_manifest.csv")

model_x <- "gpt-5.4-mini"


# Initialize --------------------------------------------------------------

dir.create(batch_input_dir_x, recursive = TRUE, showWarnings = FALSE)

if (!file.exists(prompt_path_x)) {
  stop("Prompt file not found: ", prompt_path_x)
}

if (!file.exists(chunk_manifest_path_x)) {
  stop("Chunk manifest not found: ", chunk_manifest_path_x)
}

prompt_text_x <- paste(readLines(prompt_path_x, warn = FALSE), collapse = "\n")

if (!nzchar(trimws(prompt_text_x))) {
  stop("Prompt file is empty: ", prompt_path_x)
}

chunk_manifest_dfx <- readr::read_csv(
  chunk_manifest_path_x,
  show_col_types = FALSE
)

if (nrow(chunk_manifest_dfx) == 0) {
  stop("Chunk manifest is empty.")
}


# Helpers -----------------------------------------------------------------

build_custom_id_x <- function(row_dfx) {
  paste0(
    "citation_key=", row_dfx$citation_key,
    "|row_id=", row_dfx$row_id,
    "|chunk_id=", row_dfx$chunk_id,
    "|llm_queue_id=", row_dfx$llm_queue_id
  )
}


build_request_body_x <- function(citation_text_x, prompt_text_x, model_x) {
  list(
    model = model_x,
    input = list(
      list(
        role = "system",
        content = list(
          list(
            type = "input_text",
            text = prompt_text_x
          )
        )
      ),
      list(
        role = "user",
        content = list(
          list(
            type = "input_text",
            text = citation_text_x
          )
        )
      )
    )
  )
}


build_batch_record_x <- function(row_dfx, prompt_text_x, model_x) {
  list(
    custom_id = build_custom_id_x(row_dfx),
    method = "POST",
    url = "/v1/responses",
    body = build_request_body_x(
      citation_text_x = row_dfx$citation_text,
      prompt_text_x   = prompt_text_x,
      model_x         = model_x
    )
  )
}


write_jsonl_x <- function(records_lx, out_path_x) {
  con_x <- file(out_path_x, open = "wt", encoding = "UTF-8")
  on.exit(close(con_x), add = TRUE)
  
  purrr::walk(
    records_lx,
    function(rec_x) {
      json_line_x <- jsonlite::toJSON(
        rec_x,
        auto_unbox = TRUE,
        null = "null",
        force = TRUE
      )
      writeLines(json_line_x, con_x, sep = "\n")
    }
  )
  
  invisible(out_path_x)
}


# Validate Chunk Schema ---------------------------------------------------

required_cols_x <- c(
  "llm_queue_id",
  "chunk_id",
  "prompt_version",
  "row_id",
  "citation_key",
  "activity_nr",
  "citation_id",
  "citation_text",
  "gdc_score",
  "gdc_bucket"
)


# Build Batch Inputs ------------------------------------------------------

batch_manifest_lx <- vector("list", length = nrow(chunk_manifest_dfx))

for (i_x in seq_len(nrow(chunk_manifest_dfx))) {
  
  chunk_id_x   <- chunk_manifest_dfx$chunk_id[[i_x]]
  chunk_file_x <- chunk_manifest_dfx$chunk_file[[i_x]]
  
  if (!file.exists(chunk_file_x)) {
    stop("Chunk file listed in manifest does not exist: ", chunk_file_x)
  }
  
  message("Processing chunk ", chunk_id_x, ": ", chunk_file_x)
  
  chunk_dfx <- readr::read_csv(
    chunk_file_x,
    show_col_types = FALSE
  )
  
  if (nrow(chunk_dfx) == 0) {
    stop("Chunk file is empty: ", chunk_file_x)
  }
  
  missing_cols_x <- setdiff(required_cols_x, names(chunk_dfx))
  
  if (length(missing_cols_x) > 0) {
    stop(
      paste0(
        "Chunk file is missing required columns: ",
        paste(missing_cols_x, collapse = ", "),
        " | file: ",
        chunk_file_x
      )
    )
  }
  
  if (length(unique(chunk_dfx$chunk_id)) != 1) {
    stop("Chunk file contains multiple chunk_id values: ", chunk_file_x)
  }
  
  if (unique(chunk_dfx$chunk_id) != chunk_id_x) {
    stop(
      paste0(
        "Chunk manifest chunk_id (", chunk_id_x,
        ") does not match chunk file chunk_id (",
        unique(chunk_dfx$chunk_id), ")."
      )
    )
  }
  
  chunk_dfx <- chunk_dfx |>
    mutate(
      citation_text = str_squish(citation_text)
    )
  
  if (any(!nzchar(chunk_dfx$citation_text))) {
    stop("One or more rows have empty citation_text in chunk: ", chunk_file_x)
  }
  
  row_lx <- split(chunk_dfx, seq_len(nrow(chunk_dfx)))
  
  batch_records_lx <- purrr::map(
    row_lx,
    function(row_dfx) {
      build_batch_record_x(
        row_dfx       = row_dfx,
        prompt_text_x = prompt_text_x,
        model_x       = model_x
      )
    }
  )
  batch_file_x <- file.path(
    batch_input_dir_x,
    sprintf("gdc_batch_input_chunk_%03d.jsonl", chunk_id_x)
  )
  
  write_jsonl_x(
    records_lx = batch_records_lx,
    out_path_x = batch_file_x
  )
  
  batch_manifest_lx[[i_x]] <- tibble::tibble(
    chunk_id = chunk_id_x,
    prompt_version = prompt_version_x,
    model = model_x,
    source_chunk_file = chunk_file_x,
    batch_input_file = batch_file_x,
    n_rows = nrow(chunk_dfx),
    min_llm_queue_id = min(chunk_dfx$llm_queue_id),
    max_llm_queue_id = max(chunk_dfx$llm_queue_id),
    min_row_id = min(chunk_dfx$row_id),
    max_row_id = max(chunk_dfx$row_id),
    file_size_bytes = file.info(batch_file_x)$size %||% NA_real_
  )
}

batch_manifest_dfx <- bind_rows(batch_manifest_lx)

readr::write_csv(batch_manifest_dfx, batch_manifest_path_x)


# Console Summary ---------------------------------------------------------

message("Prompt file: ", prompt_path_x)
message("Batch input directory: ", batch_input_dir_x)
message("Batch manifest written: ", batch_manifest_path_x)
message("Total batch files: ", nrow(batch_manifest_dfx))
message("Total queued citations: ", format(sum(batch_manifest_dfx$n_rows), big.mark = ","))

print(batch_manifest_dfx)