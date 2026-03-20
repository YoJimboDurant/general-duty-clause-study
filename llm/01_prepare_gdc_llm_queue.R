#
# PREPARE GDC LLM QUEUE
#
# PURPOSE
# Build the OpenAI review queue from gdc_scored for rows flagged as
# probable_gdc or possible_gdc, then split the queue into manageable chunks.
#
# WHEN TO RUN
# Run this after ingest/01_ingest_gdc_scored.R has successfully loaded
# gdc_scored into DuckDB.
#
# INPUTS
# - DuckDB table: gdc_scored
#
# OUTPUTS
# - outputs/llm/gdc_llm_queue_master.csv
# - outputs/llm/gdc_llm_chunk_manifest.csv
# - outputs/llm/chunks/gdc_llm_queue_chunk_*.csv
#
# NOTES
# - This script does NOT call OpenAI
# - It prepares a stable review queue for downstream batch submission
# - Chunking is built in to avoid oversized payloads and unwieldy jobs
#

# Setup -------------------------------------------------------------------

source("R/config.R")
source("R/utils_duckdb.R")

library(DBI)
library(dplyr)
library(readr)
library(stringr)


# Configuration -----------------------------------------------------------

queue_dir_x       <- file.path(dir_outputs_x, "llm")
chunk_dir_x       <- file.path(queue_dir_x, "chunks")
master_queue_x    <- file.path(queue_dir_x, "gdc_llm_queue_master.csv")
chunk_manifest_x  <- file.path(queue_dir_x, "gdc_llm_chunk_manifest.csv")

prompt_version_x  <- "gdc_extraction_v2"
chunk_size_x      <- 500L


# Initialize Directories --------------------------------------------------

dir.create(queue_dir_x, recursive = TRUE, showWarnings = FALSE)
dir.create(chunk_dir_x, recursive = TRUE, showWarnings = FALSE)


# Connect -----------------------------------------------------------------

assert_duckdb_exists_x()
con_x <- connect_duckdb_x(read_only_x = TRUE)

tables_x <- DBI::dbListTables(con_x)

if (!"gdc_scored" %in% tables_x) {
  disconnect_duckdb_x(con_x)
  stop("Required table not found: gdc_scored")
}


# Pull Candidate Rows -----------------------------------------------------

queue_sql_x <- "
SELECT
  row_id,
  citation_key,
  activity_nr,
  citation_id,
  citation_text,
  text_norm,
  gdc_score,
  gdc_bucket,
  is_probable_gdc,
  is_possible_gdc,
  is_gdc_candidate
FROM gdc_scored
WHERE gdc_bucket IN ('probable_gdc', 'possible_gdc')
ORDER BY gdc_score DESC, row_id
"

queue_dfx <- DBI::dbGetQuery(con_x, queue_sql_x)

disconnect_duckdb_x(con_x)


# Validate Queue ----------------------------------------------------------

if (nrow(queue_dfx) == 0) {
  stop("Queue query returned zero rows.")
}

required_cols_x <- c(
  "row_id",
  "citation_key",
  "activity_nr",
  "citation_id",
  "citation_text",
  "gdc_score",
  "gdc_bucket"
)

missing_cols_x <- setdiff(required_cols_x, names(queue_dfx))

if (length(missing_cols_x) > 0) {
  stop(
    paste0(
      "Queue is missing required columns: ",
      paste(missing_cols_x, collapse = ", ")
    )
  )
}


# Prepare Queue -----------------------------------------------------------

queue_dfx <- queue_dfx |>
  mutate(
    llm_queue_id = row_number(),
    prompt_version = prompt_version_x,
    citation_text = str_squish(citation_text),
    citation_char_n = nchar(citation_text),
    chunk_id = ((llm_queue_id - 1L) %/% chunk_size_x) + 1L
  ) |>
  relocate(
    llm_queue_id,
    chunk_id,
    prompt_version,
    .before = row_id
  )


# Write Master Queue ------------------------------------------------------

readr::write_csv(queue_dfx, master_queue_x)


# Build and Write Chunks --------------------------------------------------

chunk_ids_x <- sort(unique(queue_dfx$chunk_id))

chunk_manifest_dfx <- lapply(chunk_ids_x, function(chunk_id_x) {
  
  chunk_dfx <- queue_dfx |>
    filter(chunk_id == chunk_id_x)
  
  chunk_file_x <- file.path(
    chunk_dir_x,
    sprintf("gdc_llm_queue_chunk_%03d.csv", chunk_id_x)
  )
  
  readr::write_csv(chunk_dfx, chunk_file_x)
  
  data.frame(
    chunk_id = chunk_id_x,
    chunk_file = chunk_file_x,
    n_rows = nrow(chunk_dfx),
    min_llm_queue_id = min(chunk_dfx$llm_queue_id),
    max_llm_queue_id = max(chunk_dfx$llm_queue_id),
    stringsAsFactors = FALSE
  )
}) |>
  bind_rows()

readr::write_csv(chunk_manifest_dfx, chunk_manifest_x)


# Console Summary ---------------------------------------------------------

message("Master queue written: ", master_queue_x)
message("Chunk manifest written: ", chunk_manifest_x)
message("Total queue rows: ", format(nrow(queue_dfx), big.mark = ","))
message("Total chunks: ", format(nrow(chunk_manifest_dfx), big.mark = ","))

print(
  queue_dfx |>
    count(gdc_bucket, sort = TRUE)
)

print(chunk_manifest_dfx)