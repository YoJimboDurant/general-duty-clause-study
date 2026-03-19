#
# INGEST GDC_SCORED
#
# PURPOSE
# Load the prepared gdc_scored flat file into DuckDB as table gdc_scored.
#
# WHEN TO RUN
# Run this after prep/01_build_gdc_scored_source.R has successfully created:
#   data_clean/gdc_scored.csv
#
# INPUTS
# - data_clean/gdc_scored.csv
#
# OUTPUTS
# - DuckDB table: gdc_scored
#
# NOTES
# - This script replaces only the gdc_scored table
# - It does NOT rebuild or clear the full DuckDB database
# - This is the ingest-layer handoff from prep -> DuckDB
#

# Setup -------------------------------------------------------------------

source("R/config.R")
source("R/utils_duckdb.R")

library(DBI)
library(duckdb)
library(readr)
library(dplyr)


# Configuration -----------------------------------------------------------

gdc_scored_csv_path_x <- file.path(dir_data_clean_x, "gdc_scored.csv")
target_table_x        <- "gdc_scored"


# Validate Input File -----------------------------------------------------

if (!file.exists(gdc_scored_csv_path_x)) {
  stop(
    paste0(
      "Required input file not found: ", gdc_scored_csv_path_x, "\n",
      "Run prep/01_build_gdc_scored_source.R first."
    )
  )
}


# Read Source File --------------------------------------------------------

message("Reading source file: ", gdc_scored_csv_path_x)

gdc_scored_dfx <- readr::read_csv(
  gdc_scored_csv_path_x,
  show_col_types = FALSE
)

if (nrow(gdc_scored_dfx) == 0) {
  stop("Input file was read successfully but contains zero rows.")
}

required_cols_x <- c(
  "row_id",
  "citation_key",
  "activity_nr",
  "citation_id",
  "citation_text",
  "gdc_score",
  "gdc_bucket",
  "is_probable_gdc",
  "is_possible_gdc",
  "is_gdc_candidate"
)

missing_cols_x <- setdiff(required_cols_x, names(gdc_scored_dfx))

if (length(missing_cols_x) > 0) {
  stop(
    paste0(
      "Input file is missing required columns: ",
      paste(missing_cols_x, collapse = ", ")
    )
  )
}


# Connect to DuckDB -------------------------------------------------------

ensure_project_dirs_x()
con_x <- connect_duckdb_x()


# Replace Target Table ----------------------------------------------------

message("Replacing DuckDB table: ", target_table_x)

DBI::dbExecute(
  con_x,
  paste0("DROP TABLE IF EXISTS ", target_table_x)
)

DBI::dbWriteTable(
  con_x,
  name = target_table_x,
  value = gdc_scored_dfx,
  overwrite = FALSE
)


# Post-Load Checks --------------------------------------------------------

table_n_dfx <- DBI::dbGetQuery(
  con_x,
  paste0("SELECT COUNT(*) AS n FROM ", target_table_x)
)

table_n_x <- table_n_dfx$n[[1]]

message("Rows loaded to gdc_scored: ", format(table_n_x, big.mark = ","))

bucket_check_dfx <- DBI::dbGetQuery(
  con_x,
  "
  SELECT
    gdc_bucket,
    COUNT(*) AS n
  FROM gdc_scored
  GROUP BY gdc_bucket
  ORDER BY n DESC
  "
)

print(bucket_check_dfx)

distinct_key_check_dfx <- DBI::dbGetQuery(
  con_x,
  "
  SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT citation_key) AS distinct_citation_keys
  FROM gdc_scored
  "
)

print(distinct_key_check_dfx)


# Optional Indexes --------------------------------------------------------

DBI::dbExecute(
  con_x,
  "
  CREATE INDEX IF NOT EXISTS idx_gdc_scored_activity_nr
  ON gdc_scored(activity_nr)
  "
)

DBI::dbExecute(
  con_x,
  "
  CREATE INDEX IF NOT EXISTS idx_gdc_scored_citation_key
  ON gdc_scored(citation_key)
  "
)


# Cleanup -----------------------------------------------------------------

disconnect_duckdb_x(con_x)

message("Done.")