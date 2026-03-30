library(DBI)
library(duckdb)
library(fs)
library(dplyr)
library(readr)
library(purrr)
library(tibble)
library(stringr)


############################################
# SECTION 1 — PATHS
############################################

project_root_x <- "C:/Users/JamesDurant/general-duty-clause-study"

download_dir_x <- file.path(project_root_x, "data_raw", "dol_downloads")
extract_dir_x  <- file.path(project_root_x, "data_raw", "dol_extract", "inspection")
db_dir_x       <- file.path(project_root_x, "data_clean", "duckdb")
db_path_x      <- file.path(db_dir_x, "osha_gdc.duckdb")

# Bulk ZIP pattern is consistent with the OSHA DOL catalog bulk files you already used.
# If DOL ever changes the filename, this is the one line you should update.
zip_url_x  <- "https://data.dol.gov/data-catalog/OSHA/inspection/OSHA_inspection.zip"
zip_path_x <- file.path(download_dir_x, "OSHA_inspection.zip")

raw_table_x      <- "inspection_raw"
rejects_table_x  <- "inspection_raw_rejects"
file_log_table_x <- "inspection_raw_file_log"

dir.create(download_dir_x, recursive = TRUE, showWarnings = FALSE)
dir.create(extract_dir_x,  recursive = TRUE, showWarnings = FALSE)
dir.create(db_dir_x,       recursive = TRUE, showWarnings = FALSE)


############################################
# SECTION 2 — DOWNLOAD BULK ZIP WITH CURL
############################################

if (file.exists(zip_path_x)) {
  message("Existing file found at: ", zip_path_x)
  message("Renaming existing file before fresh download...")
  
  incomplete_zip_path_x <- file.path(
    download_dir_x,
    paste0(
      tools::file_path_sans_ext(basename(zip_path_x)),
      "_INCOMPLETE_",
      format(Sys.time(), "%Y%m%dT%H%M%S"),
      ".zip"
    )
  )
  
  file.rename(zip_path_x, incomplete_zip_path_x)
}

curl_status_x <- system2(
  command = "curl",
  args = c(
    "-L",
    "-C", "-",
    zip_url_x,
    "-o", normalizePath(zip_path_x, winslash = "/", mustWork = FALSE)
  )
)

if (!file.exists(zip_path_x)) {
  stop("Curl download did not create the zip file.")
}

zip_info_dfx <- file.info(zip_path_x)
print(zip_info_dfx[, c("size", "mtime")])

if (!isTRUE(curl_status_x == 0)) {
  stop("Curl download returned non-zero exit status: ", curl_status_x)
}

message("Curl download finished successfully.")


############################################
# SECTION 3 — VERIFY AND LIST ZIP CONTENTS
############################################

zip_info_dfx <- file.info(zip_path_x)
print(zip_info_dfx[, c("size", "mtime")])

zip_contents_dfx <- utils::unzip(zip_path_x, list = TRUE)
print(zip_contents_dfx)


############################################
# SECTION 4 — EXTRACT
############################################

existing_csvs_x <- list.files(
  extract_dir_x,
  pattern = "\\.csv$",
  recursive = TRUE,
  full.names = TRUE
)

if (length(existing_csvs_x) > 0) {
  message("CSV files already exist in extract directory.")
  message("Skipping extraction.")
} else {
  message("Extracting ZIP contents to: ", extract_dir_x)
  
  utils::unzip(
    zipfile = zip_path_x,
    exdir   = extract_dir_x
  )
}

csv_files_x <- list.files(
  extract_dir_x,
  pattern = "\\.csv$",
  recursive = TRUE,
  full.names = TRUE
)

csv_files_x <- sort(csv_files_x)

if (length(csv_files_x) == 0) {
  stop("No CSV files found after extraction.")
}

message("Found ", length(csv_files_x), " CSV file(s).")
print(tibble(file_index_x = seq_along(csv_files_x), file_path_x = csv_files_x))


############################################
# SECTION 5 — INSPECT HEADER ONLY
############################################

header_line_x <- readr::read_lines(
  file = csv_files_x[[1]],
  n_max = 1,
  progress = FALSE
)

if (length(header_line_x) != 1) {
  stop("Could not read header line from the first CSV.")
}

header_fields_x <- strsplit(header_line_x, ",", fixed = TRUE)[[1]] |>
  stringr::str_trim(side = "both") |>
  gsub('^"|"$', "", x = _)

header_preview_dfx <- tibble(
  column_position_x = seq_along(header_fields_x),
  column_name_x = header_fields_x
)

message("Header preview:")
print(header_preview_dfx, n = Inf)

message("Zero-row schema preview with readr:")
schema_preview_dfx <- suppressMessages(
  readr::read_csv(
    file = csv_files_x[[1]],
    n_max = 0,
    col_types = readr::cols(.default = readr::col_character()),
    progress = FALSE,
    show_col_types = FALSE,
    quote = "\"",
    escape_double = TRUE
  )
) |>
  names() |>
  tibble(column_name_x = _)

print(schema_preview_dfx, n = Inf)


############################################
# SECTION 6 — CONNECT TO DUCKDB
############################################

con_x <- DBI::dbConnect(
  duckdb::duckdb(),
  dbdir = db_path_x,
  read_only = FALSE
)

on.exit({
  try(DBI::dbDisconnect(con_x, shutdown = TRUE), silent = TRUE)
}, add = TRUE)

message("Connected to DuckDB at: ", db_path_x)


############################################
# SECTION 7 — DROP / RESET TARGET TABLES
############################################

DBI::dbExecute(con_x, paste0("DROP TABLE IF EXISTS ", raw_table_x))
DBI::dbExecute(con_x, paste0("DROP TABLE IF EXISTS ", rejects_table_x))
DBI::dbExecute(con_x, paste0("DROP TABLE IF EXISTS ", file_log_table_x))
DBI::dbExecute(con_x, "DROP TABLE IF EXISTS reject_errors")
DBI::dbExecute(con_x, "DROP TABLE IF EXISTS reject_scans")

DBI::dbExecute(
  con_x,
  paste0(
    "CREATE TABLE ", file_log_table_x, " (
      file_index BIGINT,
      file_name VARCHAR,
      file_path VARCHAR,
      rows_loaded BIGINT,
      rows_rejected BIGINT,
      load_ts TIMESTAMP
    )"
  )
)


############################################
# SECTION 8 — HELPER TO BUILD DuckDB read_csv SQL
############################################

build_read_csv_sql_x <- function(path_x) {
  path_sql_x <- gsub("\\\\", "/", path_x)
  
  paste0(
    "read_csv(",
    "'", path_sql_x, "', ",
    "delim = ',', ",
    "quote = '\"', ",
    "escape = '\"', ",
    "header = TRUE, ",
    "all_varchar = TRUE, ",
    "ignore_errors = TRUE, ",
    "store_rejects = TRUE, ",
    "sample_size = -1",
    ")"
  )
}


############################################
# SECTION 9 — INGEST EACH CSV FILE
############################################

for (i_x in seq_along(csv_files_x)) {
  
  file_x <- csv_files_x[[i_x]]
  file_name_x <- basename(file_x)
  stage_table_x <- paste0("stg_inspection_", i_x)
  
  message("--------------------------------------------------")
  message("Loading file ", i_x, " of ", length(csv_files_x))
  message("File name: ", file_name_x)
  
  DBI::dbExecute(con_x, paste0("DROP TABLE IF EXISTS ", stage_table_x))
  DBI::dbExecute(con_x, "DROP TABLE IF EXISTS reject_errors")
  DBI::dbExecute(con_x, "DROP TABLE IF EXISTS reject_scans")
  
  read_sql_x <- build_read_csv_sql_x(file_x)
  
  create_stage_sql_x <- paste0(
    "CREATE TABLE ", stage_table_x, " AS ",
    "SELECT * FROM ", read_sql_x
  )
  
  DBI::dbExecute(con_x, create_stage_sql_x)
  
  if (!DBI::dbExistsTable(con_x, raw_table_x)) {
    DBI::dbExecute(
      con_x,
      paste0("CREATE TABLE ", raw_table_x, " AS SELECT * FROM ", stage_table_x)
    )
  } else {
    DBI::dbExecute(
      con_x,
      paste0("INSERT INTO ", raw_table_x, " SELECT * FROM ", stage_table_x)
    )
  }
  
  rows_loaded_x <- DBI::dbGetQuery(
    con_x,
    paste0("SELECT COUNT(*) AS n FROM ", stage_table_x)
  )$n[[1]]
  
  reject_exists_x <- DBI::dbGetQuery(
    con_x,
    "
    SELECT COUNT(*) AS n
    FROM information_schema.tables
    WHERE lower(table_name) = 'reject_errors'
    "
  )$n[[1]] > 0
  
  rows_rejected_x <- 0L
  
  if (isTRUE(reject_exists_x)) {
    rows_rejected_x <- DBI::dbGetQuery(
      con_x,
      "SELECT COUNT(*) AS n FROM reject_errors"
    )$n[[1]]
    
    if (rows_rejected_x > 0) {
      if (!DBI::dbExistsTable(con_x, rejects_table_x)) {
        DBI::dbExecute(
          con_x,
          paste0(
            "CREATE TABLE ", rejects_table_x, " AS
             SELECT
               ", i_x, " AS file_index,
               '", gsub("'", "''", file_name_x), "' AS file_name,
               '", gsub("'", "''", gsub("\\\\", "/", file_x)), "' AS file_path,
               CURRENT_TIMESTAMP AS captured_ts,
               *
             FROM reject_errors"
          )
        )
      } else {
        DBI::dbExecute(
          con_x,
          paste0(
            "INSERT INTO ", rejects_table_x, "
             SELECT
               ", i_x, " AS file_index,
               '", gsub("'", "''", file_name_x), "' AS file_name,
               '", gsub("'", "''", gsub("\\\\", "/", file_x)), "' AS file_path,
               CURRENT_TIMESTAMP AS captured_ts,
               *
             FROM reject_errors"
          )
        )
      }
    }
  }
  
  DBI::dbExecute(
    con_x,
    paste0(
      "INSERT INTO ", file_log_table_x, "
       VALUES (",
      i_x, ", ",
      "'", gsub("'", "''", file_name_x), "', ",
      "'", gsub("'", "''", gsub("\\\\", "/", file_x)), "', ",
      rows_loaded_x, ", ",
      rows_rejected_x, ", ",
      "CURRENT_TIMESTAMP",
      ")"
    )
  )
  
  DBI::dbExecute(con_x, paste0("DROP TABLE IF EXISTS ", stage_table_x))
  
  message("Rows loaded from file:   ", format(rows_loaded_x, big.mark = ","))
  message("Rows rejected from file: ", format(rows_rejected_x, big.mark = ","))
}


############################################
# SECTION 10 — POST-LOAD VALIDATION
############################################

message("--------------------------------------------------")
message("POST-LOAD VALIDATION")

total_rows_dfx <- DBI::dbGetQuery(
  con_x,
  paste0("SELECT COUNT(*) AS n_rows FROM ", raw_table_x)
)

if (DBI::dbExistsTable(con_x, rejects_table_x)) {
  total_rejects_dfx <- DBI::dbGetQuery(
    con_x,
    paste0("SELECT COUNT(*) AS n_rejects FROM ", rejects_table_x)
  )
} else {
  total_rejects_dfx <- tibble(n_rejects = 0L)
}

schema_dfx <- DBI::dbGetQuery(
  con_x,
  paste0("DESCRIBE SELECT * FROM ", raw_table_x)
)

file_log_dfx <- DBI::dbGetQuery(
  con_x,
  paste0("SELECT * FROM ", file_log_table_x, " ORDER BY file_index")
)

print(total_rows_dfx)
print(total_rejects_dfx)
print(schema_dfx)
print(file_log_dfx)

message("Inspection ingest complete.")


############################################
# SECTION 11 — OPTIONAL QA PREVIEW
############################################

preview_dfx <- DBI::dbGetQuery(
  con_x,
  paste0("SELECT * FROM ", raw_table_x, " LIMIT 10")
)

print(preview_dfx)


############################################
# SECTION 12 — RECOMMENDED VALIDATION SQL
############################################

cat(
  "
-- 1) Total rows
SELECT COUNT(*) AS n_rows
FROM inspection_raw;

-- 2) Reject count
SELECT COUNT(*) AS n_rejects
FROM inspection_raw_rejects;

-- 3) File-level audit
SELECT *
FROM inspection_raw_file_log
ORDER BY file_index;

-- 4) Schema
DESCRIBE SELECT * FROM inspection_raw;

-- 5) Quick key audit for activity number
-- Replace activity_nr if the exact column name differs
SELECT
  COUNT(*) AS n_rows,
  COUNT(activity_nr) AS n_activity_nr_nonmissing,
  COUNT(DISTINCT activity_nr) AS n_activity_nr_distinct
FROM inspection_raw;

-- 6) Join readiness check against violation_raw
SELECT COUNT(*) AS n_matched
FROM inspection_raw i
INNER JOIN violation_raw v
  ON i.activity_nr = v.activity_nr;

-- 7) Counts per year after you identify the inspection date field
-- Replace open_date with the real field name once confirmed
-- SELECT
--   SUBSTR(open_date, 1, 4) AS year,
--   COUNT(*) AS n
-- FROM inspection_raw
-- GROUP BY 1
-- ORDER BY 1;
"
)


dbDisconnect(con_x, shutdown = TRUE)
