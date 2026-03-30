library(DBI)
library(duckdb)
library(fs)
library(dplyr)
library(readr)
library(purrr)
library(tibble)
library(readr)


############################################
# SECTION 1 — PATHS
############################################

project_root_x <- getwd()

download_dir_x <- file.path(project_root_x, "data_raw", "dol_downloads")
extract_dir_x  <- file.path(project_root_x, "data_raw", "dol_extract", "violation")

zip_url_x  <- "https://data.dol.gov/data-catalog/OSHA/violation/OSHA_violation.zip"
zip_path_x <- file.path(download_dir_x, "OSHA_violation.zip")

dir.create(download_dir_x, recursive = TRUE, showWarnings = FALSE)
dir.create(extract_dir_x,  recursive = TRUE, showWarnings = FALSE)


############################################
# SECTION 2 — DOWNLOAD BULK ZIP WITH CURL
############################################

# Optional: rename any existing partial/incomplete file
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
# SECTION 4 — Extract
############################################

extract_dir_x <- file.path(project_root_x, "data_raw", "dol_extract", "violation")

dir.create(extract_dir_x, recursive = TRUE, showWarnings = FALSE)

zip_path_x <- file.path(project_root_x, "data_raw", "dol_downloads", "OSHA_violation.zip")

# Optional: skip extraction if already done
existing_csvs_x <- list.files(extract_dir_x, pattern = "\\.csv$", recursive = TRUE)

if (length(existing_csvs_x) > 0) {
  message("CSV files already exist in extract directory. Skipping extraction.")
} else {
  message("Extracting OSHA violation zip...")
  
  utils::unzip(
    zipfile = zip_path_x,
    exdir   = extract_dir_x
  )
  
  message("Extraction complete.")
}

length(list.files(extract_dir_x, pattern = "\\.csv$", recursive = TRUE))

# ============================================
# SECTION 5 — CONNECT TO DUCKDB
# ============================================

# Define DuckDB directory + file
duckdb_dir_x <- file.path(project_root_x, "data_clean", "duckdb")
dir.create(duckdb_dir_x, recursive = TRUE, showWarnings = FALSE)

db_path_x <- file.path(duckdb_dir_x, "osha_gdc.duckdb")

# Connect to DuckDB
con <- dbConnect(
  duckdb::duckdb(),
  dbdir = db_path_x,
  read_only = FALSE
)

# Confirm connection
dbGetQuery(con, "SELECT 'DuckDB connected' AS status")



############################################
# SECTION 6 — LOCATE EXTRACTED CSV CHUNKS
############################################



csv_paths_x <- dir_ls(
  path = extract_dir_x,
  recurse = TRUE,
  regexp = "\\.csv$",
  type = "file"
) %>%
  as.character() %>%
  sort()

if (length(csv_paths_x) == 0) {
  stop("No CSV files found in extract directory: ", extract_dir_x)
}

csv_inventory_dfx <- tibble::tibble(
  path_x = csv_paths_x,
  file_name_x = path_file(csv_paths_x),
  size_bytes = file_info(csv_paths_x)$size,
  size_mb = round(size_bytes / 1024^2, 2)
) %>%
  arrange(file_name_x)

print(csv_inventory_dfx, n = 20)
cat("\nNumber of CSV chunk files found:", length(csv_paths_x), "\n")
cat("Total extracted CSV size (MB):", round(sum(csv_inventory_dfx$size_mb), 2), "\n")

# Optional: save inventory for reproducibility
readr::write_csv(
  csv_inventory_dfx,
  file.path(project_root_x, "data_raw", "source_file_manifest_violation_bulk.csv")
)


# ============================================
# SECTION 7 — INSPECT FIRST CSV
# ============================================

first_csv_x <- normalizePath(csv_paths_x[1], winslash = "/")

cat(read_lines(first_csv_x, n_max = 3), sep = "\n")

schema_dfx <- dbGetQuery(
  con,
  sprintf("
    DESCRIBE
    SELECT *
    FROM read_csv_auto('%s', HEADER = TRUE, SAMPLE_SIZE = 200000)
  ", first_csv_x)
)

print(schema_dfx)

rowcount_dfx <- dbGetQuery(
  con,
  sprintf("
    SELECT COUNT(*) AS n_rows
    FROM read_csv_auto('%s', HEADER = TRUE, SAMPLE_SIZE = 200000)
  ", first_csv_x)
)

print(rowcount_dfx)

############################################
# SECTION 8 — INGEST ALL CSV CHUNKS TO DUCKDB
############################################



# Start fresh
dbExecute(con, "DROP TABLE IF EXISTS violation_raw")
dbExecute(con, "DROP TABLE IF EXISTS violation_csv_rejects_all")

#-------------------------------------------
# 8A — CREATE EMPTY TARGET TABLE
#-------------------------------------------

first_csv_x <- normalizePath(csv_paths_x[1], winslash = "/")

dbExecute(
  con,
  sprintf("
    CREATE TABLE violation_raw AS
    SELECT *
    FROM read_csv(
      '%s',
      HEADER = TRUE,
      ALL_VARCHAR = TRUE
    )
    LIMIT 0
  ", first_csv_x)
)

# Table to accumulate reject metadata
dbExecute(
  con,
  "
  CREATE TABLE violation_csv_rejects_all (
    file_path VARCHAR,
    line_number BIGINT,
    csv_line VARCHAR,
    error_message VARCHAR
  )
  "
)

#-------------------------------------------
# 8B — LOAD FILES ONE BY ONE
#-------------------------------------------

load_results_lx <- vector("list", length(csv_paths_x))

for (i_x in seq_along(csv_paths_x)) {
  
  file_x <- normalizePath(csv_paths_x[i_x], winslash = "/")
  message(sprintf("[%s/%s] Loading %s", i_x, length(csv_paths_x), basename(file_x)))
  
  # clean per-file rejects tables if they exist
  dbExecute(con, "DROP TABLE IF EXISTS violation_csv_rejects")
  dbExecute(con, "DROP TABLE IF EXISTS violation_csv_reject_scans")
  dbExecute(con, "DROP TABLE IF EXISTS violation_stage")
  
  # stage this file
  
  dbExecute(
    con,
    sprintf("
    CREATE TABLE violation_stage AS
    SELECT *
    FROM read_csv(
      '%s',
      HEADER = TRUE,
      DELIM = ',',
      QUOTE = '\"',
      ESCAPE = '\"',
      ALL_VARCHAR = TRUE,
      IGNORE_ERRORS = TRUE,
      STORE_REJECTS = TRUE,
      REJECTS_TABLE = 'violation_csv_rejects',
      REJECTS_SCAN = 'violation_csv_reject_scans',
      REJECTS_LIMIT = 0
    )
  ", file_x)
  )
  
  # append staged rows
  dbExecute(con, "
    INSERT INTO violation_raw
    SELECT * FROM violation_stage
  ")
  
  # append rejects, if any
  reject_exists_x <- dbGetQuery(
    con,
    "
    SELECT COUNT(*) AS n
    FROM information_schema.tables
    WHERE table_name = 'violation_csv_rejects'
    "
  )$n[1]
  
  if (reject_exists_x > 0) {
    dbExecute(
      con,
      sprintf("
        INSERT INTO violation_csv_rejects_all
        SELECT
          '%s' AS file_path,
          line AS line_number,
          csv_line,
          error_message
        FROM violation_csv_rejects
      ", file_x)
    )
    
    reject_n_x <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM violation_csv_rejects")$n[1]
  } else {
    reject_n_x <- 0
  }
  
  loaded_n_x <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM violation_stage")$n[1]
  
  load_results_lx[[i_x]] <- tibble(
    file_path_x = file_x,
    loaded_rows_n = loaded_n_x,
    rejected_rows_n = reject_n_x
  )
}

load_results_dfx <- bind_rows(load_results_lx)

print(load_results_dfx)


############################################
# SECTION 6C — VERIFY LOAD
############################################

violation_rowcount_dfx <- dbGetQuery(
  con,
  "SELECT COUNT(*) AS n_rows FROM violation_raw"
)

reject_count_dfx <- dbGetQuery(
  con,
  "SELECT COUNT(*) AS n_rejects FROM violation_csv_rejects_all"
)

violation_schema_dfx <- dbGetQuery(
  con,
  "DESCRIBE violation_raw"
)

print(violation_rowcount_dfx)
print(reject_count_dfx)
print(violation_schema_dfx)

reject_examples_dfx <- dbGetQuery(
  con,
  "
  SELECT *
  FROM violation_csv_rejects_all
  ORDER BY file_path, line_number
  "
)

print(reject_examples_dfx, n = 50)
# ============================================
# SECTION 6 — PROFILE STANDARD
# ============================================

print(
  dbGetQuery(con, "
    SELECT standard, COUNT(*) AS n
    FROM violation_raw
    GROUP BY standard
    ORDER BY n DESC
    LIMIT 50
  ")
)

print(
  dbGetQuery(con, "
    SELECT *
    FROM violation_raw
    WHERE standard = '5A0001'
    LIMIT 20
  ")
)


rejects_dfx <- dbGetQuery(
  con,
  "
  SELECT *
  FROM violation_csv_rejects_all
  ORDER BY file_path, line_number
  "
)

dbDisconnect(con, shutdown = TRUE)
