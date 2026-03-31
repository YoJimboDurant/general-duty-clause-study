source("R/config.R")
source("R/utils_duckdb.R")

library(DBI)
library(duckdb)
library(dplyr)
library(readr)
library(stringr)
library(tibble)


############################################
# SECTION 1 — PATHS
############################################

project_root_x <- "C:/Users/JamesDurant/general-duty-clause-study"


gdc_file_x <- file.path(
  project_root_x,
  "data_clean",
  "gdc_scored.csv"
)

gdc_table_x <- "gdc_scored"
gdc_probable_table_x <- "gdc_probable"
gdc_analysis_table_x <- "gdc_probable_inspection_analysis"


############################################
# SECTION 2 — READ GDC SCORED CSV
############################################

if (!file.exists(gdc_file_x)) {
  stop("File not found: ", gdc_file_x)
}

gdc_dfx <- readr::read_csv(
  gdc_file_x,
  show_col_types = FALSE,
  progress = TRUE,
  guess_max = 100000
)

message("Rows read: ", nrow(gdc_dfx))
message("Columns read: ", ncol(gdc_dfx))
print(names(gdc_dfx))


############################################
# SECTION 3 — STANDARDIZE COLUMN NAMES
############################################

gdc_dfx <- gdc_dfx %>%
  rename(
    ROW_ID = row_id,
    CITATION_KEY = citation_key,
    ACTIVITY_NR = activity_nr,
    CITATION_ID = citation_id,
    CITATION_TEXT = citation_text,
    N_LINES = n_lines,
    LOAD_DT = load_dt,
    TEXT_NORM = text_norm,
    GDC_SCORE = gdc_score,
    GDC_BUCKET = gdc_bucket,
    IS_PROBABLE_GDC = is_probable_gdc,
    IS_POSSIBLE_GDC = is_possible_gdc,
    IS_GDC_CANDIDATE = is_gdc_candidate
  )

print(names(gdc_dfx))


############################################
# SECTION 4 — STANDARDIZE TYPES / VALUES
############################################

gdc_dfx <- gdc_dfx %>%
  mutate(
    ACTIVITY_NR = as.character(ACTIVITY_NR) %>% str_trim(),
    CITATION_ID = as.character(CITATION_ID) %>% str_trim(),
    CITATION_KEY = as.character(CITATION_KEY) %>% str_trim(),
    CITATION_TEXT = as.character(CITATION_TEXT),
    TEXT_NORM = as.character(TEXT_NORM),
    LOAD_DT = as.character(LOAD_DT) %>% str_trim(),
    GDC_BUCKET = as.character(GDC_BUCKET) %>% str_trim()
  ) %>%
  filter(!is.na(ACTIVITY_NR), ACTIVITY_NR != "")

message("Rows after ACTIVITY_NR cleanup: ", nrow(gdc_dfx))


############################################
# SECTION 5 — REQUIRED COLUMN CHECK
############################################

required_cols_x <- c(
  "ACTIVITY_NR",
  "CITATION_ID",
  "CITATION_TEXT",
  "GDC_SCORE",
  "IS_PROBABLE_GDC"
)

missing_cols_x <- setdiff(required_cols_x, names(gdc_dfx))

if (length(missing_cols_x) > 0) {
  stop(
    "Missing required columns after standardization: ",
    paste(missing_cols_x, collapse = ", ")
  )
}


############################################
# SECTION 6 — CONNECT TO DUCKDB
############################################

ensure_project_dirs_x()
con_x <- connect_duckdb_x()


on.exit({
  try(DBI::dbDisconnect(con_x, shutdown = TRUE), silent = TRUE)
}, add = TRUE)


############################################
# SECTION 7 — DROP AND RELOAD TABLES
############################################

DBI::dbExecute(con_x, paste0("DROP TABLE IF EXISTS ", gdc_analysis_table_x))
DBI::dbExecute(con_x, paste0("DROP TABLE IF EXISTS ", gdc_probable_table_x))
DBI::dbExecute(con_x, paste0("DROP TABLE IF EXISTS ", gdc_table_x))

DBI::dbWriteTable(
  conn = con_x,
  name = gdc_table_x,
  value = gdc_dfx,
  overwrite = TRUE
)

message("Reloaded table: ", gdc_table_x)


############################################
# SECTION 8 — VALIDATE FULL GDC SCORED TABLE
############################################

gdc_counts_dfx <- DBI::dbGetQuery(
  con_x,
  "
  SELECT
    COUNT(*) AS n_rows,
    COUNT(DISTINCT ACTIVITY_NR) AS n_activity_distinct,
    COUNT(DISTINCT ACTIVITY_NR || '-' || CITATION_ID) AS n_activity_citation_distinct
  FROM gdc_scored
  "
)

print(gdc_counts_dfx)

gdc_missing_activity_dfx <- DBI::dbGetQuery(
  con_x,
  "
  SELECT
    COUNT(*) AS n_missing_activity
  FROM gdc_scored
  WHERE ACTIVITY_NR IS NULL OR TRIM(ACTIVITY_NR) = ''
  "
)

print(gdc_missing_activity_dfx)

gdc_schema_dfx <- DBI::dbGetQuery(
  con_x,
  "DESCRIBE SELECT * FROM gdc_scored"
)

print(gdc_schema_dfx)

gdc_preview_dfx <- DBI::dbGetQuery(
  con_x,
  "SELECT * FROM gdc_scored LIMIT 10"
)

print(gdc_preview_dfx)


############################################
# SECTION 9 — CHECK GDC BUCKETS / FLAGS
############################################

gdc_bucket_dfx <- DBI::dbGetQuery(
  con_x,
  "
  SELECT
    GDC_BUCKET,
    COUNT(*) AS n
  FROM gdc_scored
  GROUP BY GDC_BUCKET
  ORDER BY n DESC
  "
)

print(gdc_bucket_dfx)

gdc_flag_counts_dfx <- DBI::dbGetQuery(
  con_x,
  "
  SELECT
    IS_PROBABLE_GDC,
    COUNT(*) AS n
  FROM gdc_scored
  GROUP BY IS_PROBABLE_GDC
  ORDER BY n DESC
  "
)

print(gdc_flag_counts_dfx)


############################################
# SECTION 10 — BUILD PROBABLE GDC TABLE
############################################

# This form is robust whether the flag came in as logical or text.
DBI::dbExecute(
  con_x,
  "
  CREATE TABLE gdc_probable AS
  SELECT *
  FROM gdc_scored
  WHERE CAST(IS_PROBABLE_GDC AS VARCHAR) ILIKE 'true'
  "
)

message("Created table: ", gdc_probable_table_x)


############################################
# SECTION 11 — VALIDATE PROBABLE GDC TABLE
############################################

gdc_probable_counts_dfx <- DBI::dbGetQuery(
  con_x,
  "
  SELECT
    COUNT(*) AS n_rows,
    COUNT(DISTINCT ACTIVITY_NR) AS n_activity_distinct,
    COUNT(DISTINCT ACTIVITY_NR || '-' || CITATION_ID) AS n_activity_citation_distinct
  FROM gdc_probable
  "
)

print(gdc_probable_counts_dfx)

gdc_probable_preview_dfx <- DBI::dbGetQuery(
  con_x,
  "SELECT * FROM gdc_probable LIMIT 10"
)

print(gdc_probable_preview_dfx)


############################################
# SECTION 12 — JOIN COVERAGE CHECK
############################################

gdc_join_check_dfx <- DBI::dbGetQuery(
  con_x,
  "
  SELECT
    COUNT(DISTINCT g.ACTIVITY_NR) AS n_gdc_activity,
    COUNT(DISTINCT CASE
      WHEN i.ACTIVITY_NR IS NOT NULL THEN g.ACTIVITY_NR
      ELSE NULL
    END) AS n_matched_activity,
    COUNT(DISTINCT CASE
      WHEN i.ACTIVITY_NR IS NULL THEN g.ACTIVITY_NR
      ELSE NULL
    END) AS n_unmatched_activity
  FROM gdc_probable g
  LEFT JOIN inspection_raw i
    ON g.ACTIVITY_NR = i.ACTIVITY_NR
  "
)

print(gdc_join_check_dfx)


############################################
# SECTION 13 — BUILD PROBABLE GDC + INSPECTION TABLE
############################################

DBI::dbExecute(
  con_x,
  "
  CREATE TABLE gdc_probable_inspection_analysis AS
  SELECT
    g.*,
    i.REPORTING_ID,
    i.STATE_FLAG,
    i.ESTAB_NAME,
    i.SITE_ADDRESS,
    i.SITE_CITY,
    i.SITE_STATE,
    i.SITE_ZIP,
    i.OWNER_TYPE,
    i.OWNER_CODE,
    i.SAFETY_HLTH,
    i.SIC_CODE,
    i.NAICS_CODE,
    i.INSP_TYPE,
    i.INSP_SCOPE,
    i.UNION_STATUS,
    i.NR_IN_ESTAB,
    i.OPEN_DATE,
    i.CASE_MOD_DATE,
    i.CLOSE_CONF_DATE,
    i.CLOSE_CASE_DATE,
    i.LOAD_DT AS INSPECTION_LOAD_DT
  FROM gdc_probable g
  INNER JOIN inspection_raw i
    ON g.ACTIVITY_NR = i.ACTIVITY_NR
  "
)

message("Created table: ", gdc_analysis_table_x)


############################################
# SECTION 14 — VALIDATE JOINED ANALYSIS TABLE
############################################

gdc_analysis_counts_dfx <- DBI::dbGetQuery(
  con_x,
  "
  SELECT
    COUNT(*) AS n_rows,
    COUNT(DISTINCT ACTIVITY_NR) AS n_activity_distinct,
    COUNT(DISTINCT ACTIVITY_NR || '-' || CITATION_ID) AS n_activity_citation_distinct
  FROM gdc_probable_inspection_analysis
  "
)

print(gdc_analysis_counts_dfx)

gdc_state_flag_dfx <- DBI::dbGetQuery(
  con_x,
  "
  SELECT
    STATE_FLAG,
    COUNT(*) AS n
  FROM gdc_probable_inspection_analysis
  GROUP BY STATE_FLAG
  ORDER BY n DESC
  "
)

print(gdc_state_flag_dfx)


############################################
# SECTION 15 — YEAR COUNTS
############################################

gdc_year_counts_dfx <- DBI::dbGetQuery(
  con_x,
  "
  SELECT
    SUBSTR(OPEN_DATE, 1, 4) AS year,
    COUNT(*) AS n_probable_gdc_rows
  FROM gdc_probable_inspection_analysis
  GROUP BY 1
  ORDER BY 1
  "
)

print(gdc_year_counts_dfx)


############################################
# SECTION 16 — OPTIONAL FEDERAL OSHA COUNTS
############################################

gdc_federal_year_counts_dfx <- DBI::dbGetQuery(
  con_x,
  "
  SELECT
    SUBSTR(OPEN_DATE, 1, 4) AS year,
    COUNT(*) AS n_probable_gdc_rows_federal
  FROM gdc_probable_inspection_analysis
  WHERE STATE_FLAG = 'F'
  GROUP BY 1
  ORDER BY 1
  "
)

print(gdc_federal_year_counts_dfx)