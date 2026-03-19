#
# DUCKDB UTILITIES
#
# PURPOSE
# Helper functions for connecting to and disconnecting from the DuckDB
# database used in the OSHA General Duty Clause study project.
#
# These utilities standardize database access across ingest, build,
# analysis, and LLM-support scripts.
#
# WHEN TO RUN
# Source this file after:
#   source("R/config.R")
#
# Example:
#   source("R/config.R")
#   source("R/utils_duckdb.R")
#
#   ensure_project_dirs_x()
#   con_x <- connect_duckdb_x()
#
#   # ... do work ...
#
#   disconnect_duckdb_x(con_x)
#
# IMPORTANT NOTES
# - This file assumes db_path_x is defined in R/config.R
# - Use these helpers instead of repeating dbConnect/dbDisconnect logic
# - Keeps pipeline behavior consistent and easier to debug
#
# RELATED FILES
# - R/config.R
# - R/utils_sql.R
# - ingest/*.R
# - build/*.R
#

# Libraries ---------------------------------------------------------------

library(DBI)
library(duckdb)


# Connect to DuckDB -------------------------------------------------------

connect_duckdb_x <- function(read_only_x = FALSE) {
  
  if (!exists("db_path_x", inherits = TRUE)) {
    stop("db_path_x not found. Source R/config.R before calling connect_duckdb_x().")
  }
  
  ensure_project_dirs_x()
  
  con_x <- DBI::dbConnect(
    duckdb::duckdb(),
    dbdir = db_path_x,
    read_only = read_only_x
  )
  
  return(con_x)
}


# Disconnect from DuckDB --------------------------------------------------

disconnect_duckdb_x <- function(con_x, shutdown_x = TRUE) {
  
  if (missing(con_x) || is.null(con_x)) {
    return(invisible(NULL))
  }
  
  try(
    DBI::dbDisconnect(con_x, shutdown = shutdown_x),
    silent = TRUE
  )
  
  invisible(NULL)
}


# Check Database Exists ---------------------------------------------------

duckdb_exists_x <- function() {
  file.exists(db_path_x)
}


# Assert Database Exists --------------------------------------------------

assert_duckdb_exists_x <- function() {
  
  if (!duckdb_exists_x()) {
    stop(
      paste0(
        "DuckDB database not found at: ", db_path_x, "\n",
        "Run the required ingest steps before running this script."
      )
    )
  }
  
  invisible(TRUE)
}