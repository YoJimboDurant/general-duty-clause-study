#
# SQL UTILITIES
#
# PURPOSE
# Helper functions for reading and executing external SQL files used in the
# OSHA General Duty Clause study project.
#
# These utilities support the build layer by separating SQL logic from R
# orchestration code.
#
# WHEN TO RUN
# Source this file after:
#   source("R/config.R")
#   source("R/utils_duckdb.R")
#
# Example:
#   source("R/config.R")
#   source("R/utils_duckdb.R")
#   source("R/utils_sql.R")
#
#   con_x <- connect_duckdb_x()
#   execute_sql_file_x(con_x, file.path(dir_sql_x, "build_gdc_coded.sql"))
#
#   disconnect_duckdb_x(con_x)
#
# IMPORTANT NOTES
# - This file assumes dir_sql_x is defined in R/config.R
# - Use these helpers to keep build scripts short and consistent
# - SQL files should contain the transformation logic; R should orchestrate
#
# RELATED FILES
# - R/config.R
# - R/utils_duckdb.R
# - sql/*.sql
# - build/*.R
#

# Read SQL File -----------------------------------------------------------

read_sql_file_x <- function(sql_path_x) {
  
  if (missing(sql_path_x) || is.null(sql_path_x) || !nzchar(sql_path_x)) {
    stop("A valid sql_path_x must be supplied.")
  }
  
  if (!file.exists(sql_path_x)) {
    stop(paste0("SQL file not found: ", sql_path_x))
  }
  
  sql_x <- paste(readLines(sql_path_x, warn = FALSE), collapse = "\n")
  
  if (!nzchar(trimws(sql_x))) {
    stop(paste0("SQL file is empty: ", sql_path_x))
  }
  
  return(sql_x)
}


# Execute SQL File --------------------------------------------------------

execute_sql_file_x <- function(con_x, sql_path_x) {
  
  if (missing(con_x) || is.null(con_x)) {
    stop("A valid DuckDB connection must be supplied.")
  }
  
  sql_x <- read_sql_file_x(sql_path_x)
  
  DBI::dbExecute(con_x, sql_x)
  
  invisible(TRUE)
}


# Query SQL File ----------------------------------------------------------

query_sql_file_x <- function(con_x, sql_path_x) {
  
  if (missing(con_x) || is.null(con_x)) {
    stop("A valid DuckDB connection must be supplied.")
  }
  
  sql_x <- read_sql_file_x(sql_path_x)
  
  out_dfx <- DBI::dbGetQuery(con_x, sql_x)
  
  return(out_dfx)
}