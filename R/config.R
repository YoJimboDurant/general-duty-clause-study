# 
# CONFIGURATION FILE — PROJECT PATHS & DIRECTORY SETUP
# 
#
# PURPOSE
# Centralized configuration for the OSHA General Duty Clause study project.
# Defines all project paths, directory structure, and database locations.
#
# Eliminates:
# - hardcoded file paths
# - duplicated directory logic
# - environment-specific inconsistencies
#
# WHEN TO RUN
# Source this file at the top of EVERY script:
#   source("R/config.R")
#
# Run ensure_project_dirs_x() when:
# - starting the pipeline
# - before writing files or databases
#
# KEY OUTPUTS
# - project_root_x : project root directory
# - db_path_x      : DuckDB database path
# - dir_*_x        : standardized directories
#
# IMPORTANT NOTES
# - All paths are relative to project root
# - Do NOT hardcode paths elsewhere
# - This is the single source of truth for paths
#
# RELATED FILES
# - R/utils_duckdb.R
# - R/utils_sql.R
# - run_pipeline.R
# 


# Project Root ------------------------------------------------------------

get_project_root_x <- function() {
  normalizePath(".", winslash = "/", mustWork = TRUE)
}


# Core Paths --------------------------------------------------------------

project_root_x <- get_project_root_x()

dir_r_x        <- file.path(project_root_x, "R")
dir_prep_x     <- file.path(project_root_x, "prep")
dir_ingest_x   <- file.path(project_root_x, "ingest")
dir_build_x    <- file.path(project_root_x, "build")
dir_analysis_x <- file.path(project_root_x, "analysis")
dir_llm_x      <- file.path(project_root_x, "llm")
dir_sql_x      <- file.path(project_root_x, "sql")
dir_outputs_x  <- file.path(project_root_x, "outputs")
dir_docs_x     <- file.path(project_root_x, "docs")
dir_explore_x  <- file.path(project_root_x, "exploratory")

dir_data_raw_x          <- file.path(project_root_x, "data_raw")
dir_data_clean_x        <- file.path(project_root_x, "data_clean")
dir_data_intermediate_x <- file.path(project_root_x, "data_intermediate")

dir_duckdb_x <- file.path(dir_data_clean_x, "duckdb")
db_path_x    <- file.path(dir_duckdb_x, "osha.duckdb")


# Subdirectories ----------------------------------------------------------

dir_dol_downloads_x  <- file.path(dir_data_raw_x, "dol_downloads")
dir_dol_extract_x    <- file.path(dir_data_raw_x, "dol_extract")

dir_llm_prompts_x    <- file.path(dir_llm_x, "prompts")

dir_outputs_logs_x   <- file.path(dir_outputs_x, "logs")
dir_outputs_tables_x <- file.path(dir_outputs_x, "tables")
dir_outputs_figs_x   <- file.path(dir_outputs_x, "figures")


# Directory Initializer ---------------------------------------------------

ensure_project_dirs_x <- function() {
  
  # Data dirs
  dir.create(dir_data_raw_x, recursive = TRUE, showWarnings = FALSE)
  dir.create(dir_data_clean_x, recursive = TRUE, showWarnings = FALSE)
  dir.create(dir_data_intermediate_x, recursive = TRUE, showWarnings = FALSE)
  
  # DuckDB + raw data
  dir.create(dir_duckdb_x, recursive = TRUE, showWarnings = FALSE)
  dir.create(dir_dol_downloads_x, recursive = TRUE, showWarnings = FALSE)
  dir.create(dir_dol_extract_x, recursive = TRUE, showWarnings = FALSE)
  
  # Outputs
  dir.create(dir_outputs_x, recursive = TRUE, showWarnings = FALSE)
  dir.create(dir_outputs_logs_x, recursive = TRUE, showWarnings = FALSE)
  dir.create(dir_outputs_tables_x, recursive = TRUE, showWarnings = FALSE)
  dir.create(dir_outputs_figs_x, recursive = TRUE, showWarnings = FALSE)
  
  # LLM
  dir.create(dir_llm_prompts_x, recursive = TRUE, showWarnings = FALSE)
}