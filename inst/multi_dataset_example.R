#' Example of using the generalized ETL pipeline with multiple datasets
#' 
#' This script demonstrates how to use the reshape_multi_exp_data function to process
#' multiple different agricultural experimental datasets into the ICASA standard format.
#' 
#' @importFrom dplyr "%>%" select mutate relocate group_by arrange
#' @importFrom tidyr all_of everything

# Load libraries ----------------------------------------------------------

# Only run if libraries are not locally installed or to update them
#source("./inst/install_libraries.R")

# Load libraries, internal data (e.g., vocabularies and maps), and internal functions
library(csmTools)
library(dplyr)
library(tidyr)
library(lubridate)
library(readxl)
library(stringr)
library(countrycode)

# Source our new functions directly
source('R/reshape_multi_exp_data.R')
source('R/dataset_config_handler.R')

# Example 1: Process the Seehausen dataset ---------------------------------

cat("Processing Seehausen dataset...\n")

# Load Seehausen dataset files
see_path <- "./inst/extdata/lte_seehausen/0_raw"
see_files <- list.files(path = see_path, pattern = "\\.csv$")
see_paths <- sapply(see_files, function(x){ paste0(see_path, "/", x) })
see_db <- lapply(see_paths, function(x) { read.csv(x, fileEncoding = "iso-8859-1") })

# Simplify names
names(see_db) <- str_extract(names(see_db), "_V[0-9]+_[0-9]+_(.*)") # cut names after version number
names(see_db) <- gsub("_V[0-9]+_[0-9]+_", "", names(see_db)) # drop version number
names(see_db) <- sub("\\..*$", "", names(see_db)) # drop file extension

# Get metadata
see_metadata <- read_excel("./inst/extdata/lte_seehausen/0_raw/lte_seehausen_xls_metadata.xls")

# Create Seehausen configuration
see_config <- create_dataset_config("seehausen")

# Find mother table
# We already know it's the VERSUCHSAUFBAU table for Seehausen
see_mother_tbl <- see_db$VERSUCHSAUFBAU

# Reshape Seehausen data using our new function
see_fmt <- reshape_multi_exp_data(
  db = see_db, 
  metadata = see_metadata, 
  mother_tbl = see_mother_tbl,
  dataset_config = see_config
)

cat("Seehausen dataset processed successfully!\n")
cat("- Experiment name:", attr(see_fmt, "EXP_DETAILS"), "\n")
cat("- Site code:", attr(see_fmt, "SITE_CODE"), "\n")
if(!is.null(see_fmt$OBSERVED_Summary)) cat("- Found OBSERVED_Summary component\n")
if(!is.null(see_fmt$OBSERVED_TimeSeries)) cat("- Found OBSERVED_TimeSeries component\n")

# Example 2: Process the Munch dataset -------------------------------------

cat("\nProcessing Munch dataset...\n")

# Load Munch dataset files
munch_path <- "./inst/extdata/lte_Munch/0_raw"
munch_files <- list.files(path = munch_path, pattern = "\\.csv$")

cat("Found", length(munch_files), "CSV files in Munch dataset\n")

munch_paths <- sapply(munch_files, function(x){ paste0(munch_path, "/", x) })
munch_db <- lapply(munch_paths, function(x) { 
  tryCatch(
    read.csv(x, fileEncoding = "iso-8859-1"),
    error = function(e) {
      cat("Error reading file", x, ":", e$message, "\n")
      return(NULL)
    }
  )
})

# Remove any NULL entries (files that couldn't be read)
munch_db <- munch_db[!sapply(munch_db, is.null)]

# Simplify names - Extract everything between "v140_mun.V2_0_2012_" and ".csv"
names(munch_db) <- sapply(basename(munch_paths), function(x) {
  gsub("v140_mun\\.V2_0_2012_([^\\.]+)\\.csv", "\\1", x)
})

# Get metadata
munch_metadata <- read_excel("./inst/extdata/lte_Munch/0_raw/lte_mun_xls_metadata.xls")

# Create Munch configuration with appropriate settings for this dataset
munch_config <- create_dataset_config("munch")

# Special pre-processing for Munch dataset to link lab data to plot information
cat("Pre-processing Munch dataset for lab data tables...\n")

# Link BODENLABORWERTE with plot and year info
if("BODENLABORWERTE" %in% names(munch_db) && "PROBENAHME_BODEN" %in% names(munch_db)) {
  boden <- munch_db$BODENLABORWERTE
  probenahme <- munch_db$PROBENAHME_BODEN
  
  if("Probenahme_Boden_ID" %in% colnames(boden) && "ï..Probenahme_Boden_ID" %in% colnames(probenahme)) {
    cat("  Linking BODENLABORWERTE with PROBENAHME_BODEN...\n")
    linked_boden <- boden %>%
      left_join(probenahme %>% 
                  select(ï..Probenahme_Boden_ID, Versuchsjahr, Parzelle_ID),
                by = c("Probenahme_Boden_ID" = "ï..Probenahme_Boden_ID"))
    
    munch_db$BODENLABORWERTE <- linked_boden
    cat("  BODENLABORWERTE linked successfully.\n")
  }
}

# Link PFLANZENLABORWERTE with plot and year info
if("PFLANZENLABORWERTE" %in% names(munch_db) && "PROBENAHME_PFLANZEN" %in% names(munch_db)) {
  pflanzen <- munch_db$PFLANZENLABORWERTE
  probenahme <- munch_db$PROBENAHME_PFLANZEN
  
  if("Probenahme_Pflanzen_ID" %in% colnames(pflanzen) && "ï..Probenahme_Pflanzen_ID" %in% colnames(probenahme)) {
    cat("  Linking PFLANZENLABORWERTE with PROBENAHME_PFLANZEN...\n")
    linked_pflanzen <- pflanzen %>%
      left_join(probenahme %>% 
                  select(ï..Probenahme_Pflanzen_ID, Versuchsjahr, Parzelle_ID),
                by = c("Probenahme_Pflanzen_ID" = "ï..Probenahme_Pflanzen_ID"))
    
    munch_db$PFLANZENLABORWERTE <- linked_pflanzen
    cat("  PFLANZENLABORWERTE linked successfully.\n")
  }
}

# Select VERSUCHSAUFBAU as the mother table
munch_mother_tbl <- munch_db$VERSUCHSAUFBAU

# Reshape Munch data using our new function
munch_fmt <- reshape_multi_exp_data(
  db = munch_db, 
  metadata = munch_metadata, 
  mother_tbl = munch_mother_tbl,
  dataset_config = munch_config
)

cat("Munch dataset processed successfully!\n")
cat("- Experiment name:", attr(munch_fmt, "EXP_DETAILS"), "\n")
cat("- Site code:", attr(munch_fmt, "SITE_CODE"), "\n")
if(!is.null(munch_fmt$OBSERVED_Summary)) cat("- Found OBSERVED_Summary component with", length(munch_fmt$OBSERVED_Summary), "tables\n")
if(!is.null(munch_fmt$OBSERVED_TimeSeries)) cat("- Found OBSERVED_TimeSeries component with", length(munch_fmt$OBSERVED_TimeSeries), "tables\n")

# Compare the results of both datasets -------------------------------------

cat("\nComparing both datasets:\n")
cat("Seehausen has", length(see_fmt$OBSERVED_Summary), "summary tables and", 
    length(see_fmt$OBSERVED_TimeSeries), "time series tables\n")
cat("Munch has", length(munch_fmt$OBSERVED_Summary), "summary tables and", 
    length(munch_fmt$OBSERVED_TimeSeries), "time series tables\n")

# For Munch dataset, show what observed data tables were included
if(!is.null(munch_fmt$OBSERVED_Summary) && length(munch_fmt$OBSERVED_Summary) > 0) {
  cat("\nMunch OBSERVED_Summary tables:\n")
  for(tbl_name in names(munch_fmt$OBSERVED_Summary)) {
    cat("- ", tbl_name, " (", nrow(munch_fmt$OBSERVED_Summary[[tbl_name]]), " rows)\n", sep="")
  }
}

if(!is.null(munch_fmt$OBSERVED_TimeSeries) && length(munch_fmt$OBSERVED_TimeSeries) > 0) {
  cat("\nMunch OBSERVED_TimeSeries tables:\n")
  for(tbl_name in names(munch_fmt$OBSERVED_TimeSeries)) {
    cat("- ", tbl_name, " (", nrow(munch_fmt$OBSERVED_TimeSeries[[tbl_name]]), " rows)\n", sep="")
  }
}

# Notes on extending the pipeline for additional datasets ----------------

cat("\nTo extend this pipeline for additional datasets:\n")
cat("1. Create dataset-specific configurations using create_dataset_config()\n")
cat("2. Use these configurations with reshape_multi_exp_data()\n")
cat("3. For complex datasets, extend the configuration with additional mappings\n")
cat("4. The output will be in the standardized ICASA format regardless of input structure\n") 