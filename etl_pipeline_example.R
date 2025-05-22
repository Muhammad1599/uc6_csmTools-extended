#' Example of UC6 ETL pipeline with LTE data from experimental datasets
#' 
#' This pipeline demonstrates the use of the generalized reshape function with multiple
#' long-term experiment datasets (Seehausen, Munch, and Rau).
#' 
#' @importFrom stringr str_extract
#' @importFrom readxl read_excel
#' @importFrom dplyr "%>%" select mutate relocate group_by arrange bind_rows filter if_all dense_rank desc ungroup
#' @importFrom dplyr cur_group_id distinct where across left_join
#' @importFrom tidyr all_of everything separate starts_with unnest
#' @importFrom tibble as_tibble
#' @importFrom lubridate as_date year
#' @importFrom DSSAT read_sol write_sol write_filex write_filea write_wth write_dssbatch run_dssat read_output
#' @importFrom ggplot2 ggplot aes geom_line geom_point geom_hline labs guides guide_legend theme_bw theme
#' @importFrom ggplot2 scale_colour_manual scale_size_manual scale_linewidth_manual
#' @importFrom ggplot2 element_text element_blank
#' 

# Load libraries ----------------------------------------------------------

# Only run if libraries are not locally installed or to update them (NB: all at once!)
#source("./inst/install_libraries.R")

# Load libraries, internal data (e.g., vocabularies and maps), and internal functions
try(detach("package:csmTools", unload=TRUE)) # dev only, only install once in prod!
remotes::install_local("./", force = TRUE)
source("./inst/load_libraries.R")
library(csmTools)

# At the start of your script
setwd("/Users/muhammadarslan/Downloads/uc6_csmTools-main 2 copy")

# Source the generalized reshape function
source("./generalized_reshape_function.R")

# ---- Load and process Seehausen dataset --------------------------------

cat("\n================= PROCESSING SEEHAUSEN DATASET =================\n")

# Load Seehausen data
cat("Loading Seehausen dataset files...\n")
seehausen_files <- list.files(path = "./inst/extdata/lte_seehausen/0_raw", pattern = "\\.csv$")
seehausen_paths <- sapply(seehausen_files, function(x){ paste0("./inst/extdata/lte_seehausen/0_raw/", x) })
seehausen_list <- lapply(seehausen_paths, function(x) { file <- read.csv(x, fileEncoding = "iso-8859-1") })

# Simplify names
names(seehausen_list) <- str_extract(names(seehausen_list), "_V[0-9]+_[0-9]+_(.*)") # cut names after version number
names(seehausen_list) <- gsub("_V[0-9]+_[0-9]+_", "", names(seehausen_list)) # drop version number
names(seehausen_list) <- sub("\\..*$", "", names(seehausen_list)) # drop file extension

# Get metadata
cat("Loading Seehausen metadata...\n")
seehausen_metadata <- tryCatch(
  read_excel("./inst/extdata/lte_seehausen/0_raw/lte_seehausen_xls_metadata.xls"),
  error = function(e) {
    cat("Warning: Could not read metadata file. Using default configuration.\n")
    NULL
  }
)

# Process Seehausen dataset
cat("Processing Seehausen dataset using generalized reshape function...\n")
if(!is.null(seehausen_metadata) && "VERSUCHSAUFBAU" %in% names(seehausen_list)) {
  # Method 1: Using full parameters
  seehausen_fmt <- reshape_exp_data(db = seehausen_list, 
                                   metadata = seehausen_metadata, 
                                   mother_tbl = seehausen_list$VERSUCHSAUFBAU)
} else {
  # Method 2: Using simplified dataset name
  seehausen_fmt <- reshape_exp_data("seehausen")
}

# ---- Load and process Munch dataset -----------------------------------

cat("\n================= PROCESSING MUNCH DATASET =================\n")

# Load Munch data
cat("Loading Munch dataset files...\n")
munch_files <- list.files(path = "./inst/extdata/lte_Munch/0_raw", pattern = "\\.csv$")
munch_paths <- sapply(munch_files, function(x){ paste0("./inst/extdata/lte_Munch/0_raw/", x) })
munch_list <- lapply(munch_paths, function(x) { 
  tryCatch(
    read.csv(x, fileEncoding = "iso-8859-1"),
    error = function(e) {
      cat("Warning: Could not read file", x, "\n")
      NULL
    }
  )
})
munch_list <- munch_list[!sapply(munch_list, is.null)]

# Simplify names
names(munch_list) <- str_extract(names(munch_list), "_V[0-9]+_[0-9]+_(.*)") # cut names after version number
names(munch_list) <- gsub("_V[0-9]+_[0-9]+_", "", names(munch_list)) # drop version number
names(munch_list) <- sub("\\..*$", "", names(munch_list)) # drop file extension

# Get metadata
cat("Loading Munch metadata...\n")
munch_metadata <- tryCatch(
  read_excel("./inst/extdata/lte_Munch/0_raw/lte_munch_xls_metadata.xls"),
  error = function(e) {
    cat("Warning: Could not read metadata file. Using default configuration.\n")
    NULL
  }
)

# Process Munch dataset
cat("Processing Munch dataset using generalized reshape function...\n")
if(!is.null(munch_metadata) && "VERSUCHSAUFBAU" %in% names(munch_list)) {
  # Method 1: Using full parameters
  munch_fmt <- reshape_exp_data(db = munch_list, 
                               metadata = munch_metadata, 
                               mother_tbl = munch_list$VERSUCHSAUFBAU)
} else {
  # Method 2: Using simplified dataset name
  munch_fmt <- reshape_exp_data("munch")
}

# ---- Load and process Rau dataset ------------------------------------

cat("\n================= PROCESSING RAU DATASET =================\n")

# Load Rau data
cat("Loading Rau dataset files...\n")
rau_files <- list.files(path = "./inst/extdata/rau", pattern = "\\.csv$")
rau_paths <- sapply(rau_files, function(x){ paste0("./inst/extdata/rau/", x) })
rau_list <- lapply(rau_paths, function(x) { 
  tryCatch(
    read.csv(x, fileEncoding = "iso-8859-1"),
    error = function(e) {
      cat("Warning: Could not read file", x, "\n")
      NULL
    }
  )
})
rau_list <- rau_list[!sapply(rau_list, is.null)]

# Simplify names
names(rau_list) <- str_extract(names(rau_list), "_L[0-9]+_V[0-9]+_[0-9]+_(.*)") # cut names after version number
names(rau_list) <- gsub("_L[0-9]+_V[0-9]+_[0-9]+_", "", names(rau_list)) # drop version number
names(rau_list) <- sub("\\..*$", "", names(rau_list)) # drop file extension

# Get metadata
cat("Loading Rau metadata...\n")
rau_metadata <- tryCatch(
  read_excel("./inst/extdata/rau/ltfe_rauischholzhausen_xls_metadata.xls"),
  error = function(e) {
    cat("Warning: Could not read metadata file. Using default configuration.\n")
    NULL
  }
)

# Process Rau dataset
cat("Processing Rau dataset using generalized reshape function...\n")
if(!is.null(rau_metadata) && "VERSUCHSAUFBAU" %in% names(rau_list)) {
  # Method 1: Using full parameters
  rau_fmt <- reshape_exp_data(db = rau_list, 
                             metadata = rau_metadata, 
                             mother_tbl = rau_list$VERSUCHSAUFBAU)
} else {
  # Method 2: Using simplified dataset name
  rau_fmt <- reshape_exp_data("rau")
}

# ---- Display and Compare Results -------------------------------------

cat("\n================= DATASETS SUMMARY =================\n")

# Function to display dataset contents
display_dataset_contents <- function(fmt, name) {
  cat(paste0("\n\n========== ", name, " DATASET CONTENTS ==========\n\n"))
  
  # Display general structure
  cat("Components:", paste(names(fmt), collapse=", "), "\n")
  cat("Experiment details:", attr(fmt, "EXP_DETAILS"), "\n")
  cat("Site code:", attr(fmt, "SITE_CODE"), "\n")
  cat("Number of treatments:", nrow(fmt$TREATMENTS), "\n")
  cat("Number of management tables:", length(fmt$MANAGEMENT), "\n")
  cat("Number of observed summary tables:", length(fmt$OBSERVED_Summary), "\n")
  cat("Number of observed timeseries tables:", length(fmt$OBSERVED_TimeSeries), "\n\n")
  
  # Display content of each component
  cat("--- GENERAL Information ---\n")
  print(fmt$GENERAL)
  cat("\n")
  
  cat("--- FIELDS Information ---\n")
  print(head(fmt$FIELDS, 5))
  cat("... (showing 5 of", nrow(fmt$FIELDS), "rows)\n\n")
  
  cat("--- TREATMENTS Information ---\n")
  print(head(fmt$TREATMENTS, 5))
  cat("... (showing 5 of", nrow(fmt$TREATMENTS), "rows)\n\n")
  
  # Display some management tables
  cat("--- MANAGEMENT Tables ---\n")
  if (length(fmt$MANAGEMENT) > 0) {
    # Show first two management tables
    i <- 0
    for (tbl_name in names(fmt$MANAGEMENT)[1:min(2, length(fmt$MANAGEMENT))]) {
      i <- i + 1
      cat(paste0("Management Table ", i, ": ", tbl_name, "\n"))
      print(head(fmt$MANAGEMENT[[tbl_name]], 3))
      cat("... (showing 3 of", nrow(fmt$MANAGEMENT[[tbl_name]]), "rows)\n\n")
    }
  } else {
    cat("No management tables found.\n\n")
  }
  
  # Display some observed summary tables
  cat("--- OBSERVED_Summary Tables ---\n")
  if (length(fmt$OBSERVED_Summary) > 0) {
    # Show first two observed summary tables
    i <- 0
    for (tbl_name in names(fmt$OBSERVED_Summary)[1:min(2, length(fmt$OBSERVED_Summary))]) {
      i <- i + 1
      cat(paste0("Observed Summary Table ", i, ": ", tbl_name, "\n"))
      print(head(fmt$OBSERVED_Summary[[tbl_name]], 3))
      cat("... (showing 3 of", nrow(fmt$OBSERVED_Summary[[tbl_name]]), "rows)\n\n")
    }
  } else {
    cat("No observed summary tables found.\n\n")
  }
  
  # Display observed timeseries tables (if any)
  cat("--- OBSERVED_TimeSeries Tables ---\n")
  if (length(fmt$OBSERVED_TimeSeries) > 0) {
    # Show first two observed timeseries tables
    i <- 0
    for (tbl_name in names(fmt$OBSERVED_TimeSeries)[1:min(2, length(fmt$OBSERVED_TimeSeries))]) {
      i <- i + 1
      cat(paste0("Observed TimeSeries Table ", i, ": ", tbl_name, "\n"))
      print(head(fmt$OBSERVED_TimeSeries[[tbl_name]], 3))
      cat("... (showing 3 of", nrow(fmt$OBSERVED_TimeSeries[[tbl_name]]), "rows)\n\n")
    }
  } else {
    cat("No observed timeseries tables found.\n\n")
  }
}

# Display contents of all three datasets
display_dataset_contents(seehausen_fmt, "SEEHAUSEN")
display_dataset_contents(munch_fmt, "MUNCH")
display_dataset_contents(rau_fmt, "RAU")

cat("\nPipeline execution completed successfully.\n") 
