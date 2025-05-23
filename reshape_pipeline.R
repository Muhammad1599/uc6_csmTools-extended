# Pipeline for processing agricultural experimental datasets
library(dplyr)
library(tidyr)
library(lubridate)
library(countrycode)
library(stringr)

# Source the complete reshape function
source("complete_reshape_function.R")

# Function to show metadata
show_metadata <- function(metadata) {
  if (is.null(metadata)) {
    cat("\nNo metadata available")
    return()
  }
  
  cat("\nMETADATA:")
  for (field in names(metadata)) {
    cat("\n  ", field, ": ", metadata[[field]], sep="")
  }
  cat("\n")
}

# Function to show table head
show_table_head <- function(df, table_name) {
  if (is.null(df) || nrow(df) == 0) {
    cat("\n", paste0(table_name, ":"), "Empty or NULL table")
    return()
  }
  
  cat("\n", paste0(table_name, " (", nrow(df), " rows x ", ncol(df), " columns)"))
  cat("\nFirst rows:\n")
  print(head(df, 3))
  cat("\n")
}

# Function to display reshaped data heads
display_reshaped_data <- function(fmt_data, dataset_name, metadata) {
  cat("\n\n=================================================================")
  cat("\nRESHAPED DATA FOR", toupper(dataset_name))
  cat("\n=================================================================")
  
  # Display metadata first
  show_metadata(metadata)
  
  # Display FIELDS
  cat("\n--- FIELDS ---")
  show_table_head(fmt_data$FIELDS, "Fields table")
  
  # Display TREATMENTS
  cat("\n--- TREATMENTS ---")
  show_table_head(fmt_data$TREATMENTS, "Treatments table")
  
  # Display MANAGEMENT tables
  cat("\n--- MANAGEMENT ---")
  if (!is.null(fmt_data$MANAGEMENT)) {
    for (mgmt_name in names(fmt_data$MANAGEMENT)) {
      show_table_head(fmt_data$MANAGEMENT[[mgmt_name]], mgmt_name)
    }
  }
  
  # Display OBSERVATIONS tables
  cat("\n--- OBSERVATIONS ---")
  if (!is.null(fmt_data$OBSERVATIONS)) {
    for (obs_name in names(fmt_data$OBSERVATIONS)) {
      show_table_head(fmt_data$OBSERVATIONS[[obs_name]], obs_name)
    }
  }
  
  # Display BALANCES tables
  cat("\n--- BALANCES ---")
  if (!is.null(fmt_data$BALANCES) && length(fmt_data$BALANCES) > 0) {
    for (bal_name in names(fmt_data$BALANCES)) {
      show_table_head(fmt_data$BALANCES[[bal_name]], bal_name)
    }
  } else {
    cat("\nNo balance data available\n")
  }
}

# Function to process a single dataset and return components
process_dataset <- function(dataset_name) {
  # Get data path
  data_path <- file.path(".", "inst", "extdata", paste0("lte_", dataset_name), "0_raw")
  if (!dir.exists(data_path) && dataset_name == "rau") {
    data_path <- file.path(".", "inst", "extdata", "rau")
  }
  
  # Load and process raw data
  cat("\nProcessing", dataset_name, "dataset...\n")
  
  # Create db_list from raw data
  db_list <- list()
  files <- list.files(path = data_path, pattern = "\\.csv$", full.names = TRUE)
  
  for (file in files) {
    tryCatch({
      df <- read.csv(file, fileEncoding = "iso-8859-1", check.names = FALSE)
      # Remove BOM character if present
      colnames(df) <- gsub("^ï..", "", colnames(df))
      db_list[[basename(file)]] <- df
    }, error = function(e) {
      warning(paste("Error reading", file, ":", e$message))
    })
  }
  
  # Process data using complete reshape function
  result <- complete_reshape_exp_data(dataset_name)
  
  # Extract metadata
  metadata <- result$METADATA
  
  # Create formatted dataset
  formatted_data <- list(
    FIELDS = result$DESIGN,
    TREATMENTS = result$TREATMENTS,
    MANAGEMENT = result$MANAGEMENT,
    OBSERVATIONS = result$OBSERVATIONS,
    BALANCES = result$BALANCES
  )
  
  # Return all components
  return(list(
    db_list = db_list,
    metadata = metadata,
    formatted_data = formatted_data
  ))
}

# Process all datasets
datasets <- c("seehausen", "munch", "rau")
all_results <- list()

for (dataset in datasets) {
  all_results[[dataset]] <- process_dataset(dataset)
}

# Extract components for each dataset
seehausen_components <- all_results$seehausen
munch_components <- all_results$munch
rau_components <- all_results$rau

# Create individual components in the global environment
# Seehausen
seehausen_db_list <- seehausen_components$db_list
seehausen_metadata <- seehausen_components$metadata
seehausen_fmt <- seehausen_components$formatted_data

# Munch
munch_db_list <- munch_components$db_list
munch_metadata <- munch_components$metadata
munch_fmt <- munch_components$formatted_data

# Rau
rau_db_list <- rau_components$db_list
rau_metadata <- rau_components$metadata
rau_fmt <- rau_components$formatted_data

# Display reshaped data heads for each dataset
display_reshaped_data(seehausen_fmt, "Seehausen", seehausen_metadata)
display_reshaped_data(munch_fmt, "Munch", munch_metadata)
display_reshaped_data(rau_fmt, "Rau", rau_metadata)
