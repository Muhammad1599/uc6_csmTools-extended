# Complete Improved Reshape Function for Agricultural Experimental Datasets
# This implementation processes all columns and links related tables

library(dplyr)
library(tidyr)
library(lubridate)
library(countrycode)
library(stringr)

#' Complete reshape function for agricultural experimental data
#' 
#' @param dataset_name Dataset name: "seehausen", "munch", or "rau"
#' @param data_path Path to the dataset files (default: "./inst/extdata/")
#' 
#' @return A list containing all components of the reshaped data
#' 
complete_reshape_exp_data <- function(dataset_name) {
  # Dataset configurations
  config <- list(
    seehausen = list(
      site_info = list(
        seehausen = list(
          site = "Seehausen",
          country = "Germany",
          location = list(lat = 51.5, lon = 11.3),
          exp_details = "Long-term field experiment"
        )
      ),
      version = "1.0"
    ),
    munch = list(
      site_info = list(
        munch = list(
          site = "Munch",
          country = "Germany",
          location = list(lat = 48.4, lon = 11.7),
          exp_details = "Long-term field experiment"
        )
      ),
      version = "1.0"
    ),
    rau = list(
      site_info = list(
        rau = list(
          site = "Rauischholzhausen",
          country = "Germany",
          location = list(lat = 50.8, lon = 8.9),
          exp_details = "Long-term field experiment"
        )
      ),
      version = "1.0"
    )
  )
  
  # Get dataset configuration
  dataset_config <- config[[dataset_name]]
  if (is.null(dataset_config)) {
    stop(paste("Unknown dataset:", dataset_name))
  }
  
  # Determine data path
  data_path <- file.path(".", "inst", "extdata", paste0("lte_", dataset_name), "0_raw")
  if (!dir.exists(data_path)) {
    # Try alternate path for Rau dataset
    if (dataset_name == "rau") {
      data_path <- file.path(".", "inst", "extdata", "rau")
      if (!dir.exists(data_path)) {
        stop(paste("Data directory not found:", data_path))
      }
    } else {
      stop(paste("Data directory not found:", data_path))
    }
  }
  
  cat("Loading dataset from", data_path, "\n")
  
  # Load dataset
  db <- load_dataset(dataset_name, data_path)
  
  # Validate dataset
  validate_dataset(db, dataset_name)
  
  # Create tables list with data path
  tables <- list(data_path = data_path)
  
  # Process tables
  result <- process_tables(db, tables, dataset_config)
  
  return(result)
}

#' Load dataset from CSV files
#' 
#' @param dataset_name Dataset name
#' @param data_path Path to the data files
#' 
#' @return List of data frames
load_dataset <- function(dataset_name, data_path) {
  # Get all CSV files in the directory
  files <- list.files(path = data_path, pattern = "\\.csv$", full.names = TRUE)
  
  if (length(files) == 0) {
    stop(paste("No CSV files found in", data_path))
  }
  
  # Load all CSV files with improved error handling
  db <- list()
  for (file in files) {
    tryCatch({
      df <- read.csv(file, fileEncoding = "iso-8859-1", check.names = FALSE)
      
      # Basic cleaning and validation
      if (ncol(df) == 0 || nrow(df) == 0) {
        warning(paste("Empty file:", file))
        next
      }
      
      # Remove BOM character from column names if present
      colnames(df) <- gsub("^ï..", "", colnames(df))
      
      # Remove completely empty columns
      df <- df[, colSums(!is.na(df)) > 0]
      
      # Convert character columns that should be numeric
      for (col in names(df)) {
        if (is.character(df[[col]]) && all(grepl("^[0-9.]+$", na.omit(df[[col]])))) {
          df[[col]] <- as.numeric(df[[col]])
        }
      }
      
      # Add to database with basename as key
      db[[basename(file)]] <- df
      
    }, error = function(e) {
      warning(paste("Error reading", file, ":", e$message))
    })
  }
  
  if (length(db) == 0) {
    stop("No valid data files could be loaded")
  }
  
  # Standardize column names
  db <- standardize_column_names(db, dataset_name)
  
  return(db)
}

#' Standardize column names across tables
#' 
#' @param db List of data frames
#' @param dataset_name Dataset name
#' 
#' @return Updated list of data frames with standardized column names
standardize_column_names <- function(db, dataset_name) {
  # Dataset-specific column mappings
  dataset_mappings <- list(
    seehausen = list(
      "Year" = c("Versuchsjahr", "Jahr"),
      "Plot_id" = c("Parzelle_ID", "PARZELLE_ID"),
      "Treatment_id" = c("Pruefglied_ID", "PRUEFGLIED_ID"),
      "Rep_no" = c("Wiederholung", "WIEDERHOLUNG"),
      "Crop" = c("Kultur", "KULTUR"),
      "Yield" = c("Ertrag", "ERTRAG"),
      "Date" = c("Datum", "DATUM", "Termin")
    ),
    munch = list(
      "Year" = c("Versuchsjahr", "Jahr"),
      "Plot_id" = c("Parzelle_ID", "PARZID"),
      "Treatment_id" = c("Pruefglied_ID", "PRUEFGLIED"),
      "Rep_no" = c("Wiederholung", "WDHLG"),
      "Crop" = c("Kultur", "FRUCHT"),
      "Yield" = c("Ertrag", "ERTRAG"),
      "Date" = c("Datum", "DATUM", "Termin")
    ),
    rau = list(
      "Year" = c("Versuchsjahr", "Jahr"),
      "Plot_id" = c("Parzelle_ID", "PARZELLE_ID"),
      "Treatment_id" = c("Pruefglied_ID", "PRUEFGLIED_ID"),
      "Rep_no" = c("Wiederholung", "WIEDERHOLUNG"),
      "Crop" = c("Kultur", "FRUCHT"),
      "Yield" = c("Ertrag", "ERTRAG"),
      "Date" = c("Datum", "DATUM", "Termin")
    )
  )
  
  # Get mappings for current dataset
  mappings <- dataset_mappings[[dataset_name]]
  if (is.null(mappings)) {
    warning(paste("No column mappings defined for dataset:", dataset_name))
    return(db)
  }
  
  # Apply mappings to each dataframe
  for (i in seq_along(db)) {
    df_cols <- colnames(db[[i]])
    
    # Apply exact matches (case-insensitive)
    for (std_name in names(mappings)) {
      old_cols <- mappings[[std_name]]
      matches <- which(tolower(df_cols) %in% tolower(old_cols))
      if (length(matches) > 0) {
        colnames(db[[i]])[matches] <- std_name
      }
    }
    
    # Handle fuzzy matches for similar column names
    for (std_name in names(mappings)) {
      old_cols <- mappings[[std_name]]
      for (old_col in old_cols) {
        similar_cols <- grep(paste0("^", old_col, ".*$"), df_cols, value = TRUE, ignore.case = TRUE)
        if (length(similar_cols) > 0) {
          for (sim_col in similar_cols) {
            if (!(tolower(sim_col) %in% tolower(unlist(mappings)))) {
              colnames(db[[i]])[df_cols == sim_col] <- std_name
            }
          }
        }
      }
    }
  }
  
  return(db)
}

#' Get dataset configuration
#' 
#' @param dataset_name Name of the dataset
#' 
#' @return Configuration list
get_dataset_config <- function(dataset_name) {
  if (dataset_name == "seehausen") {
    return(list(
      site_info = list(
        exp_details = "LTE Seehausen, Duengungs-Kombinationsversuch Seehausen (F1-70)",
        site = "Seehausen",
        country = "Germany",
        location = list(lat = 51.401884, lon = 12.418934)
      ),
      year_variants = c("Versuchsjahr", "JAHR", "ERNTEJAHR", "Jahr"),
      plot_variants = c("Parzelle_ID", "PARZID", "ï..Parzelle_ID", "Parzelle"),
      rep_variants = c("Wiederholung", "WDHLG", "REPLIKAT"),
      treatment_candidates = c("Pruefglied_ID", "Treatment_id", "Pruefglied", "Variante", "Var", "Treatment"),
      table_patterns = list(
        experiment_design = "VERSUCHSAUFBAU|Versuchsaufbau|design",
        plant_sampling = "PFLANZE|Pflanze|plant",
        soil_sampling = "BODEN|Boden|soil",
        yield = "ERTRAG|Ertrag|yield",
        fertilization = "DUENGUNG|Duengung|fertilization",
        tillage = "BODENBEARBEITUNG|Bodenbearbeitung|tillage",
        sowing = "AUSSAAT|Aussaat|sowing",
        harvest = "ERNTE|Ernte|harvest"
      ),
      lab_value_link_columns = c("Proben_ID", "Probe_ID", "Sample_ID")
    ))
  } else if (dataset_name == "munch") {
    return(list(
      site_info = list(
        exp_details = "LTE Muencheberg, Static Fertilization Experiment V140",
        site = "Muencheberg",
        country = "Germany",
        location = list(lat = 52.515, lon = 14.122)
      ),
      year_variants = c("Versuchsjahr", "JAHR", "ERNTEJAHR", "Jahr"),
      plot_variants = c("Parzelle_ID", "PARZID", "ï..Parzelle_ID", "Parzelle"),
      rep_variants = c("Wiederholung", "WDHLG", "REPLIKAT"),
      treatment_candidates = c("Pruefglied_ID", "Treatment_id", "Pruefglied", "Variante", "Var", "Treatment"),
      table_patterns = list(
        experiment_design = "VERSUCHSAUFBAU|Versuchsaufbau|design",
        plant_sampling = "PFLANZE|Pflanze|plant",
        soil_sampling = "BODEN|Boden|soil",
        yield = "ERTRAG|Ertrag|yield",
        fertilization = "DUENGUNG|Duengung|fertilization",
        tillage = "BODENBEARBEITUNG|Bodenbearbeitung|tillage",
        sowing = "AUSSAAT|Aussaat|sowing",
        harvest = "ERNTE|Ernte|harvest"
      ),
      lab_value_link_columns = c("Proben_ID", "Probe_ID", "Sample_ID")
    ))
  } else if (dataset_name == "rau") {
    return(list(
      site_info = list(
        exp_details = "LTE Rauischholzhausen, Long-term fertilization experiment",
        site = "Rauischholzhausen",
        country = "Germany",
        location = list(lat = 50.6747, lon = 8.8809)
      ),
      year_variants = c("Versuchsjahr", "JAHR", "ERNTEJAHR", "Jahr"),
      plot_variants = c("Parzelle_ID", "PARZID", "ï..Parzelle_ID", "Parzelle"),
      rep_variants = c("Wiederholung", "WDHLG", "REPLIKAT"),
      treatment_candidates = c("Pruefglied_ID", "Treatment_id", "Pruefglied", "Variante", "Var", "Treatment"),
      table_patterns = list(
        experiment_design = "VERSUCHSAUFBAU|Versuchsaufbau|design",
        plant_sampling = "PFLANZE|Pflanze|plant",
        soil_sampling = "BODEN|Boden|soil",
        yield = "ERTRAG|Ertrag|yield",
        fertilization = "DUENGUNG|Duengung|fertilization",
        tillage = "BODENBEARBEITUNG|Bodenbearbeitung|tillage",
        sowing = "AUSSAAT|Aussaat|sowing",
        harvest = "ERNTE|Ernte|harvest"
      ),
      lab_value_link_columns = c("Proben_ID", "Probe_ID", "Sample_ID")
    ))
  } else {
    stop(paste("Unknown dataset:", dataset_name))
  }
}

#' Identify key tables in the dataset
#' 
#' @param db List of data frames
#' @param config Dataset configuration
#' 
#' @return List of identified tables by category
identify_key_tables <- function(db, config) {
  # Initialize result
  result <- list(
    design = NULL,
    management = list(),
    observed_summary = list(),
    observed_timeseries = list(),
    other = list()
  )
  
  # Find design table
  for (tbl_name in names(db)) {
    if (grepl(config$table_patterns$experiment_design, tbl_name, ignore.case = TRUE)) {
      result$design <- tbl_name
      break
    }
  }
  
  # If not found by name, try to find by structure
  if (is.null(result$design)) {
    for (tbl_name in names(db)) {
      df <- db[[tbl_name]]
      if ("Year" %in% colnames(df) && 
          "Plot_id" %in% colnames(df) && 
          "Treatment_id" %in% colnames(df)) {
        result$design <- tbl_name
        break
      }
    }
  }
  
  # Process remaining tables
  for (tbl_name in names(db)) {
    if (tbl_name == result$design) next
    
    # Check if it's a management table
    if (grepl(paste(c(
      config$table_patterns$fertilization,
      config$table_patterns$tillage,
      config$table_patterns$sowing,
      config$table_patterns$harvest
    ), collapse = "|"), tbl_name, ignore.case = TRUE)) {
      result$management[[tbl_name]] <- db[[tbl_name]]
    }
    # Check if it's an observed summary table
    else if (grepl(paste(c(
      config$table_patterns$plant_sampling,
      config$table_patterns$soil_sampling,
      config$table_patterns$yield
    ), collapse = "|"), tbl_name, ignore.case = TRUE)) {
      result$observed_summary[[tbl_name]] <- db[[tbl_name]]
    }
    # Check for time series data (has date columns)
    else if (has_multiple_date_columns(db[[tbl_name]])) {
      result$observed_timeseries[[tbl_name]] <- db[[tbl_name]]
    }
    # Otherwise, categorize as other
    else {
      result$other[[tbl_name]] <- db[[tbl_name]]
    }
  }
  
  # Try to link lab value tables with their sample tables
  result <- link_related_tables(db, result, config)
  
  return(result)
}

#' Check if a data frame has multiple date columns
#' 
#' @param df Data frame to check
#' 
#' @return Boolean indicating if the data frame has multiple date columns
has_multiple_date_columns <- function(df) {
  date_col_count <- 0
  for (col in colnames(df)) {
    if (is_date_column(df[[col]])) {
      date_col_count <- date_col_count + 1
      if (date_col_count > 1) return(TRUE)
    }
  }
  return(FALSE)
}

#' Check if a vector is likely a date
#' 
#' @param x Vector to check
#' 
#' @return Boolean indicating if the vector is likely a date
is_date_column <- function(x) {
  # If already a Date class
  if (inherits(x, "Date")) return(TRUE)
  
  # If character, check for date patterns
  if (is.character(x)) {
    # Remove empty strings
    x <- x[x != ""]
    if (length(x) == 0) return(FALSE)
    
    # Try common date formats
    date_patterns <- c(
      "^\\d{4}-\\d{2}-\\d{2}$",  # YYYY-MM-DD
      "^\\d{2}/\\d{2}/\\d{4}$",  # MM/DD/YYYY
      "^\\d{2}\\.\\d{2}\\.\\d{4}$"  # DD.MM.YYYY
    )
    
    for (pattern in date_patterns) {
      if (mean(grepl(pattern, x), na.rm = TRUE) > 0.7) {
        return(TRUE)
      }
    }
  }
  
  return(FALSE)
}

#' Link related tables (e.g., sample tables with lab value tables)
#' 
#' @param db List of all data frames
#' @param tables List of categorized tables
#' @param config Dataset configuration
#' 
#' @return Updated list of categorized tables with linked tables
link_related_tables <- function(db, tables, config) {
  # Identify potential lab value tables
  lab_value_tables <- tables$other
  
  # For each observed summary table (likely sample tables)
  for (sample_tbl_name in names(tables$observed_summary)) {
    sample_tbl <- db[[sample_tbl_name]]
    
    # Check if it has a link column (e.g., Sample_ID)
    link_cols <- intersect(colnames(sample_tbl), config$lab_value_link_columns)
    
    if (length(link_cols) > 0) {
      link_col <- link_cols[1]
      
      # Find lab value tables that have the same link column
      for (lab_tbl_name in names(lab_value_tables)) {
        lab_tbl <- db[[lab_tbl_name]]
        
        if (link_col %in% colnames(lab_tbl)) {
          # This is likely a related lab value table
          # Move it from 'other' to 'observed_summary'
          tables$observed_summary[[lab_tbl_name]] <- lab_tbl
          tables$other[[lab_tbl_name]] <- NULL
          
          # Mark it as linked to the sample table
          attr(tables$observed_summary[[lab_tbl_name]], "linked_to") <- sample_tbl_name
          attr(tables$observed_summary[[lab_tbl_name]], "link_column") <- link_col
        }
      }
    }
  }
  
  return(tables)
}

#' Process the tables to create the final output structure
#' 
#' @param db List of all data frames
#' @param tables List of categorized tables
#' @param config Dataset configuration
#' 
#' @return List containing the final structured output
process_tables <- function(db, tables, config) {
  # Process existing components
  design_components <- process_design_tables(db, tables, config)
  
  # Initialize tables
  management_tables <- list()
  observation_tables <- list()
  balance_tables <- list()
  
  # Process balance tables first
  balance_patterns <- list(
    NITROGEN = c(".*STICKSTOFFBILANZ.*", ".*N_BILANZ.*", ".*NBILANZ.*"),
    ENERGY = c(".*ENERGIEBILANZ.*", ".*E_BILANZ.*", ".*EBILANZ.*")
  )
  
  for (balance_type in names(balance_patterns)) {
    pattern <- paste(balance_patterns[[balance_type]], collapse = "|")
    matching_files <- grep(pattern, names(db), value = TRUE, ignore.case = TRUE)
    
    if (length(matching_files) > 0) {
      for (file in matching_files) {
        df <- db[[file]]
        if (!is.null(df) && nrow(df) > 0) {
          processed_df <- process_balance_data(df, balance_type)
          if (!is.null(processed_df)) {
            if (is.null(balance_tables[[balance_type]])) {
              balance_tables[[balance_type]] <- processed_df
            } else {
              balance_tables[[balance_type]] <- rbind(balance_tables[[balance_type]], processed_df)
            }
          }
        }
      }
    }
  }
  
  # Process management tables
  management_patterns <- c(
    ".*ERNTE.*", ".*DUENGUNG.*", ".*BODENBEARBEITUNG.*", ".*AUSSAAT.*",
    ".*FRUCHTFOLGE.*"
  )
  
  # Process observation tables
  observation_patterns <- c(
    ".*PFLANZENSCHUTZ.*", ".*BODENBIOLOGIE.*", ".*PROBENAHME.*", ".*LABORWERTE.*",
    ".*BONITUR.*", ".*KLIMADATEN.*"
  )
  
  # Get dataset name from config
  dataset_name <- names(config$site_info)[1]
  
  # Process tables based on patterns
  for (tbl_name in names(db)) {
    # Skip already processed balance tables
    if (any(sapply(unlist(balance_patterns), function(p) grepl(p, tbl_name, ignore.case = TRUE)))) next
    
    if (any(sapply(management_patterns, function(p) grepl(p, tbl_name, ignore.case = TRUE)))) {
      table_type <- gsub(".*_(.*?)\\.csv$", "\\1", tbl_name)
      management_tables[[table_type]] <- db[[tbl_name]]
    } else if (any(sapply(observation_patterns, function(p) grepl(p, tbl_name, ignore.case = TRUE)))) {
      table_type <- gsub(".*_(.*?)\\.csv$", "\\1", tbl_name)
      if (grepl("KLIMADATEN", tbl_name, ignore.case = TRUE)) {
        observation_tables[[table_type]] <- process_climate_data(db[[tbl_name]])
      } else if (grepl("BONITUR", tbl_name, ignore.case = TRUE)) {
        observation_tables[[table_type]] <- process_crop_assessment(db[[tbl_name]])
      } else {
        observation_tables[[table_type]] <- db[[tbl_name]]
      }
    }
  }
  
  # Create metadata with enhanced information
  metadata <- process_metadata(db, tables, config)
  
  # Return all components
  return(list(
    METADATA = metadata,
    DESIGN = design_components$FIELDS,
    TREATMENTS = design_components$TREATMENTS,
    MANAGEMENT = management_tables,
    OBSERVATIONS = observation_tables,
    BALANCES = balance_tables
  ))
}

# Helper function to concatenate strings
"%+%" <- function(a, b) paste0(a, b)

#' Wrapper to maintain compatibility with the original interface
#' 
#' @param db Dataset name or list of dataframes
#' @param metadata Optional metadata
#' @param mother_tbl Optional mother table
#' 
#' @return Reshaped experimental data
reshape_exp_data <- function(db, metadata = NULL, mother_tbl = NULL) {
  if (is.character(db) && length(db) == 1) {
    return(complete_reshape_exp_data(db))
  } else if (is.list(db)) {
    # Identify the dataset type from the tables
    dataset_type <- "generic"
    table_names <- names(db)
    
    if (any(grepl("seehausen", table_names, ignore.case = TRUE))) {
      dataset_type <- "seehausen"
    } else if (any(grepl("munch", table_names, ignore.case = TRUE))) {
      dataset_type <- "munch"
    } else if (any(grepl("rauischholzhausen|rau", table_names, ignore.case = TRUE))) {
      dataset_type <- "rau"
    }
    
    # Process using the appropriate configuration
    config <- get_dataset_config(dataset_type)
    
    # Identify key tables
    tables <- identify_key_tables(db, config)
    
    # Process tables to create standardized components
    return(process_tables(db, tables, config))
  } else {
    stop("Invalid input: expected dataset name or list of data frames")
  }
}

validate_dataset <- function(db, dataset_name) {
  # Map dataset-specific file patterns to required tables
  file_patterns <- list(
    seehausen = list(
      design = ".*VERSUCHSAUFBAU.*|.*L0212.*",
      treatments = ".*PRUEFGLIED.*|.*L0214.*",
      plots = ".*PARZELLE.*|.*L0213.*"
    ),
    munch = list(
      design = ".*VERSUCHSAUFBAU.*",
      treatments = ".*PRUEFGLIED.*",
      plots = ".*PARZELLE.*"
    ),
    rau = list(
      design = ".*VERSUCHSAUFBAU.*|.*L0212.*",
      treatments = ".*PRUEFGLIED.*|.*L0214.*",
      plots = ".*PARZELLE.*|.*L0213.*"
    )
  )
  
  # Get patterns for current dataset
  patterns <- file_patterns[[dataset_name]]
  if (is.null(patterns)) {
    stop(paste("No file patterns defined for dataset:", dataset_name))
  }
  
  # Check for required tables using dataset-specific patterns
  found_tables <- sapply(patterns, function(pattern) {
    any(sapply(names(db), function(x) grepl(pattern, x, ignore.case = TRUE)))
  })
  
  if (!all(found_tables)) {
    missing <- names(patterns)[!found_tables]
    stop(paste("Missing required tables:", paste(missing, collapse=", ")))
  }
  
  # Find design table
  design_pattern <- patterns$design
  design_table <- NULL
  design_table_name <- NULL
  for (tbl_name in names(db)) {
    if (grepl(design_pattern, tbl_name, ignore.case = TRUE)) {
      design_table <- db[[tbl_name]]
      design_table_name <- tbl_name
      break
    }
  }
  
  if (is.null(design_table)) {
    stop("Could not find design table")
  }
  
  # Print debug information
  cat("\nDesign table found:", design_table_name, "\n")
  cat("Design table columns:", paste(colnames(design_table), collapse=", "), "\n")
  
  # Check if required columns exist in the design table
  required_cols <- list(
    seehausen = list(
      original = c("Versuchsjahr", "Parzelle_ID", "Pruefglied_ID"),
      standardized = c("Year", "Plot_id", "Treatment_id")
    ),
    munch = list(
      original = c("Versuchsjahr", "Parzelle_ID", "Pruefglied_ID"),
      standardized = c("Year", "Plot_id", "Treatment_id")
    ),
    rau = list(
      original = c("Versuchsjahr", "Parzelle_ID", "Pruefglied_ID"),
      standardized = c("Year", "Plot_id", "Treatment_id")
    )
  )
  
  # Check for either original or standardized column names
  dataset_cols <- required_cols[[dataset_name]]
  if (is.null(dataset_cols)) {
    stop(paste("No required columns defined for dataset:", dataset_name))
  }
  
  # For each required column, check if either the original or standardized name exists
  missing_cols <- c()
  for (i in seq_along(dataset_cols$original)) {
    orig_col <- dataset_cols$original[i]
    std_col <- dataset_cols$standardized[i]
    if (!(orig_col %in% colnames(design_table) || std_col %in% colnames(design_table))) {
      missing_cols <- c(missing_cols, orig_col)
    }
  }
  
  if (length(missing_cols) > 0) {
    stop(paste("Missing required columns in design table:", paste(missing_cols, collapse=", ")))
  }
}

#' Process design tables to create FIELDS and TREATMENTS components
#' @param db List of data frames
#' @param tables List of categorized tables
#' @param config Dataset configuration
#' @return List containing FIELDS and TREATMENTS components
process_design_tables <- function(db, tables, config) {
  # Process FIELDS table
  FIELDS <- process_fields_table(db, tables, config)
  
  # Process TREATMENTS table
  TREATMENTS <- process_treatments_table(db, tables, config)
  
  return(list(
    FIELDS = FIELDS,
    TREATMENTS = TREATMENTS
  ))
}

#' Process FIELDS table
#' @param db List of data frames
#' @param tables List of categorized tables
#' @param config Dataset configuration
#' @return FIELDS data frame
process_fields_table <- function(db, tables, config) {
  # Create basic FIELDS structure
  FIELDS <- data.frame(
    FL_ID = 1,
    FL_NAME = config$site_info[[1]]$site,
    FL_LAT = config$site_info[[1]]$location$lat,
    FL_LON = config$site_info[[1]]$location$lon,
    SITE_COUNTRY = config$site_info[[1]]$country,
    stringsAsFactors = FALSE
  )
  
  # Get plots table if available
  plots_pattern <- switch(names(config$site_info)[1],
    "seehausen" = ".*PARZELLE.*|.*L0213.*",
    "munch" = ".*PARZELLE.*",
    "rau" = ".*PARZELLE.*|.*L0213.*"
  )
  
  plots_table <- NULL
  for (tbl_name in names(db)) {
    if (grepl(plots_pattern, tbl_name, ignore.case = TRUE)) {
      plots_table <- db[[tbl_name]]
      break
    }
  }
  
  # Add field-specific information if available
  if (!is.null(plots_table)) {
    field_cols <- grep("^field|^fl_|^standort", colnames(plots_table), 
                      ignore.case = TRUE, value = TRUE)
    if (length(field_cols) > 0) {
      field_data <- plots_table[1, field_cols, drop = FALSE]
      for (col in colnames(field_data)) {
        FIELDS[[toupper(col)]] <- field_data[[col]]
      }
    }
  }
  
  return(FIELDS)
}

#' Process TREATMENTS table
#' @param db List of data frames
#' @param tables List of categorized tables
#' @param config Dataset configuration
#' @return TREATMENTS data frame
process_treatments_table <- function(db, tables, config) {
  # Get treatments table pattern based on dataset
  treatments_pattern <- switch(names(config$site_info)[1],
    "seehausen" = ".*PRUEFGLIED.*|.*L0214.*",
    "munch" = ".*PRUEFGLIED.*",
    "rau" = ".*PRUEFGLIED.*|.*L0214.*"
  )
  
  # Find treatments table
  treatments_table <- NULL
  for (tbl_name in names(db)) {
    if (grepl(treatments_pattern, tbl_name, ignore.case = TRUE)) {
      treatments_table <- db[[tbl_name]]
      break
    }
  }
  
  if (!is.null(treatments_table)) {
    # Extract treatment information
    treatment_cols <- c(
      grep("^pruefglied|^treatment|^variante", colnames(treatments_table), 
           ignore.case = TRUE, value = TRUE),
      grep("^faktor|^factor", colnames(treatments_table), 
           ignore.case = TRUE, value = TRUE)
    )
    
    if (length(treatment_cols) > 0) {
      treatments <- unique(treatments_table[, treatment_cols, drop = FALSE])
      
      # Create TREATMENTS matrix
      TREATMENTS <- data.frame(
        TRTNO = seq_len(nrow(treatments)),
        TREATMENT_NAME = paste("Treatment", seq_len(nrow(treatments))),
        stringsAsFactors = FALSE
      )
      
      # Add treatment factors
      for (col in treatment_cols) {
        TREATMENTS[[toupper(col)]] <- treatments[[col]]
      }
    } else {
      # Default if no treatment columns found
      TREATMENTS <- data.frame(
        TRTNO = 1,
        TREATMENT_NAME = "Default Treatment",
        stringsAsFactors = FALSE
      )
    }
  } else {
    # Default if no treatments table found
    TREATMENTS <- data.frame(
      TRTNO = 1,
      TREATMENT_NAME = "Default Treatment",
      stringsAsFactors = FALSE
    )
  }
  
  return(TREATMENTS)
}

#' Process observation tables
#' @param observation_tables List of observation tables
#' @param config Dataset configuration
#' @return List containing summary and timeseries components
process_observation_tables <- function(observation_tables, config) {
  # Initialize result components
  summary_tables <- list()
  timeseries_tables <- list()
  
  # Process each observation table
  for (tbl_name in names(observation_tables)) {
    df <- observation_tables[[tbl_name]]
    
    # Standardize column names
    colnames(df) <- toupper(colnames(df))
    
    # Add ID if missing
    if (!"ID" %in% colnames(df)) {
      df$ID <- seq_len(nrow(df))
    }
    
    # Add table type
    table_type <- gsub("\\.csv$", "", tbl_name)
    table_type <- gsub("_", " ", table_type)
    df$TYPE <- toupper(table_type)
    
    # Check if it's a time series table
    has_date <- any(sapply(df, function(x) {
      inherits(x, "Date") || 
      inherits(x, "POSIXct") || 
      inherits(x, "POSIXlt") ||
      (is.character(x) && all(grepl("^\\d{4}-\\d{2}-\\d{2}", na.omit(x))))
    }))
    
    # Add to appropriate component
    if (has_date) {
      timeseries_tables[[tbl_name]] <- df
    } else {
      summary_tables[[tbl_name]] <- df
    }
  }
  
  return(list(
    summary = summary_tables,
    timeseries = timeseries_tables
  ))
}

#' Process management tables
#' @param management_tables List of management tables
#' @param config Dataset configuration
#' @return List of processed management tables
process_management_tables <- function(management_tables, config) {
  # Initialize result
  result <- list()
  
  # Process each management table
  for (tbl_name in names(management_tables)) {
    df <- management_tables[[tbl_name]]
    
    # Standardize column names
    colnames(df) <- toupper(colnames(df))
    
    # Add ID if missing
    if (!"ID" %in% colnames(df)) {
      df$ID <- seq_len(nrow(df))
    }
    
    # Add table type
    table_type <- gsub("\\.csv$", "", tbl_name)
    table_type <- gsub("_", " ", table_type)
    df$TYPE <- toupper(table_type)
    
    # Process dates if present
    date_cols <- sapply(df, function(x) {
      inherits(x, "Date") || 
      inherits(x, "POSIXct") || 
      inherits(x, "POSIXlt") ||
      (is.character(x) && all(grepl("^\\d{4}-\\d{2}-\\d{2}", na.omit(x))))
    })
    
    if (any(date_cols)) {
      for (col in names(df)[date_cols]) {
        if (!inherits(df[[col]], "Date")) {
          df[[col]] <- as.Date(df[[col]])
        }
      }
    }
    
    result[[tbl_name]] <- df
  }
  
  return(result)
}

#' Process metadata from the dataset
#' @param db List of data frames
#' @param tables List of categorized tables
#' @param config Dataset configuration
#' @return List containing GENERAL component
process_metadata <- function(db, tables, config) {
  # Get dataset name from config
  dataset_name <- names(config$site_info)[1]
  site_info <- config$site_info[[dataset_name]]
  
  # Create the GENERAL component
  GENERAL <- data.frame(
    PERSONS = "Dataset Contributors",
    EMAIL = "contact@example.com",
    ADDRESS = paste(site_info$site, site_info$country),
    SITE = site_info$site,
    COUNTRY = site_info$country,
    LATITUDE = site_info$location$lat,
    LONGITUDE = site_info$location$lon,
    REMARKS = "No additional remarks",  # Default value
    stringsAsFactors = FALSE
  )
  
  # Add experiment details if available
  if (!is.null(site_info$exp_details)) {
    GENERAL$EXPERIMENT_DETAILS <- site_info$exp_details
  }
  
  # Add version information
  if (!is.null(config$version)) {
    GENERAL$VERSION <- config$version
  }
  
  # Process remarks if available
  remarks_pattern <- ".*BEMERKUNGEN.*"
  remarks_files <- grep(remarks_pattern, names(db), value = TRUE)
  
  if (length(remarks_files) > 0) {
    all_remarks <- character(0)
    
    for (remarks_file in remarks_files) {
      remarks_df <- db[[remarks_file]]
      if (!is.null(remarks_df) && nrow(remarks_df) > 0) {
        # Try different possible column names
        possible_cols <- c("Bemerkung", "BEMERKUNG", "Bemerkungen", "BEMERKUNGEN", "Comment", "COMMENT")
        found_col <- intersect(possible_cols, colnames(remarks_df))
        
        if (length(found_col) > 0) {
          remarks <- na.omit(remarks_df[[found_col[1]]])
          if (length(remarks) > 0) {
            all_remarks <- c(all_remarks, remarks)
          }
        }
      }
    }
    
    if (length(all_remarks) > 0) {
      GENERAL$REMARKS <- paste(unique(all_remarks), collapse = "; ")
    }
  }
  
  return(GENERAL)
}

#' Add metadata attributes to the result
#' @param result List of processed components
#' @param config Dataset configuration
#' @return Result list with added metadata attributes
add_metadata_attributes <- function(result, config) {
  # Add experiment details
  attr(result, "EXP_DETAILS") <- config$site_info$exp_details
  
  # Create site code from first two letters of site name + country code
  site_code <- toupper(substr(config$site_info$site, 1, 2))
  country_code <- "DE" # Hardcoded for now, could be made dynamic
  attr(result, "SITE_CODE") <- paste0(site_code, country_code)
  
  # Add processing timestamp
  attr(result, "PROCESSED_DATE") <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  
  # Add dataset version if available
  if (!is.null(config$version)) {
    attr(result, "VERSION") <- config$version
  }
  
  return(result)
}

#' Process climate data
#' @param df Climate data frame
#' @return Processed climate data frame
process_climate_data <- function(df) {
  if (is.null(df)) return(NULL)
  
  # Standardize column names
  colnames(df) <- toupper(colnames(df))
  
  # Ensure date column is properly formatted
  if ("DATUM" %in% colnames(df)) {
    df$DATE <- as.Date(df$DATUM)
  }
  
  # Add metadata
  df$DATA_TYPE <- "CLIMATE"
  
  return(df)
}

#' Process crop assessment data (BONITUR)
#' @param df Crop assessment data frame
#' @return Processed assessment data frame
process_crop_assessment <- function(df) {
  if (is.null(df)) return(NULL)
  
  # Standardize column names
  colnames(df) <- toupper(colnames(df))
  
  # Add assessment type if available
  if ("BONITUR_TYP" %in% colnames(df)) {
    df$ASSESSMENT_TYPE <- df$BONITUR_TYP
  }
  
  # Add metadata
  df$DATA_TYPE <- "CROP_ASSESSMENT"
  
  return(df)
}

#' Process balance data (nitrogen and energy)
#' @param df Balance data frame
#' @param balance_type Type of balance ("NITROGEN" or "ENERGY")
#' @return Processed balance data frame
process_balance_data <- function(df, balance_type) {
  if (is.null(df)) return(NULL)
  
  # Standardize column names
  colnames(df) <- toupper(colnames(df))
  
  # Convert numeric columns
  numeric_cols <- sapply(df, function(x) {
    all(grepl("^[0-9.,-]+$", na.omit(x))) ||
    is.numeric(x)
  })
  
  for (col in names(df)[numeric_cols]) {
    df[[col]] <- as.numeric(gsub(",", ".", df[[col]]))
  }
  
  # Add balance type
  df$BALANCE_TYPE <- balance_type
  
  # Add metadata
  df$DATA_TYPE <- paste0(balance_type, "_BALANCE")
  
  # Add date if available
  if ("DATUM" %in% colnames(df)) {
    df$DATE <- as.Date(df$DATUM)
  } else if ("JAHR" %in% colnames(df)) {
    df$YEAR <- as.numeric(df$JAHR)
  }
  
  return(df)
}

#' Process crop rotation data
#' @param df Crop rotation data frame
#' @return Processed crop rotation data frame
process_crop_rotation <- function(df) {
  if (is.null(df)) return(NULL)
  
  # Standardize column names
  colnames(df) <- toupper(colnames(df))
  
  # Add metadata
  df$DATA_TYPE <- "CROP_ROTATION"
  
  return(df)
}
