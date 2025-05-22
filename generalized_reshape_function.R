# Generalized Reshape Script for Agricultural Experimental Datasets
# This script implements a comprehensive reshaping approach that works with multiple datasets

# Load required libraries
library(dplyr)
library(tidyr)
library(lubridate)
library(readxl)
library(stringr)
library(countrycode)

# ----- Essential Utility Functions -----

#' Get primary keys of a data frame
#' 
#' @param df a data frame
#' @param alternates whether to include alternate primary keys
#' 
#' @return a character vector of primary key column names
get_pkeys <- function(df, alternates = FALSE) {
  # Identify potential primary keys (ID columns)
  id_cols <- grep("ID$|_ID$|_id$", names(df), value = TRUE)
  
  # If no ID columns, look for columns ending with _no
  if (length(id_cols) == 0) {
    id_cols <- grep("_no$|_No$|_NO$", names(df), value = TRUE)
  }
  
  # If still no candidates, return the first column name
  if (length(id_cols) == 0) {
    return(names(df)[1])
  }
  
  return(id_cols)
}

#' Extract the name of a data frame from a list of data frames
#' 
#' @param ls a list of data frames containing the focal data frame
#' @param df a data frame; the focal table
#' 
#' @return a length 1 character vector corresponding to the name of the table in the original dataset
#' 
get_df_name <- function(ls, df) {
  names(ls)[sapply(ls, function(x) identical(x, df))]
}

#' Determine whether a vector is date data
#' 
#' @param x a vector
#' @param formats optional vector of date formats to try
#' 
#' @return a logical value indicating whether x is a date or not
#' 
is_date <- function(x, formats = c("Ymd", "mdY", "dmy", "ymd", "mdy")) {
  # Return FALSE if x is NA or NULL
  if (is.null(x) || all(is.na(x))) {
    return(FALSE)
  }
  
  # Convert all dates in DSSAT format %Y%j into usable formats
  if (is.numeric(x) & all(nchar(as.character(x)) == 5)) {
    x <- format(as.Date(as.character(x), format = "%y%j"), "%Y-%m-%d")
  }
  
  # If some values do not deparse (warning message), return NA (prevent parsing of 5 digits+ numericals)
  dates <- tryCatch(
    {
      lubridate::parse_date_time(x, orders = formats)
    },
    warning = function(w){
      return(NA)
    }
  )
  
  return(!all(is.na(dates)))
}

#' Find a column by matching against a list of candidate names
#'
#' @param df a data frame
#' @param candidates a character vector of possible column names
#'
#' @return the name of the first matching column, or NULL if no match is found
find_column_by_candidates <- function(df, candidates) {
  for (col in candidates) {
    if (col %in% colnames(df)) {
      return(col)
    }
  }
  return(NULL)
}

#' Check if a table has a link to another table through common columns
#'
#' @param df1 first data frame
#' @param df2 second data frame
#' @param subset optional subset of column names to check for linking
#'
#' @return logical indicating if the tables can be linked
has_link <- function(df1, df2, subset = NULL) {
  if(is.null(subset)) {
    common_cols <- intersect(names(df1), names(df2))
  } else {
    common_cols <- intersect(names(df1), subset)
  }
  
  return(length(common_cols) > 0)
}

#' Find parent table for a given table
#'
#' @param df child data frame
#' @param tables list of potential parent tables
#'
#' @return the parent table or NULL if no parent found
get_parent <- function(df, tables) {
  # Get primary keys of the child table
  child_keys <- get_pkeys(df)
  
  # Look for tables that have these keys as columns
  for(table_name in names(tables)) {
    parent_df <- tables[[table_name]]
    if(any(child_keys %in% colnames(parent_df))) {
      return(parent_df)
    }
  }
  
  return(NULL)
}

#' Merge tables based on their relationship type
#'
#' @param child_tables list of child tables
#' @param parent_tables list of parent tables
#' @param type type of relationship ("child-parent", "bidirectional")
#' @param drop_keys whether to drop key columns after merging
#'
#' @return list of merged tables
merge_tbls <- function(child_tables, parent_tables, type = "child-parent", drop_keys = TRUE) {
  if(type == "child-parent") {
    # For each child table, find its parent and merge
    result <- lapply(names(child_tables), function(child_name) {
      child_df <- child_tables[[child_name]]
      
      # Find keys that match with parent tables
      child_keys <- get_pkeys(child_df)
      
      for(parent_name in names(parent_tables)) {
        parent_df <- parent_tables[[parent_name]]
        
        # Check if parent has any of the child's keys as columns
        common_keys <- intersect(child_keys, colnames(parent_df))
        
        if(length(common_keys) > 0) {
          # Merge the tables
          merged_df <- left_join(child_df, parent_df, by = common_keys)
          
          # Drop the keys if specified
          if(drop_keys) {
            merged_df <- merged_df[!names(merged_df) %in% common_keys]
          }
          
          return(merged_df)
        }
      }
      
      # If no parent found, return the original child
      return(child_df)
    })
    
    names(result) <- names(child_tables)
    return(result)
  } 
  else if(type == "bidirectional") {
    # Similar to child-parent but try both directions
    result <- child_tables
    
    # Try merging each child with each parent
    for(child_name in names(child_tables)) {
      child_df <- child_tables[[child_name]]
      child_keys <- get_pkeys(child_df)
      
      for(parent_name in names(parent_tables)) {
        parent_df <- parent_tables[[parent_name]]
        
        # Check both ways
        common_keys_cp <- intersect(child_keys, colnames(parent_df))
        common_keys_pc <- intersect(get_pkeys(parent_df), colnames(child_df))
        
        if(length(common_keys_cp) > 0) {
          # Child -> Parent merge
          result[[child_name]] <- left_join(child_df, parent_df, by = common_keys_cp)
          if(drop_keys) {
            result[[child_name]] <- result[[child_name]][!names(result[[child_name]]) %in% common_keys_cp]
          }
        } 
        else if(length(common_keys_pc) > 0) {
          # Parent -> Child merge
          result[[child_name]] <- left_join(parent_df, child_df, by = common_keys_pc)
          if(drop_keys) {
            result[[child_name]] <- result[[child_name]][!names(result[[child_name]]) %in% common_keys_pc]
          }
        }
      }
    }
    
    return(result)
  }
  
  return(child_tables)  # Default: return unmodified
}

#' Determine if a table represents treatment data
#'
#' @param df data frame to check
#' @param year_col name of the year column
#' @param plot_col name of the plot column
#'
#' @return logical vector indicating if each row is treatment-related
is_treatment <- function(df, year_col, plot_col) {
  # Check if both year and plot columns exist
  if(!(year_col %in% colnames(df) && plot_col %in% colnames(df))) {
    return(rep(FALSE, nrow(df)))
  }
  
  # Group by year and count unique plots
  plot_counts <- df %>%
    group_by(across(all_of(year_col))) %>%
    summarize(plot_count = n_distinct(!!sym(plot_col))) %>%
    ungroup()
  
  # Determine if the table represents treatments
  # (if the number of unique plots per year is consistent)
  is_consistent <- length(unique(plot_counts$plot_count)) == 1
  
  return(rep(is_consistent, nrow(df)))
}

# ----- Dataset Configuration and Loading Functions -----

#' Load experimental dataset based on configuration
#' 
#' @param config dataset configuration with path and pattern information
#' 
#' @return a list of data frames representing the dataset tables
load_dataset <- function(config) {
  cat(paste0("Loading ", config$name, " dataset...\n"))
  
  # Get all CSV files
  files <- list.files(path = config$path, pattern = "\\.csv$", full.names = TRUE)
  
  # Show what files we're loading
  cat("Found", length(files), "CSV files:\n")
  for(file in files) {
    cat("  ", basename(file), "\n")
  }
  
  # Load the data frames
  db <- lapply(files, function(x) { 
    tryCatch(read.csv(x, fileEncoding = "iso-8859-1"), 
             error = function(e) {
               cat("Error reading", x, ":", e$message, "\n")
               return(NULL)
             })
  })
  db <- db[!sapply(db, is.null)]
  
  # Extract table names using the provided pattern
  names(db) <- basename(files)
  if (!is.null(config$name_pattern)) {
    names(db) <- gsub(config$name_pattern, "\\1", names(db))
  }
  
  cat("Loaded", length(db), "tables with names:\n")
  for(name in names(db)) {
    cat("  ", name, " (", nrow(db[[name]]), " rows, ", ncol(db[[name]]), " columns)\n", sep="")
  }
  
  # Standardize column names
  year_col <- config$col_mapping$year
  plot_col <- config$col_mapping$plot
  rep_col <- config$col_mapping$rep
  
  for (i in seq_along(db)) {
    # Year columns
    for (old_name in config$year_variants) {
      if (old_name %in% colnames(db[[i]])) {
        colnames(db[[i]])[colnames(db[[i]]) == old_name] <- year_col
        cat("  Renamed", old_name, "to", year_col, "in", names(db)[i], "\n")
      }
    }
    
    # Plot columns
    for (old_name in config$plot_variants) {
      if (old_name %in% colnames(db[[i]])) {
        colnames(db[[i]])[colnames(db[[i]]) == old_name] <- plot_col
        cat("  Renamed", old_name, "to", plot_col, "in", names(db)[i], "\n")
      }
    }
    
    # Replication columns
    for (old_name in config$rep_variants) {
      if (old_name %in% colnames(db[[i]])) {
        colnames(db[[i]])[colnames(db[[i]]) == old_name] <- rep_col
        cat("  Renamed", old_name, "to", rep_col, "in", names(db)[i], "\n")
      }
    }
    
    # Fix duplicate column names if any
    dup_cols <- which(duplicated(colnames(db[[i]])))
    if(length(dup_cols) > 0) {
      cat("  Found duplicate column names in", names(db)[i], ", fixing...\n")
      for(j in dup_cols) {
        colnames(db[[i]])[j] <- paste0(colnames(db[[i]])[j], "_dup")
      }
    }
  }
  
  return(db)
}

#' Create dataset configuration for specific datasets
#' 
#' @param dataset_name name of the dataset ("seehausen", "munch", or "rau")
#' 
#' @return a list with dataset configuration
create_dataset_config <- function(dataset_name) {
  if(tolower(dataset_name) == "seehausen") {
    return(list(
      name = "Seehausen",
      path = "./inst/extdata/lte_seehausen/0_raw",
      name_pattern = "lte_see.V[0-9]+_[0-9]+_(.+)\\.csv",
      col_mapping = list(
        year = "Year",
        plot = "Plot_id",
        rep = "Rep_no"
      ),
      year_variants = c("Versuchsjahr", "JAHR", "ERNTEJAHR"),
      plot_variants = c("Parzelle_ID", "PARZID", "ï..Parzelle_ID"),
      rep_variants = c("Wiederholung", "WDHLG", "REPLIKAT"),
      plots_len = 64,
      max_events = 8,
      site_info = list(
        person = "Test Person",
        email = "test@example.com",
        address = "Test Address",
        site = "Seehausen",
        country = "Germany",
        exp_details = "LTE Seehausen, Duengungs-Kombinationsversuch Seehausen (F1-70)",
        site_code = "SEDE",
        location = list(
          lat = 51.401884,  # Replace with actual lat
          lon = 12.418934   # Replace with actual lon
        )
      ),
      table_patterns = list(
        ertrag = "ERTRAG",
        ernte = "ERNTE",
        bodenlabor = "BODENLABORWERTE",
        boden = "BODEN",
        probenahme_boden = "PROBENAHME_BODEN",
        pflanzenlabor = "PFLANZENLABORWERTE",
        pflanze = "PFLANZE",
        probenahme_pflanzen = "PROBENAHME_PFLANZEN",
        duengung = "DUENGUNG",
        aussaat = "AUSSAAT",
        beregnung = "BEREGNUNG",
        bodenbearbeitung = "BODENBEARBEITUNG",
        pflanzenschutz = "PFLANZENSCHUTZ",
        versuchsaufbau = "VERSUCHSAUFBAU",
        parzelle = "PARZELLE"
      ),
      link_keys = list(
        ertrag_ernte = "Ernte_ID",
        bodenlabor_probenahme = "Probenahme_Boden_ID",
        pflanzenlabor_probenahme = "Probenahme_Pflanzen_ID"
      ),
      treatment_candidates = c("Pruefglied_ID", "Treatment_id", "Pruefglied", "Variante", "Var", "Treatment")
    ))
  } else if(tolower(dataset_name) == "munch") {
    return(list(
      name = "Muencheberg",
      path = "./inst/extdata/lte_Munch/0_raw",
      name_pattern = "v140_mun.V2_0_(.+)\\.csv",
      col_mapping = list(
        year = "Year",
        plot = "Plot_id",
        rep = "Rep_no"
      ),
      year_variants = c("Versuchsjahr", "JAHR", "ERNTEJAHR"),
      plot_variants = c("Parzelle_ID", "Parzelle", "PARZID", "ï..Parzelle_ID"),
      rep_variants = c("Wiederholung", "WDHLG", "REPLIKAT"),
      plots_len = 168,  # Replace with actual plot count
      max_events = 8,
      site_info = list(
        person = "Test Person",
        email = "test@example.com",
        address = "Test Address",
        site = "Muencheberg",
        country = "Germany",
        exp_details = "LTE Muencheberg, Static Fertilization Experiment V140",
        site_code = "MUDE",
        location = list(
          lat = 52.515,  # Replace with actual lat
          lon = 14.122   # Replace with actual lon
        )
      ),
      table_patterns = list(
        ertrag = "ERTRAG",
        ernte = "ERNTE",
        bodenlabor = "BODENLABORWERTE",
        boden = "BODEN",
        probenahme_boden = "PROBENAHME_BODEN",
        pflanzenlabor = "PFLANZENLABORWERTE",
        pflanze = "PFLANZE",
        probenahme_pflanzen = "PROBENAHME_PFLANZEN",
        duengung = "DUENGUNG",
        aussaat = "AUSSAAT",
        beregnung = "BEREGNUNG",
        bodenbearbeitung = "BODENBEARBEITUNG",
        pflanzenschutz = "PFLANZENSCHUTZ",
        versuchsaufbau = "VERSUCHSAUFBAU",
        parzelle = "PARZELLE"
      ),
      link_keys = list(
        ertrag_ernte = "Ernte_ID",
        bodenlabor_probenahme = "Probenahme_Boden_ID",
        pflanzenlabor_probenahme = "Probenahme_Pflanzen_ID"
      ),
      treatment_candidates = c("Pruefglied_ID", "Treatment_id", "Pruefglied", "Variante", "Var", "Treatment")
    ))
  } else if(tolower(dataset_name) == "rau") {
    return(list(
      name = "Rauischholzhausen",
      path = "./inst/extdata/rau",
      name_pattern = "ltfe_rauischholzhausen.ID_L[0-9]+_V[0-9]+_[0-9]+_(.+)\\.csv",
      col_mapping = list(
        year = "Year",
        plot = "Plot_id",
        rep = "Rep_no"
      ),
      year_variants = c("Versuchsjahr", "JAHR", "ERNTEJAHR", "Jahr"),
      plot_variants = c("Parzelle_ID", "Parzelle", "PARZID", "ï..Parzelle_ID"),
      rep_variants = c("Wiederholung", "WDHLG", "REPLIKAT"),
      plots_len = 225,  # Based on the parzelle table which has 225 plots
      max_events = 8,
      site_info = list(
        person = "Test Person",
        email = "test@example.com",
        address = "Test Address",
        site = "Rauischholzhausen",
        country = "Germany",
        exp_details = "LTE Rauischholzhausen, Long-term fertilization experiment",
        site_code = "RAUD",
        location = list(
          lat = 50.6747,  # Approximate coordinates for Rauischholzhausen
          lon = 8.8809
        )
      ),
      table_patterns = list(
        ertrag = "ERTRAG",
        ernte = "ERNTE",
        bodenlabor = "BODENLABORWERTE",
        boden = "BODEN",
        probenahme_boden = "PROBENAHME_BODEN",
        pflanzenlabor = "PFLANZENLABORWERTE",
        pflanze = "PFLANZE",
        probenahme_pflanzen = "PROBENAHME_PFLANZEN",
        duengung = "DUENGUNG",
        aussaat = "AUSSAAT",
        beregnung = "BEREGNUNG",
        bodenbearbeitung = "BODENBEARBEITUNG",
        pflanzenschutz = "PFLANZENSCHUTZ",
        versuchsaufbau = "VERSUCHSAUFBAU",
        parzelle = "PARZELLE",
        pruefglied = "PRUEFGLIED",
        faktor = "FAKTOR",
        klimadaten = "KLIMADATEN"
      ),
      link_keys = list(
        pflanzenlabor_probenahme = "Probenahme_Pflanzen_ID", 
        bodenlabor_probenahme = "Probenahme_Boden_ID",
        ertrag_ernte = "Ernte_ID"
      ),
      treatment_candidates = c("Pruefglied_ID", "Pruefglied", "Variante", "Var", "Treatment")
    ))
  } else {
    warning(paste0("Unknown dataset: ", dataset_name, ". Using Seehausen configuration."))
    return(create_dataset_config("seehausen"))
  }
}

#' Tag data tables by type based on naming patterns and content
#' 
#' @param db a list of data frames
#' @param config a dataset configuration
#' 
#' @return a list with categorized tables (management, observed_summary, observed_timeseries, other)
#'
tag_data_type <- function(db, config) {
  # Initialize categories
  management <- list()
  observed_summary <- list()
  observed_timeseries <- list()
  other <- list()
  
  is_rau_dataset <- grepl("rauischholzhausen", config$name, ignore.case = TRUE)
  is_munch_dataset <- grepl("muencheberg", config$name, ignore.case = TRUE)
  
  for (df_name in names(db)) {
    df <- db[[df_name]]
    cat("  Processing table:", df_name, "\n")
    
    # Check if key columns exist
    has_year <- config$col_mapping$year %in% colnames(df)
    has_plot <- config$col_mapping$plot %in% colnames(df)
    
    # For Munch dataset, handle specific column naming
    if (is_munch_dataset) {
      # For Munch, special handling for BODENLABORWERTE and PROBENAHME_BODEN
      if (grepl(config$table_patterns$bodenlabor, df_name, ignore.case = TRUE)) {
        cat("    Soil data table detected\n")
        
        # Find the corresponding probenahme table
        probe_table_name <- NULL
        for (name in names(db)) {
          if (grepl(config$table_patterns$probenahme_boden, name, ignore.case = TRUE)) {
            probe_table_name <- name
            break
          }
        }
        
        if (!is.null(probe_table_name)) {
          cat("    Linking soil lab values with soil sampling\n")
          cat("    Found soil sampling table, linking...\n")
          probe_table <- db[[probe_table_name]]
          
          # Check what linking columns are available
          if ("Probenahme_Boden_ID" %in% colnames(df) && "Probenahme_Boden_ID" %in% colnames(probe_table)) {
            joined_df <- df %>%
              left_join(probe_table, by = "Probenahme_Boden_ID")
            
            if (config$col_mapping$year %in% colnames(joined_df) && config$col_mapping$plot %in% colnames(joined_df)) {
              observed_summary[[df_name]] <- joined_df
            } else {
              other[[df_name]] <- joined_df
            }
            next
          }
        } else {
          # If no linking possible but has year and plot columns
          if (has_year && has_plot) {
            cat("    Categorizing as 'observed_summary'\n")
            observed_summary[[df_name]] <- df
          } else {
            cat("    Missing year or plot column, categorizing as 'other'\n")
            other[[df_name]] <- df
          }
          next
        }
      }
      
      # For Munch, special handling for PFLANZENLABORWERTE and PROBENAHME_PFLANZEN
      if (grepl(config$table_patterns$pflanzenlabor, df_name, ignore.case = TRUE)) {
        cat("    Plant data table detected\n")
        
        # Find the corresponding probenahme table
        probe_table_name <- NULL
        for (name in names(db)) {
          if (grepl(config$table_patterns$probenahme_pflanzen, name, ignore.case = TRUE)) {
            probe_table_name <- name
            break
          }
        }
        
        if (!is.null(probe_table_name)) {
          cat("    Linking plant lab values with plant sampling\n")
          cat("    Found plant sampling table, linking...\n")
          probe_table <- db[[probe_table_name]]
          
          # Check what linking columns are available
          if ("Probenahme_Pflanzen_ID" %in% colnames(df) && "Probenahme_Pflanzen_ID" %in% colnames(probe_table)) {
            joined_df <- df %>%
              left_join(probe_table, by = "Probenahme_Pflanzen_ID")
            
            if (config$col_mapping$year %in% colnames(joined_df) && config$col_mapping$plot %in% colnames(joined_df)) {
              observed_summary[[df_name]] <- joined_df
        } else {
              other[[df_name]] <- joined_df
            }
            next
          }
        } else {
          # If no linking possible but has year and plot columns
          if (has_year && has_plot) {
            cat("    Categorizing as 'observed_summary'\n")
            observed_summary[[df_name]] <- df
          } else {
            cat("    Missing year or plot column, categorizing as 'other'\n")
          other[[df_name]] <- df
          }
          next
        }
      }
      
      # For yield data
      if (grepl(config$table_patterns$ertrag, df_name, ignore.case = TRUE)) {
        cat("    Ertrag table detected, categorizing as 'observed_summary'\n")
        
        # Check if the table has a link to the harvest table
        ernte_table_name <- NULL
        for (name in names(db)) {
          if (grepl(config$table_patterns$ernte, name, ignore.case = TRUE)) {
            ernte_table_name <- name
            break
          }
        }
        
        # If an ernte table exists and ertrag doesn't have Year/Plot_id columns but has Ernte_ID
        if (!is.null(ernte_table_name) && !has_year && !has_plot && "Ernte_ID" %in% colnames(df)) {
          cat("    Linking ERTRAG with ERNTE to get Year and Plot_id\n")
          cat("    Found ERNTE table, linking...\n")
          ernte_table <- db[[ernte_table_name]]
          joined_df <- df %>%
            left_join(ernte_table, by = "Ernte_ID")
          observed_summary[[df_name]] <- joined_df
      } else {
        observed_summary[[df_name]] <- df
        }
        next
      }
      
    } 
    # If it's a Rau dataset, handle special column naming for plant and soil lab data
    else if (is_rau_dataset) {
      # For Rau dataset, adjust the linking keys to match the actual column structure
      # For plant data
      if (grepl(config$table_patterns$pflanzenlabor, df_name, ignore.case = TRUE)) {
        cat("    Plant data table detected\n")
        pflanzenlabor_probenahme_col <- "Probenahme_Pflanzen_ID"
        
        # Find the corresponding probenahme table
        probe_table_name <- NULL
        for (name in names(db)) {
          if (grepl(config$table_patterns$probenahme_pflanzen, name, ignore.case = TRUE)) {
            probe_table_name <- name
            break
          }
        }
        
        if (!is.null(probe_table_name)) {
          cat("    Found plant sampling table, linking...\n")
          probe_table <- db[[probe_table_name]]
          
          # Check what keys are available in both tables
          if (pflanzenlabor_probenahme_col %in% colnames(df) && pflanzenlabor_probenahme_col %in% colnames(probe_table)) {
            joined_df <- df %>%
              left_join(probe_table, by = c("Probenahme_Pflanzen_ID" = "Probenahme_Pflanzen_ID"))
            
            if (has_year && has_plot) {
              observed_summary[[df_name]] <- joined_df
            } else {
              other[[df_name]] <- joined_df
            }
            next
          } else {
            cat("    Cannot link tables - missing required column\n")
            # If no linking possible but has year and plot columns
            if (has_year && has_plot) {
              cat("    Categorizing as 'observed_summary'\n")
              observed_summary[[df_name]] <- df
            } else {
              cat("    Missing year or plot column, categorizing as 'other'\n")
              other[[df_name]] <- df
            }
            next
          }
        }
      }
      
      # For soil data
      if (grepl(config$table_patterns$bodenlabor, df_name, ignore.case = TRUE)) {
        cat("    Soil data table detected\n")
        bodenlabor_probenahme_col <- "Probenahme_Boden_ID"
        
        # Find the corresponding probenahme table
        probe_table_name <- NULL
        for (name in names(db)) {
          if (grepl(config$table_patterns$probenahme_boden, name, ignore.case = TRUE)) {
            probe_table_name <- name
            break
          }
        }
        
        if (!is.null(probe_table_name)) {
          cat("    Found soil sampling table, linking...\n")
          probe_table <- db[[probe_table_name]]
          
          # Check what keys are available in both tables
          if (bodenlabor_probenahme_col %in% colnames(df) && bodenlabor_probenahme_col %in% colnames(probe_table)) {
            joined_df <- df %>%
              left_join(probe_table, by = c("Probenahme_Boden_ID" = "Probenahme_Boden_ID"))
            
            if (has_year && has_plot) {
              observed_summary[[df_name]] <- joined_df
            } else {
              other[[df_name]] <- joined_df
            }
            next
          } else {
            cat("    Cannot link tables - missing required column\n")
            # If no linking possible but has year and plot columns
            if (has_year && has_plot) {
              cat("    Categorizing as 'observed_summary'\n")
              observed_summary[[df_name]] <- df
            } else {
              cat("    Missing year or plot column, categorizing as 'other'\n")
              other[[df_name]] <- df
            }
            next
          }
        }
      }
      
      # For yield data
      if (grepl(config$table_patterns$ertrag, df_name, ignore.case = TRUE)) {
        cat("    Ertrag table detected, categorizing as 'observed_summary'\n")
        observed_summary[[df_name]] <- df
        next
      }
    } else {
      # For other datasets (Seehausen), keep the original logic
      
      # Check for plant lab data
      if (grepl(config$table_patterns$pflanzenlabor, df_name, ignore.case = TRUE)) {
      cat("    Plant data table detected\n")
        pflanzenlabor_probenahme_col <- config$link_keys$pflanzenlabor_probenahme
        
        # Find the corresponding probenahme table
        probe_table_name <- NULL
        for (name in names(db)) {
          if (grepl(config$table_patterns$probenahme_pflanzen, name, ignore.case = TRUE)) {
            probe_table_name <- name
            break
          }
        }
        
        if (!is.null(probe_table_name)) {
        cat("    Linking plant lab values with plant sampling\n")
          cat("    Found plant sampling table, linking...\n")
          probe_table <- db[[probe_table_name]]
          
          # Join the tables to get Year and Plot_id columns
          if (pflanzenlabor_probenahme_col %in% colnames(df)) {
            joined_df <- df %>%
              left_join(probe_table, by = setNames("Probenahme_Pflanzen_ID", pflanzenlabor_probenahme_col))
            
            if (config$col_mapping$year %in% colnames(joined_df) && config$col_mapping$plot %in% colnames(joined_df)) {
              observed_summary[[df_name]] <- joined_df
            } else {
              other[[df_name]] <- joined_df
            }
            next
          }
        }
      }
      
      # Check for soil lab data
      if (grepl(config$table_patterns$bodenlabor, df_name, ignore.case = TRUE)) {
        cat("    Soil data table detected\n")
        bodenlabor_probenahme_col <- config$link_keys$bodenlabor_probenahme
        
        # Find the corresponding probenahme table
        probe_table_name <- NULL
        for (name in names(db)) {
          if (grepl(config$table_patterns$probenahme_boden, name, ignore.case = TRUE)) {
            probe_table_name <- name
            break
          }
        }
        
        if (!is.null(probe_table_name)) {
          cat("    Linking soil lab values with soil sampling\n")
          cat("    Found soil sampling table, linking...\n")
          probe_table <- db[[probe_table_name]]
          
          # Join the tables to get Year and Plot_id columns
          if (bodenlabor_probenahme_col %in% colnames(df)) {
            joined_df <- df %>%
              left_join(probe_table, by = setNames("Probenahme_Boden_ID", bodenlabor_probenahme_col))
            
            if (config$col_mapping$year %in% colnames(joined_df) && config$col_mapping$plot %in% colnames(joined_df)) {
              observed_summary[[df_name]] <- joined_df
        } else {
              other[[df_name]] <- joined_df
            }
            next
        }
      } 
      }
      
      # Check for yield data
      if (grepl(config$table_patterns$ertrag, df_name, ignore.case = TRUE)) {
        cat("    Ertrag table detected, categorizing as 'observed_summary'\n")
        
        # Check if the table has a link to the harvest table
        ernte_table_name <- NULL
        for (name in names(db)) {
          if (grepl(config$table_patterns$ernte, name, ignore.case = TRUE)) {
            ernte_table_name <- name
            break
      }
    }
        
        # If an ernte table exists and ertrag doesn't have Year/Plot_id columns but has Ernte_ID
        if (!is.null(ernte_table_name) && !has_year && !has_plot && "Ernte_ID" %in% colnames(df)) {
          cat("    Linking ERTRAG with ERNTE to get Year and Plot_id\n")
          cat("    Found ERNTE table, linking...\n")
          ernte_table <- db[[ernte_table_name]]
          joined_df <- df %>%
            left_join(ernte_table, by = "Ernte_ID")
          observed_summary[[df_name]] <- joined_df
        } else {
          observed_summary[[df_name]] <- df
        }
        next
      }
    }
    
    # For all datasets: categorize tables
    
    # Check management activities
    is_management <- FALSE
    
    # Use patterns to identify specific management tables
    if (any(sapply(c(config$table_patterns$duengung, "fertilization"), 
                 function(pattern) grepl(pattern, df_name, ignore.case = TRUE)))) {
      cat("    Fertilization table detected, categorizing as 'management'\n")
      is_management <- TRUE
    } else if (any(sapply(c(config$table_patterns$aussaat, "planting", "sowing"), 
                        function(pattern) grepl(pattern, df_name, ignore.case = TRUE)))) {
      cat("    Planting table detected, categorizing as 'management'\n")
      is_management <- TRUE
    } else if (any(sapply(c(config$table_patterns$ernte, "harvest"), 
                        function(pattern) grepl(pattern, df_name, ignore.case = TRUE)))) {
      cat("    Harvest table detected, categorizing as 'management'\n")
      is_management <- TRUE
    } else if (any(sapply(c(config$table_patterns$bodenbearbeitung, "tillage"), 
                        function(pattern) grepl(pattern, df_name, ignore.case = TRUE)))) {
      cat("    Soil tillage table detected, categorizing as 'management'\n")
      is_management <- TRUE
    } else if (any(sapply(c(config$table_patterns$pflanzenschutz, "pesticide", "protection"), 
                        function(pattern) grepl(pattern, df_name, ignore.case = TRUE)))) {
      cat("    Plant protection table detected, categorizing as 'management'\n")
      is_management <- TRUE
    } else if (any(sapply(c(config$table_patterns$versuchsaufbau, "design", "experimental"), 
                        function(pattern) grepl(pattern, df_name, ignore.case = TRUE)))) {
      cat("    Experimental design table detected, categorizing as 'management'\n")
      is_management <- TRUE
    } else if (any(sapply(c(config$table_patterns$parzelle, "plot"), 
                        function(pattern) grepl(pattern, df_name, ignore.case = TRUE)))) {
      cat("    Plot table detected, categorizing as 'other'\n")
      other[[df_name]] <- df
      next
    } else if (grepl("boden|soil", df_name, ignore.case = TRUE)) {
      cat("    Soil data table detected\n")
      if (has_year && has_plot) {
        cat("    Categorizing as 'observed_summary'\n")
        observed_summary[[df_name]] <- df
      } else {
        cat("    Missing year or plot column, categorizing as 'other'\n")
        other[[df_name]] <- df
      }
      next
    } else if (grepl("pflanze|plant", df_name, ignore.case = TRUE)) {
      cat("    Plant data table detected\n")
      if (has_year && has_plot) {
        cat("    Categorizing as 'observed_summary'\n")
        observed_summary[[df_name]] <- df
      } else {
        cat("    Missing year or plot column, categorizing as 'other'\n")
        other[[df_name]] <- df
      }
        next
    } else {
      cat("    Default categorization: 'management'\n")
      is_management <- TRUE
      }
      
    # Assign to appropriate category
    if (is_management) {
      if (has_year && has_plot) {
      management[[df_name]] <- df
      } else {
        cat("    No plot column found, categorizing as 'other'\n")
        other[[df_name]] <- df
      }
    } else {
      # If nothing matched, put in other
      cat("    No specific pattern matched, categorizing as 'other'\n")
      other[[df_name]] <- df
    }
  }
  
  cat("Categorization complete. Found:\n")
  cat("  Management tables:", length(management), "\n")
  cat("  Observed summary tables:", length(observed_summary), "\n")
  cat("  Observed timeseries tables:", length(observed_timeseries), "\n")
  cat("  Other tables:", length(other), "\n\n")
  
  return(list(
    management = management,
    observed_summary = observed_summary,
    observed_timeseries = observed_timeseries,
    other = other
  ))
}

#' Process an experimental dataset
#'
#' @param config dataset configuration
#'
#' @return a list with the reshaped dataset
process_dataset <- function(config) {
  # Load the dataset
  db <- load_dataset(config)
  
  # Define common configuration variables
  year_col <- config$col_mapping$year
  plot_col <- config$col_mapping$plot
  rep_col <- config$col_mapping$rep
  
  # Tag data by type
  data_tbls_ident <- tag_data_type(db, config)
  
  # Get categorized tables
  management_tables <- data_tbls_ident$management
  observed_summary_tables <- data_tbls_ident$observed_summary
  observed_timeseries_tables <- data_tbls_ident$observed_timeseries
  other_tables <- data_tbls_ident$other
  
  # ----- Create Fields Table -----
  
  # Get the plot table
  plot_table_name <- NULL
  for(tbl_name in names(other_tables)) {
    if(grepl(config$table_patterns$parzelle, tbl_name, ignore.case = TRUE)) {
      plot_table_name <- tbl_name
      break
    }
  }
  
  if(!is.null(plot_table_name)) {
    plot_table <- other_tables[[plot_table_name]]
    
    # Define FIELDS ID
    plot_keys <- get_pkeys(plot_table, alternates = TRUE)
    fields_cols <- setdiff(colnames(plot_table), c(plot_keys, year_col, plot_col, rep_col))
    
    # If we have location columns, create fields table
    if(any(grepl("Latitude|Longitude|lat|long", fields_cols, ignore.case = TRUE))) {
      cat("Creating FIELDS table from plot information\n")
      
      # Make field table
      FIELDS_tbl <- plot_table %>%
        mutate(FL_ID = row_number()) %>%
        relocate(FL_ID, .before = everything())
      
      FIELDS <- FIELDS_tbl %>%
        select(FL_ID, all_of(fields_cols)) %>%
        distinct()
      
      # Standardize column names if possible
      for(col in colnames(FIELDS)) {
        if(grepl("Latitude|lat", col, ignore.case = TRUE)) {
          colnames(FIELDS)[colnames(FIELDS) == col] <- "FL_LAT"
        } else if(grepl("Longitude|long", col, ignore.case = TRUE)) {
          colnames(FIELDS)[colnames(FIELDS) == col] <- "FL_LON"
        }
      }
    } else {
      # Create a minimal fields table with default coordinates
      cat("Creating minimal FIELDS table\n")
      FIELDS <- data.frame(
        FL_ID = 1,
        FL_LAT = config$site_info$location$lat,
        FL_LON = config$site_info$location$lon
      )
      
      FIELDS_tbl <- plot_table %>%
        mutate(FL_ID = 1)
    }
  } else {
    # If we don't have a plot table, create a minimal one
    cat("No plot table found, creating minimal FIELDS table\n")
    FIELDS <- data.frame(
      FL_ID = 1,
      FL_LAT = config$site_info$location$lat,
      FL_LON = config$site_info$location$lon
    )
    
    # Find all unique plots across the dataset
    all_plots <- unique(unlist(lapply(c(management_tables, observed_summary_tables), 
                                     function(df) if(plot_col %in% colnames(df)) df[[plot_col]] else NULL)))
    
    FIELDS_tbl <- data.frame(
      Plot_id = all_plots,
      FL_ID = 1
    )
  }
  
  # ----- Process Management Tables -----
  
  # Try to find the design/treatment table
  design_table_name <- NULL
  for(tbl_name in names(management_tables)) {
    if(any(sapply(c(config$table_patterns$versuchsaufbau), 
                 function(pattern) grepl(pattern, tbl_name, ignore.case = TRUE)))) {
      design_table_name <- tbl_name
      break
    }
  }
  
  if(!is.null(design_table_name)) {
    design_table <- management_tables[[design_table_name]]
    
    # Identify treatment information
    if(!is.null(config$treatment_candidates)) {
      # For Munch-like datasets
      treatment_col <- find_column_by_candidates(design_table, config$treatment_candidates)
      
      if(!is.null(treatment_col)) {
        cat("Found treatment information in", design_table_name, "using column", treatment_col, "\n")
        
        # Create treatments matrix
        TREATMENTS <- design_table %>%
          select(all_of(c(year_col, plot_col, treatment_col))) %>%
          left_join(FIELDS_tbl %>% select(Plot_id, FL_ID), by = "Plot_id") %>%
          group_by(across(all_of(treatment_col))) %>%
          mutate(TRTNO = cur_group_id()) %>%
          ungroup() %>%
          relocate(TRTNO, .before = everything()) %>%
          distinct()
      } else {
        # If no treatment column found, use replicate info if available
        if(rep_col %in% colnames(design_table)) {
          cat("No treatment column found, using replicate information from", design_table_name, "\n")
          
          # Create treatments based on replicates
          TREATMENTS <- design_table %>%
            select(all_of(c(year_col, plot_col, rep_col))) %>%
            left_join(FIELDS_tbl %>% select(Plot_id, FL_ID), by = "Plot_id") %>%
            mutate(TRTNO = as.numeric(as.factor(paste(Year, Rep_no)))) %>%
            relocate(TRTNO, .before = everything()) %>%
            distinct()
        } else {
          # Create a simple treatment matrix based on plots
          cat("No treatment or replicate information found, creating simple treatment matrix\n")
          TREATMENTS <- data.frame(
            TRTNO = 1:length(unique(design_table[[plot_col]])),
            Plot_id = unique(design_table[[plot_col]]),
            Year = rep(min(design_table[[year_col]]), length(unique(design_table[[plot_col]])))
          ) %>%
            left_join(FIELDS_tbl %>% select(Plot_id, FL_ID), by = "Plot_id")
        }
      }
    } else if("Pruefglied_ID" %in% colnames(design_table)) {
      # For Seehausen-like datasets
      cat("Found treatment information in", design_table_name, "\n")
      
      # Create treatments matrix
      TREATMENTS <- design_table %>%
        select(Year, Plot_id, Pruefglied_ID) %>%
        left_join(FIELDS_tbl %>% select(Plot_id, FL_ID), by = "Plot_id") %>%
        group_by(Pruefglied_ID) %>%
        mutate(TRTNO = cur_group_id()) %>%
        ungroup() %>%
        relocate(TRTNO, .before = everything()) %>%
        distinct()
    } else {
      # Create a simple treatment matrix based on plots
      cat("No treatment information found, creating simple treatment matrix\n")
      TREATMENTS <- data.frame(
        TRTNO = 1:length(unique(design_table[[plot_col]])),
        Plot_id = unique(design_table[[plot_col]]),
        Year = rep(min(design_table[[year_col]]), length(unique(design_table[[plot_col]])))
      ) %>%
        left_join(FIELDS_tbl %>% select(Plot_id, FL_ID), by = "Plot_id")
    }
  } else {
    # Create a minimal treatment matrix
    cat("No design table found, creating minimal treatment matrix\n")
    
    # Try to find a table with replicate information
    rep_table <- NULL
    for(tbl_list in list(management_tables, observed_summary_tables)) {
      for(tbl_name in names(tbl_list)) {
        if(rep_col %in% colnames(tbl_list[[tbl_name]])) {
          rep_table <- tbl_list[[tbl_name]]
          break
        }
      }
      if(!is.null(rep_table)) break
    }
    
    if(!is.null(rep_table)) {
      cat("Found replicate information in table\n")
      TREATMENTS <- rep_table %>%
        select(all_of(c(year_col, plot_col, rep_col))) %>%
        left_join(FIELDS_tbl %>% select(Plot_id, FL_ID), by = "Plot_id") %>%
        mutate(TRTNO = as.numeric(as.factor(paste(Year, Rep_no)))) %>%
        relocate(TRTNO, .before = everything()) %>%
        distinct()
    } else {
      # Just use plots
      all_plots <- unique(unlist(lapply(c(management_tables, observed_summary_tables), 
                                       function(df) if(plot_col %in% colnames(df)) df[[plot_col]] else NULL)))
      
      # Find a table with year information
      year_table <- NULL
      for(tbl_list in list(management_tables, observed_summary_tables)) {
        for(tbl_name in names(tbl_list)) {
          if(year_col %in% colnames(tbl_list[[tbl_name]])) {
            year_table <- tbl_list[[tbl_name]]
            break
          }
        }
        if(!is.null(year_table)) break
      }
      
      if(!is.null(year_table)) {
        # Use the minimum year as default
        default_year <- min(year_table[[year_col]])
      } else {
        # Use current year if no year information found
        default_year <- as.integer(format(Sys.Date(), "%Y"))
      }
      
      TREATMENTS <- data.frame(
        TRTNO = 1:length(all_plots),
        Plot_id = all_plots,
        Year = rep(default_year, length(all_plots))
      ) %>%
        left_join(FIELDS_tbl %>% select(Plot_id, FL_ID), by = "Plot_id")
    }
  }
  
  # Process management tables to create management events
  MANAGEMENT <- list()
  
  for(table_name in names(management_tables)) {
    df <- management_tables[[table_name]]
    
    # Skip tables that don't have both year and plot columns
    if(!(year_col %in% colnames(df) && plot_col %in% colnames(df))) {
      next
    }
    
    cat("Processing management table:", table_name, "\n")
    
    # Create a standard ID for the table
    id_name <- paste0(toupper(substr(table_name, 1, 2)), "_ID")
    
    # Get existing ID column if available
    existing_id <- get_pkeys(df)[1]
    
    # Create a processed version of the table
    processed_df <- df %>%
      # Create a management event ID if not already present
      mutate(!!id_name := if(existing_id %in% colnames(df)) .data[[existing_id]] else row_number()) %>%
      # Link to treatments
      left_join(TREATMENTS %>% select(TRTNO, Plot_id), by = "Plot_id")
    
    # Keep the table with full information (for display/reference)
    MANAGEMENT[[table_name]] <- processed_df
  }
  
  # ----- Process Observed Data -----
  
  # Process observed summary tables
  OBSERVED_Summary <- list()
  
  for(table_name in names(observed_summary_tables)) {
    df <- observed_summary_tables[[table_name]]
    
    cat("Processing observed summary table:", table_name, "\n")
    
    # Link with treatments to get TRTNO
    if(plot_col %in% colnames(df)) {
      processed_df <- df %>%
        left_join(TREATMENTS %>% select(TRTNO, Plot_id), by = "Plot_id") %>%
        relocate(TRTNO, .before = everything())
      
      OBSERVED_Summary[[table_name]] <- processed_df
    } else {
      OBSERVED_Summary[[table_name]] <- df
    }
  }
  
  # Process observed timeseries tables (if any)
  OBSERVED_TimeSeries <- list()
  
  for(table_name in names(observed_timeseries_tables)) {
    df <- observed_timeseries_tables[[table_name]]
    
    cat("Processing observed timeseries table:", table_name, "\n")
    
    # Link with treatments to get TRTNO
    if(plot_col %in% colnames(df)) {
      processed_df <- df %>%
        left_join(TREATMENTS %>% select(TRTNO, Plot_id), by = "Plot_id") %>%
        relocate(TRTNO, .before = everything())
      
      OBSERVED_TimeSeries[[table_name]] <- processed_df
    } else {
      OBSERVED_TimeSeries[[table_name]] <- df
    }
  }
  
  # ----- Create Final Output Structure -----
  
  # Create GENERAL table
  GENERAL <- data.frame(
    PERSONS = config$site_info$person,
    EMAIL = config$site_info$email,
    ADDRESS = config$site_info$address,
    SITE = config$site_info$site,
    COUNTRY = config$site_info$country
  )
  
  # Combine everything into the final structure
  result <- list(
    GENERAL = GENERAL,
    FIELDS = FIELDS,
    TREATMENTS = TREATMENTS %>% select(-Plot_id),  # Remove Plot_id from final treatment table
    MANAGEMENT = MANAGEMENT,
    OBSERVED_Summary = OBSERVED_Summary,
    OBSERVED_TimeSeries = OBSERVED_TimeSeries
  )
  
  # Add metadata attributes
  attr(result, "EXP_DETAILS") <- config$site_info$exp_details
  attr(result, "SITE_CODE") <- config$site_info$site_code
  
  return(result)
}

#' Display a summary of a reshaped dataset
#' 
#' @param fmt the reshaped dataset
#' @param name the dataset name
display_dataset_summary <- function(fmt, name) {
  cat(paste0("\n\n========== ", name, " DATASET SUMMARY ==========\n\n"))
  cat("Components in the reshaped dataset:\n")
  print(names(fmt))
  
  cat("\nExperiment details:", attr(fmt, "EXP_DETAILS"), "\n")
  cat("Site code:", attr(fmt, "SITE_CODE"), "\n\n")
  
  cat("Head of each component:\n")
  for (comp_name in names(fmt)) {
    cat("\n----- ", comp_name, " -----\n")
    
    if (comp_name == "GENERAL" || comp_name == "FIELDS") {
      print(fmt[[comp_name]])
    } else if (comp_name == "TREATMENTS") {
      cat("Treatment matrix (", nrow(fmt[[comp_name]]), " rows):\n", sep="")
      print(head(fmt[[comp_name]], 5))
    } else if (is.list(fmt[[comp_name]])) {
      if(length(fmt[[comp_name]]) == 0) {
        cat("No tables in this component\n")
      } else {
        for (tbl_name in names(fmt[[comp_name]])) {
          cat("\n* ", tbl_name, " (", nrow(fmt[[comp_name]][[tbl_name]]), " rows):\n", sep="")
          print(head(fmt[[comp_name]][[tbl_name]], 3))
        }
      }
    }
  }
}

#' Main generalized reshape function
#'
#' This function reshapes experimental datasets from multiple sources
#' into a standardized format for crop modeling
#'
#' @param dataset_name name of the dataset ("seehausen" or "munch")
#' @param custom_config optional custom configuration list
#'
#' @return a list with the reshaped dataset
reshape_exp_data_generalized <- function(dataset_name, custom_config = NULL) {
  # Create configuration for the dataset
  if (is.null(custom_config)) {
    config <- create_dataset_config(dataset_name)
  } else {
    config <- custom_config
  }
  
  # Process the dataset
  result <- process_dataset(config)
  
  # Return the reshaped dataset
  return(result)
}

#' Reshape crop experiment data into a standard ICASA format
#'
#' This function identifies the components of a crop experiment data set and re-arranges them into ICASA sections
#' (see ICASA standards: https://docs.google.com/spreadsheets/u/0/d/1MYx1ukUsCAM1pcixbVQSu49NU-LfXg-Dtt-ncLBzGAM/pub?output=html)
#' Note that the function does not handle variable mapping but only re-arranges the data structure.
#' 
#' @export
#'
#' @param db a list of data frames composing a crop experiment dataset or a dataset name ("seehausen" or "munch")
#' @param metadata a metadata table described the dataset, as returned by read_metadata (BonaRes-LTFE format)
#' @param mother_tbl a data frame; the mother table of the dataset, i.e., describing the experimental design (years, plots_id, treatments_ids, replicates)
#' 
#' @return a list containing the reshaped crop experiment data
#' 
#' @importFrom lubridate as_date parse_date_time
#' @importFrom dplyr "%>%" select group_by group_by_at ungroup mutate relocate distinct left_join arrange across cur_group_id
#' @importFrom tidyr any_of all_of everything ends_with
#' @importFrom rlang "!!" ":="
#' @importFrom countrycode countrycode
#'
reshape_exp_data <- function(db, metadata = NULL, mother_tbl = NULL) {
  # If db is a dataset name, use the generalized function directly
  if(is.character(db) && length(db) == 1) {
    return(reshape_exp_data_generalized(db))
  }
  
  # If db is a list of data frames, create a custom config
  if(is.list(db) && !is.null(metadata)) {
    cat("Using existing reshape_exp_data function with dataset parameters\n")
    
    # Extract dataset name or use a default
    dataset_name <- "custom"
    if(!is.null(metadata$name)) {
      dataset_name <- tolower(metadata$name)
    }
    
    # Determine if the dataset resembles one of our known datasets based on naming patterns
    is_seehausen <- any(grepl("seehausen", names(db), ignore.case = TRUE))
    is_munch <- any(grepl("munch", names(db), ignore.case = TRUE))
    is_rau <- any(grepl("rauischholzhausen", names(db), ignore.case = TRUE))
    
    if(is_seehausen) {
      cat("Dataset appears to be Seehausen-like\n")
      return(reshape_exp_data_generalized("seehausen"))
    } else if(is_munch) {
      cat("Dataset appears to be Munch-like\n")
      return(reshape_exp_data_generalized("munch"))
    } else if(is_rau) {
      cat("Dataset appears to be Rauischholzhausen-like\n")
      return(reshape_exp_data_generalized("rau"))
    } else {
      cat("Using original reshape_exp_data function for unknown dataset\n")
      # For custom datasets, call the original function if it exists
      if(exists("reshape_exp_data_original", mode = "function")) {
        return(reshape_exp_data_original(db, metadata, mother_tbl))
      } else {
        warning("Original reshape_exp_data_original function not found. Using Seehausen configuration.")
        return(reshape_exp_data_generalized("seehausen"))
      }
    }
  }
  
  # Default to Seehausen if no specific parameters provided
  return(reshape_exp_data_generalized("seehausen"))
}

# Run the reshape function for all three datasets and display the outputs
cat("\nProcessing Seehausen dataset...\n")
seehausen_fmt <- reshape_exp_data_generalized("seehausen")
display_dataset_summary(seehausen_fmt, "SEEHAUSEN")

cat("\nProcessing Munch dataset...\n")
munch_fmt <- reshape_exp_data_generalized("munch")
display_dataset_summary(munch_fmt, "MUNCH")

cat("\nProcessing Rau dataset...\n")
rau_fmt <- reshape_exp_data_generalized("rau")
display_dataset_summary(rau_fmt, "RAU")

# ----- Compatibility Function for Original reshape_exp_data -----

#' Compatibility wrapper for the original reshape_exp_data function
#'
#' This function provides backward compatibility with the original reshape_exp_data function
#' while leveraging the improved functionality.
#'
#' @param db a list of data frames composing a crop experiment dataset, or a dataset name ("seehausen", "munch", or "rau")
#' @param metadata a metadata table described the dataset, as returned by read_metadata (BonaRes-LTFE format)
#' @param mother_tbl a data frame; the mother table of the dataset, i.e., describing the experimental design
#' 
#' @return a list containing the reshaped crop experiment data
#' 
#' @importFrom lubridate as_date parse_date_time
#' @importFrom dplyr "%>%" select group_by ungroup mutate relocate distinct left_join arrange across cur_group_id
#' @importFrom tidyr any_of all_of everything ends_with
#' @importFrom countrycode countrycode
#'
reshape_exp_data <- function(db, metadata = NULL, mother_tbl = NULL) {
  # If db is a dataset name, use the generalized function directly
  if(is.character(db) && length(db) == 1) {
    return(reshape_exp_data_generalized(db))
  }
  
  # If db is a list of data frames, create a custom config
  if(is.list(db) && !is.null(metadata)) {
    cat("Using existing reshape_exp_data function with dataset parameters\n")
    
    # Extract dataset name or use a default
    dataset_name <- "custom"
    if(!is.null(metadata$name)) {
      dataset_name <- tolower(metadata$name)
    }
    
    # Determine if the dataset resembles one of our known datasets based on naming patterns
    is_seehausen <- any(grepl("seehausen", names(db), ignore.case = TRUE))
    is_munch <- any(grepl("munch", names(db), ignore.case = TRUE))
    is_rau <- any(grepl("rauischholzhausen", names(db), ignore.case = TRUE))
    
    if(is_seehausen) {
      cat("Dataset appears to be Seehausen-like\n")
      return(reshape_exp_data_generalized("seehausen"))
    } else if(is_munch) {
      cat("Dataset appears to be Munch-like\n")
      return(reshape_exp_data_generalized("munch"))
    } else if(is_rau) {
      cat("Dataset appears to be Rauischholzhausen-like\n")
      return(reshape_exp_data_generalized("rau"))
    } else {
      cat("Using original reshape_exp_data function for unknown dataset\n")
      # For custom datasets, call the original function if it exists
      if(exists("reshape_exp_data_original", mode = "function")) {
        return(reshape_exp_data_original(db, metadata, mother_tbl))
      } else {
        warning("Original reshape_exp_data_original function not found. Using Seehausen configuration.")
        return(reshape_exp_data_generalized("seehausen"))
      }
    }
  }
  
  # Default to Seehausen if no specific parameters provided
  return(reshape_exp_data_generalized("seehausen"))
}

cat("\nGeneralized reshape function installed as reshape_exp_data.\n")
cat("Usage:\n")
cat("  1. Call with dataset name: reshape_exp_data('seehausen')\n")
cat("  2. Call with original parameters: reshape_exp_data(db, metadata, mother_tbl)\n")
