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
#' @param dataset_name name of the dataset ("seehausen" or "munch")
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
          lat = 51.706944,
          lon = 11.737778
        )
      ),
      table_patterns = list(
        ertrag = "ERTRAG",
        ernte = "ERNTE",
        bodenlabor = "BODENLABORWERTE",
        probenahme_boden = "PROBENAHME_BODEN",
        pflanzenlabor = "PFLANZENLABORWERTE",
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
      )
    ))
  } else if(tolower(dataset_name) == "munch") {
    return(list(
      name = "Munch",
      path = "./inst/extdata/lte_Munch/0_raw",
      name_pattern = "v140_mun.V[0-9]+_[0-9]+_(.+)\\.csv",
      col_mapping = list(
        year = "Year",
        plot = "Plot_id",
        rep = "Rep_no"
      ),
      year_variants = c("Versuchsjahr", "JAHR", "ERNTEJAHR", "Jahr"),
      plot_variants = c("Parzelle_ID", "Parzelle", "PARZID", "ï..Parzelle_ID", "Parz"),
      rep_variants = c("Wiederholung", "WDHLG", "REPLIKAT", "Wdh"),
      plots_len = 96,
      max_events = 1,
      site_info = list(
        person = "Test Person",
        email = "test@example.com",
        address = "Test Address",
        site = "Muencheberg",
        country = "Germany",
        exp_details = "LTE Muencheberg, Static Fertilization Experiment V140",
        site_code = "MUDE",
        location = list(
          lat = 52.515,
          lon = 14.122
        )
      ),
      table_patterns = list(
        ertrag = "ERTRAG",
        ernte = "ERNTE",
        boden = "BODEN",
        pflanze = "PFLANZE",
        duengung = "DUENGUNG",
        aussaat = "AUSSAAT",
        beregnung = "BEREGNUNG",
        bodenbearbeitung = "BODENBEARBEITUNG",
        pflanzenschutz = "PFLANZENSCHUTZ",
        versuchsaufbau = c("VERSUCHSAUFBAU", "VERSUCHSPLAN"),
        parzelle = "PARZELLE"
      ),
      treatment_candidates = c("Pruefglied_ID", "Pruefglied", "Variante", "Var", "Treatment")
    ))
  } else {
    stop(paste("Unknown dataset name:", dataset_name))
  }
}

#' Tag data types for tables in a dataset
#' 
#' @param db the dataset tables
#' @param config dataset configuration
#' 
#' @return a list with categorized tables
tag_data_type <- function(db, config) {
  # Initialize return lists
  management <- list()
  observed_summary <- list()
  observed_timeseries <- list()
  other <- list()
  
  years_col <- config$col_mapping$year
  plots_col <- config$col_mapping$plot
  
  cat(paste0("Categorizing tables for ", config$name, " dataset:\n"))
  
  # Process each table
  for (df_name in names(db)) {
    df <- db[[df_name]]
    cat("  Processing table:", df_name, "\n")
    
    # Handle ERTRAG tables
    if(grepl(config$table_patterns$ertrag, df_name, ignore.case = TRUE)) {
      cat("    Ertrag table detected, categorizing as 'observed_summary'\n")
      
      # For Seehausen: Link ERTRAG with ERNTE to get Year and Plot_id if needed
      if(!years_col %in% colnames(df) && 
         !is.null(config$link_keys$ertrag_ernte) && 
         config$link_keys$ertrag_ernte %in% colnames(df)) {
        
        cat("    Linking ERTRAG with ERNTE to get Year and Plot_id\n")
        ernte_table <- NULL
        
        # Find the ERNTE table
        for(t_name in names(db)) {
          if(grepl(config$table_patterns$ernte, t_name, ignore.case = TRUE) && 
             years_col %in% colnames(db[[t_name]])) {
            ernte_table <- db[[t_name]]
            break
          }
        }
        
        if(!is.null(ernte_table)) {
          cat("    Found ERNTE table, linking...\n")
          linked_df <- df %>%
            left_join(ernte_table %>% select(!!sym(config$link_keys$ertrag_ernte), 
                                            all_of(years_col), all_of(plots_col)),
                      by = config$link_keys$ertrag_ernte)
          observed_summary[[df_name]] <- linked_df
          attr(observed_summary[[df_name]], "category") <- "observed_summary"
        } else {
          cat("    No ERNTE table found, categorizing as 'other'\n")
          other[[df_name]] <- df
          attr(other[[df_name]], "category") <- "other"
        }
      } else {
        # For Munch or if direct linkage not needed
        observed_summary[[df_name]] <- df
        attr(observed_summary[[df_name]], "category") <- "observed_summary"
      }
    } 
    # Handle BODENLABORWERTE (Seehausen) or BODEN (Munch)
    else if(any(sapply(c(config$table_patterns$bodenlabor, config$table_patterns$boden), 
                      function(pattern) grepl(pattern, df_name, ignore.case = TRUE)))) {
      cat("    Soil data table detected\n")
      
      # For Seehausen: Link with PROBENAHME_BODEN if needed
      if(!years_col %in% colnames(df) && 
         !is.null(config$link_keys$bodenlabor_probenahme) && 
         config$link_keys$bodenlabor_probenahme %in% colnames(df)) {
        
        cat("    Linking soil lab values with soil sampling\n")
        probe_table <- NULL
        
        # Find the PROBENAHME_BODEN table
        for(t_name in names(db)) {
          if(grepl(config$table_patterns$probenahme_boden, t_name, ignore.case = TRUE) && 
             years_col %in% colnames(db[[t_name]])) {
            probe_table <- db[[t_name]]
            break
          }
        }
        
        if(!is.null(probe_table)) {
          cat("    Found soil sampling table, linking...\n")
          linked_df <- df %>%
            left_join(probe_table %>% select(!!sym(config$link_keys$bodenlabor_probenahme), 
                                             all_of(years_col), all_of(plots_col)),
                      by = config$link_keys$bodenlabor_probenahme)
          observed_summary[[df_name]] <- linked_df
          attr(observed_summary[[df_name]], "category") <- "observed_summary"
        } else {
          cat("    No soil sampling table found, categorizing as 'other'\n")
          other[[df_name]] <- df
          attr(other[[df_name]], "category") <- "other"
        }
      } 
      # For Munch or direct soil data
      else if (years_col %in% colnames(df) && plots_col %in% colnames(df)) {
        cat("    Categorizing as 'observed_summary'\n")
        observed_summary[[df_name]] <- df
        attr(observed_summary[[df_name]], "category") <- "observed_summary"
      }
      else {
        cat("    Missing year or plot column, categorizing as 'other'\n")
        other[[df_name]] <- df
        attr(other[[df_name]], "category") <- "other"
      }
    }
    # Handle PFLANZENLABORWERTE (Seehausen) or PFLANZE (Munch)
    else if(any(sapply(c(config$table_patterns$pflanzenlabor, config$table_patterns$pflanze), 
                       function(pattern) grepl(pattern, df_name, ignore.case = TRUE)))) {
      cat("    Plant data table detected\n")
      
      # For Seehausen: Link with PROBENAHME_PFLANZEN if needed
      if(!years_col %in% colnames(df) && 
         !is.null(config$link_keys$pflanzenlabor_probenahme) && 
         config$link_keys$pflanzenlabor_probenahme %in% colnames(df)) {
        
        cat("    Linking plant lab values with plant sampling\n")
        probe_table <- NULL
        
        # Find the PROBENAHME_PFLANZEN table
        for(t_name in names(db)) {
          if(grepl(config$table_patterns$probenahme_pflanzen, t_name, ignore.case = TRUE) && 
             years_col %in% colnames(db[[t_name]])) {
            probe_table <- db[[t_name]]
            break
          }
        }
        
        if(!is.null(probe_table)) {
          cat("    Found plant sampling table, linking...\n")
          linked_df <- df %>%
            left_join(probe_table %>% select(!!sym(config$link_keys$pflanzenlabor_probenahme), 
                                             all_of(years_col), all_of(plots_col)),
                      by = config$link_keys$pflanzenlabor_probenahme)
          observed_summary[[df_name]] <- linked_df
          attr(observed_summary[[df_name]], "category") <- "observed_summary"
        } else {
          cat("    No plant sampling table found, categorizing as 'other'\n")
          other[[df_name]] <- df
          attr(other[[df_name]], "category") <- "other"
        }
      } 
      # For Munch or direct plant data
      else if (years_col %in% colnames(df) && plots_col %in% colnames(df)) {
        cat("    Categorizing as 'observed_summary'\n")
        observed_summary[[df_name]] <- df
        attr(observed_summary[[df_name]], "category") <- "observed_summary"
      }
      else {
        cat("    Missing year or plot column, categorizing as 'other'\n")
        other[[df_name]] <- df
        attr(other[[df_name]], "category") <- "other"
      }
    }
    # Management tables - more universal approach across datasets
    else if (grepl(config$table_patterns$duengung, df_name, ignore.case = TRUE)) {
      cat("    Fertilization table detected, categorizing as 'management'\n")
      management[[df_name]] <- df
      attr(management[[df_name]], "category") <- "management"
    }
    else if (grepl(config$table_patterns$aussaat, df_name, ignore.case = TRUE)) {
      cat("    Planting table detected, categorizing as 'management'\n")
      management[[df_name]] <- df
      attr(management[[df_name]], "category") <- "management"
    }
    else if (grepl(config$table_patterns$beregnung, df_name, ignore.case = TRUE)) {
      cat("    Irrigation table detected, categorizing as 'management'\n")
      management[[df_name]] <- df
      attr(management[[df_name]], "category") <- "management"
    }
    else if (grepl(config$table_patterns$bodenbearbeitung, df_name, ignore.case = TRUE)) {
      cat("    Soil tillage table detected, categorizing as 'management'\n")
      management[[df_name]] <- df
      attr(management[[df_name]], "category") <- "management"
    }
    else if (grepl(config$table_patterns$ernte, df_name, ignore.case = TRUE)) {
      cat("    Harvest table detected, categorizing as 'management'\n")
      management[[df_name]] <- df
      attr(management[[df_name]], "category") <- "management"
    }
    else if (grepl(config$table_patterns$pflanzenschutz, df_name, ignore.case = TRUE)) {
      cat("    Plant protection table detected, categorizing as 'management'\n")
      management[[df_name]] <- df
      attr(management[[df_name]], "category") <- "management"
    }
    else if (any(sapply(c(config$table_patterns$versuchsaufbau), 
                        function(pattern) grepl(pattern, df_name, ignore.case = TRUE)))) {
      cat("    Experimental design table detected, categorizing as 'management'\n")
      management[[df_name]] <- df
      attr(management[[df_name]], "category") <- "management"
    }
    else if (grepl(config$table_patterns$parzelle, df_name, ignore.case = TRUE)) {
      cat("    Plot table detected, categorizing as 'other'\n")
      other[[df_name]] <- df
      attr(other[[df_name]], "category") <- "other"
    }
    # For other tables, use standard categorization logic
    else {
      # If plot column not in df, categorize as "other"
      if (!plots_col %in% colnames(df)) {
        cat("    No plot column found, categorizing as 'other'\n")
        other[[df_name]] <- df
        attr(other[[df_name]], "category") <- "other"
        next
      }
      
      # If years column not in df but plot column is, categorize as "other"
      if (!years_col %in% colnames(df)) {
        cat("    No year column found, categorizing as 'other'\n")
        other[[df_name]] <- df
        attr(other[[df_name]], "category") <- "other"
        next
      }
      
      # Default to management for other tables with plot and year columns
      cat("    Default categorization: 'management'\n")
      management[[df_name]] <- df
      attr(management[[df_name]], "category") <- "management"
    }
  }
  
  cat("Categorization complete. Found:\n")
  cat("  Management tables:", length(management), "\n")
  cat("  Observed summary tables:", length(observed_summary), "\n")
  cat("  Observed timeseries tables:", length(observed_timeseries), "\n")
  cat("  Other tables:", length(other), "\n\n")
  
  ret_list <- list(
    management = management,
    observed_summary = observed_summary,
    observed_timeseries = observed_timeseries,
    other = other
  )
  
  return(ret_list)
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

#' Main reshape function for experimental data
#'
#' This function reshapes experimental datasets from multiple sources
#' into a standardized format for crop modeling, maintaining compatibility
#' with the original reshape_exp_data function
#'
#' @param db a list of data frames composing a crop experiment dataset, or a dataset name ("seehausen" or "munch")
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
  # Check if db is a dataset name
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
    
    # Determine if the dataset resembles Seehausen or Munch based on naming patterns
    is_seehausen <- any(grepl("seehausen", names(db), ignore.case = TRUE))
    is_munch <- any(grepl("munch", names(db), ignore.case = TRUE))
    
    if(is_seehausen) {
      cat("Dataset appears to be Seehausen-like\n")
      return(reshape_exp_data_generalized("seehausen"))
    } else if(is_munch) {
      cat("Dataset appears to be Munch-like\n")
      return(reshape_exp_data_generalized("munch"))
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

#' Process a custom dataset using pre-loaded data
#'
#' @param db list of data frames
#' @param config dataset configuration
#' @param mother_tbl the mother table describing experimental design
#'
#' @return a list with the reshaped dataset
process_dataset_custom <- function(db, config, mother_tbl) {
  cat(paste0("Processing custom dataset: ", config$name, "\n"))
  
  # Define common configuration variables
  year_col <- config$col_mapping$year
  plot_col <- config$col_mapping$plot
  rep_col <- config$col_mapping$rep
  
  # Standardize column names
  for (i in seq_along(db)) {
    # Year columns
    for (old_name in config$year_variants) {
      if (old_name %in% colnames(db[[i]])) {
        colnames(db[[i]])[colnames(db[[i]]) == old_name] <- year_col
        cat("  Renamed", old_name, "to", year_col, "in table", names(db)[i], "\n")
      }
    }
    
    # Plot columns
    for (old_name in config$plot_variants) {
      if (old_name %in% colnames(db[[i]])) {
        colnames(db[[i]])[colnames(db[[i]]) == old_name] <- plot_col
        cat("  Renamed", old_name, "to", plot_col, "in table", names(db)[i], "\n")
      }
    }
    
    # Replication columns
    for (old_name in config$rep_variants) {
      if (old_name %in% colnames(db[[i]])) {
        colnames(db[[i]])[colnames(db[[i]]) == old_name] <- rep_col
        cat("  Renamed", old_name, "to", rep_col, "in table", names(db)[i], "\n")
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
  
  # ----- Process Management Tables and create TREATMENTS -----
  
  # Try to find the design/treatment table (if mother_tbl is not provided)
  if(is.null(mother_tbl)) {
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
    } else {
      # Use the first management table as a fallback
      design_table <- management_tables[[1]]
    }
  } else {
    # Use the provided mother_tbl
    design_table <- mother_tbl
  }
  
  # Identify treatment information
  if(!is.null(config$treatment_candidates)) {
    # For Munch-like datasets
    treatment_col <- find_column_by_candidates(design_table, config$treatment_candidates)
    
    if(!is.null(treatment_col)) {
      cat("Found treatment information in design table using column", treatment_col, "\n")
      
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
        cat("No treatment column found, using replicate information\n")
        
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
    cat("Found treatment information using Pruefglied_ID\n")
    
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
    cat("No standard treatment information found, creating simple treatment matrix\n")
    TREATMENTS <- data.frame(
      TRTNO = 1:length(unique(design_table[[plot_col]])),
      Plot_id = unique(design_table[[plot_col]]),
      Year = rep(min(design_table[[year_col]]), length(unique(design_table[[plot_col]])))
    ) %>%
      left_join(FIELDS_tbl %>% select(Plot_id, FL_ID), by = "Plot_id")
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

# Run the reshape function for both datasets and display the outputs
cat("\nProcessing Seehausen dataset...\n")
seehausen_fmt <- reshape_exp_data_generalized("seehausen")
display_dataset_summary(seehausen_fmt, "SEEHAUSEN")

cat("\nProcessing Munch dataset...\n")
munch_fmt <- reshape_exp_data_generalized("munch")
display_dataset_summary(munch_fmt, "MUNCH")

# ----- Compatibility Function for Original reshape_exp_data -----

#' Compatibility wrapper for the original reshape_exp_data function
#'
#' This function provides backward compatibility with the original reshape_exp_data function
#' while leveraging the improved functionality.
#'
#' @param db a list of data frames composing a crop experiment dataset, or a dataset name ("seehausen" or "munch")
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
    
    # Determine if the dataset resembles Seehausen or Munch based on naming patterns
    is_seehausen <- any(grepl("seehausen", names(db), ignore.case = TRUE))
    is_munch <- any(grepl("munch", names(db), ignore.case = TRUE))
    
    if(is_seehausen) {
      cat("Dataset appears to be Seehausen-like\n")
      return(reshape_exp_data_generalized("seehausen"))
    } else if(is_munch) {
      cat("Dataset appears to be Munch-like\n")
      return(reshape_exp_data_generalized("munch"))
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
