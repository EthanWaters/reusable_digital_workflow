# Format the new control data into the stardard legacy format 


import_data <- function(data, control_data_type, sheet=1){
    out <- tryCatch(
      {
        # Could assign the control_data_type as the file name. Yet to implement 
        # this effectively
        file_name <- tools::file_path_sans_ext(basename(data))
        
  
        #Opens file and stores information into dataframe. Different file types
        #require different functions to read data
        file_extension <- file_ext(data)
        if (file_extension == 'xlsx'){
          data_df <- read_xlsx(data, sheet = sheet)
        } else if (file_extension == 'csv'){
          data_df <- read.csv(data, header = TRUE)
        } else {
          data_df <- read.table(file=data, header=TRUE)
        }
        
        #create matrix of warnings so they are added to the specified XML node 
        # in the metadata report in a vectorised mannor. 
        warnings <- names(warnings())
        warnings_matrix <- matrix(warnings, 1,length(warnings))
        contribute_to_metadata_report(control_data_type, "Import", warnings_matrix)
        
        #add any additional columns no longer in the current dataset. # may wish
        # to expand upon this to vary depending on where the input data was 
        # acquired from as PowerBI export and direct database querys have 
        # different requirements
        data_df <- add_required_columns(data_df, control_data_type, is_powerBI_export)
        
        return(data_df)
      },
      
      error=function(cond) {
        message(paste("Cannot read from:", data))
        message("Original error message:")
        message(cond)
      }
    ) 
  return(out)
}


format_control_data <- function(current_df, legacy_df, control_data_type, section){
 
  out <- tryCatch(
    {
      # Compare the format of any form of control data provided by GBRMPA with its 
      # respective legacy format. Necessary changes are made if the input is not in
      # legacy format. Changes and errors encounted are recorded for record keeping.
      # Error handling is setup so that the error will be captured and recorded. 
      # However, this will interrupt the workflow as this indicates the information  
      # provided is insufficient is required to be correct to proceed. 
      
      legacy_df <- cull_legacy_df
      currednt_df <- new_cull_data_df
      
      # create comments variable to store points of interest.
      comments <- ""
      
      # acquire column names and ensure they are formatted as characters
      current_col_names <- colnames(current_df)
      legacy_col_names <- colnames(legacy_df)
      
      # compare the column names of the legacy and current format
      matched_vector_entries <- match_vector_entries(current_col_names, legacy_col_names, section)
      matched_col_names <- matched_vector_entries[[1]]
      matching_entry_indices <- matched_vector_entries[[2]]
      metadata <- matched_vector_entries[[3]]
      
      if(any(is.na(matched_col_names)) || any(is.na(matching_entry_indices))){
        comments <- paste0(comments,"matching_entry_indices or matched_col_names has returned NA, indicating a fatal error in match_vector_entries")
      }
      
      # Update the current dataframe column names with the vector found above.
      colnames(current_df) <- matched_col_names
      
      # rearrange columns
      updated_current_df <- current_df[matching_entry_indices]
      
      # set the data type of all entries to ensure that performed operations have 
      # expected output. 
      legacy_df <- set_data_type(legacy_df, control_data_type) 
      current_df <- set_data_type(current_df, control_data_type) 

      #Determine initial error flags based on returned metadata
      initial_error_flag <- rep(NA, nrow(updated_current_df))
      if (!is_not_matching_column_names & !is_not_matching_column_names_updated){
        comments <- paste0(comments,"One or more columns in legacy format were 
                           not matched and exceeded the allowable levenshtein 
                           distance to fuzzy match an avaliable column.")
      }
      if(size_legacy_df > size_curent_df){
        comments <- paste0(comments,"Insufficient number of columns to match
                           legacy formatting.")
      }
        
      updated_current_df["error_flag"] <- initial_error_flag
      
      
      
      return(updated_current_df)
    },
    error=function(cond) {
      contribute_to_metadata_report(control_data_type, section, comments)
     
    },
  ) 
  
}


create_metadata_report <- function(count){
    #Generate the XML template for the control data process report.
    reports_location <<- "D:\\COTS\\Reusable Digital Workflows\\reusable_digital_workflow\\reports\\"
    
    #create file name systematically
    date <- as.character(Sys.Date())
    timestamp <- as.character(Sys.time())
    filename <- paste0(reports_location, "Processed Control Data Report ", date, " [", count, "].xml", sep="")
    
    #generate template
    template <- xml_new_root("session") 
    control_data <- xml_find_all(template, "//session")
    xml_add_child(control_data, "timestamp", timestamp)  
    xml_add_child(control_data, "info", sessionInfo()[-c(13,12,11,9,8)])  
    xml_add_child(control_data, "control_data")
    control_data <- xml_find_all(template, "//control_data")
    xml_add_child(control_data, "manta_tow")  
    xml_add_child(control_data, "cull") 
    xml_add_child(control_data, "RHIS")
    write_xml(template, file = filename, options =c("format", "no_declaration"))
}


contribute_to_metadata_report <- function(control_data_type, section, data, key = "Warning"){
  # Finds desired control data node and adds a section from the information 
  # obtained in the previously executed function. 
  
  # finds files with current date in file name and attempts to open xml file
  file_count <- 1
  reports_location <- "D:\\COTS\\Reusable Digital Workflows\\reusable_digital_workflow\\reports\\"
  trywait <- 0
  xml_files <- list.files(path= reports_location, pattern = as.character(Sys.Date()))
  xml_filename <- xml_files[file_count]
  xml_file <- paste0(reports_location, xml_filename, sep="")
  xml_file_data <- try(read_xml(xml_file))
  while ((class(xml_file_data)[[1]]=='try-error')&(trywait<=(5))){
    file_count <- file_count + 1
    print(paste('retrying in ', trywait, 'second(s)')) 
    Sys.sleep(trywait) 
    trywait <- trywait + 1 
    xml_file_data <- try(read_xml(paste0(reports_location, xml_filename, sep="")))
  }
  node <- xml_find_all(xml_file_data, paste0("//", control_data_type, sep=""))
  
  # Only add to node if it exists. Node Should always exist. 
  # A dataframe will add a key value pair for each column. A single value will  
  # directly assigned as a value. Columns with Multiple entries will form a list
  # before being assigned as a value. 
  # A matrix will pair each entry with the specified key. 
  if(length(node) > 0){
    
    if(is.data.frame(data)){
      xml_add_child(node, section, data)
    } else {
      xml_add_child(node, section)
      desired_nodes <- xml_find_all(xml_file_data, paste0("//", section, sep=""))
      newest_desired_node <- desired_nodes[[length(desired_nodes)]]
      sapply(1:length(data), function(i) {
        xml_add_child(newest_desired_node, key, data[i])
      }
      )
    }
    
  }
 
  
  write_xml(xml_file_data, file = xml_file, options =c("format", "no_declaration"))
}

outersect <- function(x, y) {
  sort(c(x[!x%in%y],
         y[!y%in%x]))
}

map_column_names <- function(column_names){
  lookup <- read.csv("mapNames.csv", header = TRUE)
  mapped_names <- lapply(column_names, function(x) lookup$target[match(x, lookup$current)])
  return(mapped_names)
}
  
add_required_columns <- function(df, control_data_type, is_powerBI_export){
  lookup <- read.csv("additionalColumns.csv", header = TRUE)
  if(is_powerBI_export == 1){
    new_columns <- lookup[lookup$type == control_data_type, 1]
  } else {
    new_columns <- lookup[((lookup$type == control_data_typ) & (lookup$is_mandatory == 1)), 1]
  }
  df[new_columns] <- NA
  return(df)
}
  
  
verify_control_dataframe <- function(new_data_df, legacy_data_df, control_data_type, ID_col, section, is_new){
  verified_data_df <- dataframe()
  colnames(verified_data_df) <- colnames(legacy_data_df)
  
  # If this is the first time processing the data it will require an export 
  # directly from the GBRMPA database to ensure that IDs are correct. This will 
  # then need to check every single entry to ensure it meets the requirements. 
  # Ultimately it is not possible to definitively know if a change / discrepancy 
  # was intentional or not, therefore both new and change entries will pass
  # through the same validation checks and if passed will be accepted as 
  # usable. Identifying discrepancies does not alter the checking process, it 
  # just offers the opportunity to ensure that a correct record wasn't 
  # mistakenly changed. For first time processing any error flagged entries can 
  # be compared to the legacy data set in an iterative process without checking 
  # IDs to find likely matches. 
  if(!is_new){
    # If there is a unique ID then perfect duplicates can easily be removed.
    if(!is_powerBI_export){
      
      # Update IDs that are NA from powerBI export based on perfect duplicates. 
      # Find close matches of rows left without an ID and no perfect duplicates. 
      # Rows with a single close match will be considered a discrepancy and then
      # the ID checked against the all the IDs in legacy_data_df to ensure it does 
      # not already exist.
      if(any(is.na(legacy_data_df$ID))){
        
        legacy_data_df <- update_IDs(new_data_df, legacy_data_df, control_data_type)
        
        # create new dataframe without rows that have NA ID so that they can
        # be passed through the check functions as new row entries. 
        legacy_data_df <- legacy_data_df[is.na(legacy_data_df$ID),]
        
      }
      
      
      #find perfect duplicates and add to verified data df
      perfect_duplicates <- inner_join(legacy_data_df, new_data_df)
      verified_data_df <- rbind(verified_data_df, perfect_duplicates)
      
      # Determine discrepancies and store both versions of the entries
      discrepancies_legacy <- anti_join(legacy_data_df, new_data_df)   
      discrepancies_new_indices <- which(new_data_df$ID_col %in% discrepancies_legacy$ID_col)
      discrepancies_new <- new_data_df[discrepancies_new_indices,]
      
      # find new entries based on whether the ID is present in both dataframes 
      new_entries <- anti_join(new_data_df, legacy_data_df, by=ID_col)  
      
      # Check that no IDs have changed. 
      if(!(length(discrepancies_new) == length(discrepancies_legacy))){
        # This would imply entries with tempory IDs have been previously processed 
        # and have now been updated. This will require an iterative process to 
        # find the closest matching record. This will be required for any
        # situation without an ID.
        FAILURE <- TRUE
        contribute_to_metadata_report(control_data_type, "Check ID Change", FAILURE)
        
      }
        
    } else {

      # find close matching rows (distance of two) based on all columns except ID. ID is not 
      # because it will always be null if the data is exported from powerBI. 
      distance <- 2
      new_data_without_ID_df <- new_data_df[ , -which(names(new_data_df) %in% c("ID", "error_flag"))]
      legacy_data_without_ID_df <- legacy_data_df[ , -which(names(legacy_data_df) %in% c("ID", "error_flag"))]
      close_match_rows <- find_close_matches(new_data_without_ID_df, legacy_data_without_ID_df, distance)
      
      discrepancies_new_indices <- c()
      discrepancies_legacy_indices <- c()
      perfect_duplicate_new_indices <- c()
      perfect_duplicate_legacy_indices <- c()
      
      # Iterate through list of close_match_rows to acquire indices of
      # discrepancies
      for(x in 1:length(close_match_rows)){
        # if a row only has one close_match_row, it is considered a discrepancy
        # or a perfect match 
        if(length(close_match_rows[[x]]) == 1){
          if(close_match_rows[[x]][[1]][3] == 0){
            perfect_duplicate_legacy_indices <- c(perfect_duplicate_legacy_indices, close_match_rows[[x]][[1]][2])
            perfect_duplicate_new_indices <- c(perfect_duplicate_new_indices, close_match_rows[[x]][[1]][1])
          } else if(close_match_rows[[x]][[1]][3] == 1){
            discrepancies_legacy_indices <- c(discrepancies_legacy_indices, close_match_rows[[x]][[1]][2])
            discrepancies_new_indices <- c(discrepancies_new_indices, close_match_rows[[x]][[1]][1])
          }
        } else if (length(close_match_rows[[x]]) > 1) {
          # if a row only has multiple close_match_rows, the one with the 
          # closest distance is considered a discrepancy. If there are multiple 
          # then they are disregarded and assumed to be new entries
          match_index_matrix <- do.call(rbind, close_match_rows[[x]])
          closest_match <- which(min(match_index_matrix[,3]) == match_index_matrix[,3])
          if(length(closest_match) == 1){
            if(close_match_rows[[x]][[1]][3] == 0){
              perfect_duplicate_legacy_indices <- c(perfect_duplicate_legacy_indices, close_match_rows[[x]][[1]][2])
              perfect_duplicate_new_indices <- c(perfect_duplicate_new_indices, close_match_rows[[x]][[1]][1])
            } else if(close_match_rows[[x]][[1]][3] == 1){
              discrepancies_legacy_indices <- c(discrepancies_legacy_indices, close_match_rows[[x]][[1]][2])
              discrepancies_new_indices <- c(discrepancies_new_indices, close_match_rows[[x]][[1]][1])
            }
          }
        } 
      }
      
      perfect_duplicates <- new_data_df[perfect_duplicate_new_indices,]
      verified_data_df <- rbind(verified_data_df, perfect_duplicates)
      
      discrepancies_legacy <- legacy_data_df[discrepancies_legacy_indices,]
      discrepancies_new <- new_data_df[discrepancies_new_indices,]
      new_entries <- unmatched_new_data_df[-discrepancies_new_indices,]
    }

    # Given that it is not possible to definitively know if a change / discrepancy 
    # was intentional or not both new and change entries will pass through the 
    # same validation checks and if passed will be accepted as usable and assumed to be . If failed, 
    # assumed to be a QA change. If failed,  the data will be flagged. Failed 
    # discrepancies will check the original legacy entry, which if failed will 
    # be left as is. 
    verified_new <- verify_entries(new_entries, control_data_type)
    verified_discrepancies <- verify_entries(discrepancies_new, control_data_type)
    verified_discrepancies <- compare_discrepancies(discrepancies_new, discrepancies_legacy, control_data_type)
    verified_data_df <- rbind(verified_data_df, verified_discrepancies)
    verified_data_df <- rbind(verified_data_df, verified_new)
   
  } else if (is_new & !is_powerBI_export){
    verified_new <- verify_entries(new_data_df, control_data_type)
    verified_data_df <- rbind(verified_data_df, verified_new)
    
  }
  
  # merge the verified dataset
  return(verified_data_df)
  
}


verify_entries <- function(){
  
  
}


update_IDs <- function(new_data_df, legacy_data_df, control_data_type){
  # Attempt to update the IDs of the legacy data if the previous processing 
  # utilised data from a powerBI export and therefore will have IDs of NA. This 
  # will find perfect matches (distance of zero).
  
  # This is very similar code to the find_close_match function. To ensure that
  # IDs found are appended to the correct entry with the least risk two minor 
  # adjustments have been made that are only suitable in this scenario. 
  # The original dataframes are iterated over to be confident the correct index
  # is known. If the ID of a Row is not NA then it will be skipped. Comparisons 
  # are made between the two original dataframes where the ID column is removed.
  legacy_data_without_ID_df <- legacy_data_df[ , -which(names(missing_id_rows) %in% c("ID", "error_flag"))]
  new_data_without_ID_df <- new_data_df[ , -which(names(new_data_df) %in% c("ID", "error_flag"))]
  
  matches <- lapply(1:nrow(legacy_data_df), function(z){ 
    if(is.na(legacy_data_df$ID[z])){
      for(i in 1:nrow(new_data_df)){ 
        if(length(na.omit(match(legacy_data_without_ID_df[z,], new_data_without_ID_df[i,]))) == length(new_data_without_ID_df[i,])){
          c(z, i)
        }
      }
    }
  })
  
  # Once matches are found, IDs can then be altered. If there are multiple 
  # matches then they are left as is. Ultimately this means they will be treated
  # as a new entry. 
  legacy_IDs <- legacy_data_df$ID
  for(x in filtered_matches){
    if(length(x) == 1){
      new_ID <- new_data_df[x[[1]][2],which(names(new_data_df) %in% c("ID"))]
      if(!new_ID %in% legacy_IDs){
        legacy_data_df[x[[1]][1],which(names(legacy_data_df) %in% c("ID"))] <- new_ID
      }
    }
  }
  
  remaining_non_ID <- nrow(legacy_data_df[is.na(legacy_data_df$ID),])
  contribute_to_metadata_report(control_data_type, "Update ID", remaining_non_ID)
  return(legacy_data_df)
}


find_close_matches <- function(x, y, distance){
  # Find list of all close matches between rows in x and y within a specified 
  # distance. This distance is the number of non perfect column matches within
  # a row. returns a list of lists. Each is a vector containing the indices of
  # the rows matched and the distance from perfect. (X_index, Y_index, Distance)
  
  matches <- lapply(1:nrow(x), function(z){ 
    for(i in 1:nrow(y)){ 
      if(length(na.omit(match(x[z,], y[i,]))) >= (length(y[i,]) - distance)){
        c(z, i,length(y[1,]) - length(na.omit(match(x[z,], y[i,]))))
      }
    }
  })
  filtered_matches <- lapply(matches, function(a) Filter(Negate(is.null), a))
  
  # Currently not necessary but it may be desirable to have a single list of 
  # vectors rather than a list of lists
  # filtered_matches <- unlist(filtered_matches, recursive = FALSE)
  return(filtered_matches)
}


find_previous_process_date <- function(){
  # finds the dates stored in the metadata report file names with REGEX. 
  # reports_location is a global variable defined when creating the meta data 
  # report. 
  xml_files <- list.files(path= reports_location, pattern = as.character("Processed Control Data Report "))
  dates <- regmatches(xml_files, regexpr("[0-9-]{10}", xml_files))
  if(length > 0) {
    return(max(as.Date(dates)))
  } else {
    return(NA)
  }
 
}

set_data_type <- function(data_df, control_data_type){
  # sets the data_type of each column of any dataframe input based on the values
  # in a lookup table stored in a CSV. This method was chosen to increase 
  # molecularity and flexibility whilst still remaining definitive. 
  
  # create list of column name partials grouped by desired data type. The data 
  # types will be utilised as list names. 
  column_names <- colnames(data_df)
  setDataType_df <- read.csv("setDataType.csv", header = TRUE)
  
  setDataTypeList <- lapply(dataTypes, function(x) setDataType_df$column[which(x == setDataType_df$dataType)])
  names(setDataTypeList) <- dataTypes 
  
  # compare column names retrieved from lookup table to column names in the 
  # in the dataframe. Check and find closest matches. sets the data types for
  # all columns 
  for(i in dataTypes){
    columns <- setDataTypeList$i
    columns <- match_vector_entries(columns, column_names, "Set Data Type")[[1]]
    if(i == "Numeric"){
      apply(columns, function(x) data_df$x <- as.numeric(data_df$x))
    } else if (i == "Date") {
      apply(columns, function(x) data_df$x <- as.Date(data_df$x))
    } else if (i == "Integer") {
      apply(columns, function(x) data_df$x <- as.integer(data_df$x))
    } else if (i == "Character"){
      apply(columns, function(x) data_df$x <- as.character(data_df$x))
    } else if (i == "Logical"){
      apply(columns, function(x) data_df$x <- as.logical(data_df$x))
    }
    
    #create matrix of warnings so they are added to the specified XML node 
    # in the metadata report in a vectorised mannor. 
    warnings <- names(warnings())
    warnings_matrix <- matrix(warnings, 1,length(warnings))
    contribute_to_metadata_report(control_data_type, paste("Set Data Type", i), warnings_matrix)
  }
  return(data_df)
  
} 


match_vector_entries <- function(current_vec, target_vec, section){
  # Compare any form two vectors and identify matching entries.
  
  out <- tryCatch(
    {
    
    # clean vector entries for easy comparison. The cleaning is done in this 
    # specific order to remove characters such as '.' that appear after
    # removing spaces or specific character from text ina CSV. 
    clean_current_vec <- gsub('[[:punct:] ]+',' ',current_vec)
    clean_target_vec <- gsub('[[:punct:] ]+',' ',target_vec)
    clean_current_vec <- gsub(' ', '',clean_current_vec)
    clean_target_vec <- gsub(' ', '',clean_target_vec)
    clean_current_vec <- gsub('\\.', '', clean_current_vec)
    clean_target_vec <- gsub('\\.', '', clean_target_vec)
    clean_current_vec <- gsub('[)(/&]', '', clean_current_vec)
    clean_target_vec <- gsub('[)(/&]', '', clean_target_vec)
    clean_current_vec <- tolower(clean_current_vec)
    clean_target_vec <- tolower(clean_target_vec)
    
    # Collect information for metadata report and define variables 
    size_target_vec <- length(target_vec)
    size_current_vec <- length(current_vec)
    
    # initialise for metadata report
    is_not_matching_entries <- NA
    is_not_matching_column_names_updated <- NA
    updated_nonmatching_column_names_str <- NA
    is_matching_indices_unique <- NA 
    is_column_name_na <- NA   
    
    # Set the maximum distance for fuzzy string matching
    maxium_levenshtein_distance <- 5
    
    # determine the current column names and indices that match a column name 
    # in the legacy format. Count how many match. 
    matching_entries <- intersect(clean_current_vec, clean_target_vec)
    matching_entries_length <- length(matching_entries)
    matching_target_entries_indices <- which(clean_target_vec %in% matching_entries)
    matching_current_entries_indexes <- which(clean_current_vec %in% matching_entries)
    
    # conditional statements to be passed to error handling so a more detailed 
    # description of any failure mode can be provided. 
    is_not_matching_entries <- !((matching_entries_length == length(clean_current_vec)) || (matching_entries_length == length(clean_target_vec)))
                                 
    # Map legacy column names to current column names that are not a perfect
    # match. This occurs by matching partial strings contained within column 
    # names, check lookup table for pre-defined column name mapppings or find 
    # closest match within a specified distance with levenshtein distances.
    # Comparisons will be performed with vector of cleaned column names but 
    # the original vector of column names with uncleaned text will be utilsied 
    # to store the column names
    if(is_not_matching_entries){
      closest_matching_indices <- c()
      
      nonmatching_current_entry_indices <- 1:length(current_vec)
      nonmatching_current_entry_indices <- nonmatching_current_entry_indices[-matching_current_entries_indexes]
      nonmatching_entries <- clean_current_vec[nonmatching_current_entry_indices]
      
      # check if there is a pre-defined mapping to a non-matching column name.
      mapped_output <- map_column_names(nonmatching_entries)
      mapped_name_indices <- which(!is.na(mapped_output))
      
      #control statements below utilised to prevent errors
      if(is.list(mapped_output)){
        mapped_output <- unlist(mapped_output)
      } 
      
      # include pre-defined mappings found and remove them from non-matching 
      # vector
      if (length(na.omit(mapped_name_indices)) > 0){
        for(x in mapped_name_indices){ 
          mapped_index <- nonmatching_current_entry_indices[x]
          current_vec[mapped_index] <- mapped_output[x]
          clean_mapped_name <- gsub('[[:punct:] ]+',' ', mapped_output[x])
          clean_mapped_name <- gsub(' ', '', clean_mapped_name)
          clean_mapped_name <- gsub('\\.', '', clean_mapped_name)
          clean_mapped_name <- gsub('[)(/&]', '', clean_mapped_name)
          clean_mapped_name <- tolower(clean_mapped_name)
          clean_current_vec[mapped_index] <- clean_mapped_name
        }
        nonmatching_entries <- nonmatching_entries[-mapped_name_indices]
        nonmatching_current_entry_indices <- nonmatching_current_entry_indices[-mapped_name_indices]
      }
      
      # find closest match within a specified distance with levenshtein
      # distances or matching partial strings contained within column 
      # names.
      for(i in nonmatching_current_entry_indices){
        entry <- clean_current_vec[i]
        levenshtein_distances <- adist(entry , clean_target_vec)
        minimum_distance <- min(levenshtein_distances)
        closest_matching_indices <- which(levenshtein_distances==minimum_distance)
        
        partial_name_matches <- grep(entry, clean_target_vec)
        if(length(partial_name_matches) == 1){
          current_vec[i] <- target_vec[partial_name_matches]
          clean_current_vec[i] <- clean_target_vec[partial_name_matches]
        } else if ((length(closest_matching_indices) == 1) & (minimum_distance < maxium_levenshtein_distance)) {
          current_vec[i] <- target_vec[closest_matching_indices]
          clean_current_vec[i] <- clean_target_vec[closest_matching_indices]
        } 
        
        
      }
    }
    # Indices should be unique and not NA. Check multiple columns weren't 
    # matched to the same column. The appropriate cut off distance may need 
    # to be tweaked overtime. For metadata report.
    duplicate_column_indices <- duplicated(current_vec)
    is_matching_indices_unique <- !any(duplicated(closest_matching_indices))
    is_column_name_na <- any(is.na(current_vec))
    
    # find list of vector of indices which indicate the position of current columns names in the legacy format. 
    # this will be used to indicate if at the end of the mapping and matching process, the program was able to 
    # correctly find all required columns. This will also then be utilised to rearrange the order of the columns 
    updated_matching_entry_indices <- sapply(clean_target_vec, function(x) match(x, clean_current_vec))
    updated_matching_entry_indices <- updated_matching_entry_indices[!is.na(updated_matching_entry_indices)]
    
    # For metadata report.
    updated_matching_entries <- current_vec[updated_matching_entry_indices]
    updated_nonmatching_entry_indices <- which(!clean_current_vec %in% clean_target_vec)
    updated_nonmatching_entries <- clean_current_vec[updated_nonmatching_entry_indices]
    is_not_matching_entries_updated <- !((length(updated_matching_entries) == length(clean_current_vec)) || (length(updated_matching_entries) == length(clean_target_vec)))
    updated_nonmatching_entries_str <- paste0(updated_nonmatching_entries, collapse=', ')
    
    
    
    metadata <- data.frame(size_target_vec,
                           size_current_vec,
                           is_not_matching_entries, 
                           is_not_matching_entries_updated,
                           updated_nonmatching_entries_str,
                           is_matching_indices_unique,
                           is_column_name_na) 
    
    # write any warnings and points of interest generated to the metadata report
    warnings <- names(warnings())
    warnings_matrix <- matrix(warnings, 1,length(warnings))
    contribute_to_metadata_report(control_data_type, section, warnings_matrix)
    contribute_to_metadata_report(control_data_type, section, metadata)
    
    output <- list(current_vec, updated_matching_entry_indices, metadata)
    
    return(output)
  
  },
    error=function(cond) {
      contribute_to_metadata_report(control_data_type, section, comments)
      
  }
  )
}




