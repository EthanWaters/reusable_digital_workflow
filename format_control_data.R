# Format the new control data into the stardard legacy format 


import_data <- function(data, control_data_type, is_powerBI_export, sheet=1){
    out <- tryCatch(
      {
        # Could assign the control_data_type as the file name. Yet to implement 
        # this effectively
        
        file_name <- tools::file_path_sans_ext(basename(data))
       
        # Opens file and stores information into dataframe. Different file types
        # require different functions to read data
        file_extension <- file_ext(data)
        if (file_extension == 'xlsx'){
          data_tibble <- read_xlsx(data, sheet = sheet)
          data_tibble_colnames <- colnames(data_tibble)
          data_df <- data.frame(data_tibble)
          colnames(data_df) <- data_tibble_colnames
        } else if (file_extension == 'csv'){
          data_df <- read.csv(data, header = TRUE,  encoding="UTF-8", check.names=FALSE)
        } else {
          data_df <- read.table(file=data, header=TRUE)
        }
        
        column_names <- colnames(data_df)
        column_names <- gsub("\\.", " ", column_names)
        column_names <- gsub("\\s+", " ", column_names)
        colnames(data_df) <- column_names
        
        # Remove rows where the majority of fields are NA
        
        # has.na <- c()
        # for(x in 1:length(data_df)){
        #   if(any(is.na(data_df[x,]))){
        #     index <- sapply(data_df[x,], function(i){is.na(i)})
        #     if(length(x[index]) >= length(x)/2){
        #       has.na <- c(has.na, x)
        #     }
        #   }
        # }
        # data_df <- data_df[-has.na,]
        
        # this is temporary until a more sophisticated method is produced
        data_df <- na.omit(data_df)
        
        # create matrix of warnings so they are added to the specified XML node 
        # in the metadata report in a vectorised mannor. 
       
        # warnings <- names(warnings())
        # warnings_matrix <- matrix(warnings, 1,length(warnings))
        # contribute_to_metadata_report(control_data_type, "Import", warnings_matrix)
        
        # add any additional columns no longer in the current dataset. # may wish
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
      
      # current_df <- new_cull_data_df
      # legacy_df <- cull_legacy_df
      # create comments variable to store points of interest.
      comments <- ""
      
      # acquire column names and ensure they are formatted as characters
      current_col_names <- colnames(current_df)
      legacy_col_names <- colnames(legacy_df)
      
      # compare the column names of the legacy and current format
      matched_vector_entries <- match_vector_entries(current_col_names, legacy_col_names, section, correct_order = FALSE, check_mapped = TRUE)
      matched_col_names <- matched_vector_entries[[1]]
      matching_entry_indices <- matched_vector_entries[[2]]
      metadata <- matched_vector_entries[[4]]
      
      if(any(is.na(matched_col_names)) || any(is.na(matching_entry_indices))){
        comments <- paste0(comments,"matching_entry_indices or matched_col_names has returned NA, indicating a fatal error in match_vector_entries")
      }
      
      # rearrange columns
      updated_current_df <- current_df[matching_entry_indices]
      
      # Update the current dataframe column names with the vector found above.
      colnames(updated_current_df) <- matched_col_names
      
      
      
      # set the data type of all entries to ensure that performed operations have 
      # expected output. 
      updated_current_df <- set_data_type(updated_current_df, control_data_type) 

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
      
      # contribute_to_metadata_report(control_data_type, section, comments)
      
      
      return(updated_current_df)
    },
    error=function(cond) {
      contribute_to_metadata_report(control_data_type, section, cond)
     
    }
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
    new_columns <- lookup[((lookup$type == control_data_type) & (lookup$is_mandatory == 1)), 1]
  }
  df[new_columns] <- NA
  return(df)
}
  
  
verify_control_dataframe <- function(new_data_df, legacy_data_df, control_data_type, ID_col, section, is_new){
  
  new_data_df <- Updated_cull_data_format
  legacy_data_df <- cull_legacy_df
  
  column_names <- colnames(legacy_data_df)
  verified_data_df <- data.frame(matrix(ncol = length(column_names), nrow = 0))
  colnames(verified_data_df) <- column_names 

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
      if(any(is.na(legacy_data_df[ID_col]))){
        legacy_data_df <- update_IDs(new_data_df, legacy_data_df, control_data_type)
        # create new dataframe without rows that have NA ID so that they can
        # be passed through the check functions as new row entries. 
        legacy_data_df <- legacy_data_df[is.na(legacy_data_df[ID_col]),]
        
      }
      
      
      #find perfect duplicates and add to verified data df
      perfect_duplicates <- inner_join(legacy_data_df, new_data_df)
      verified_data_df <- rbind(verified_data_df, perfect_duplicates)
      
      # Determine discrepancies and store both versions of the entries
      discrepancies_legacy <- anti_join(legacy_data_df, new_data_df)   
      discrepancies_new_indices <- which(new_data_df[[ID_col]] %in% discrepancies_legacy[[ID_col]])
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
      new_data_without_ID_df <- new_data_df[ , -which(names(new_data_df) %in% c(ID_col, "error_flag"))]
      legacy_data_without_ID_df <- legacy_data_df[ , -which(names(legacy_data_df) %in% c(ID_col, "error_flag"))]
      close_match_rows <- matrix_close_matches_vectorised(legacy_data_without_ID_df, new_data_without_ID_df, distance)
      
      seperated_close_matches <- separate_close_matches(close_match_rows)
      
      perfect_duplicates <- new_data_df[perfect_duplicate_new_indices,]
      verified_data_df <- rbind(verified_data_df, perfect_duplicates)
      
      discrepancies_legacy <- legacy_data_df[discrepancies_legacy_indices,]
      discrepancies_new <- new_data_df[discrepancies_new_indices,]
      new_entries <- unmatched_new_data_df[-discrepancies_new_indices,]
    }
    
    # new entries should not have been assigned a site if manta tow data. 
    # Although this test can only be used on one type of data it should never
    # fail. In any scenario that it does, a serious code review should be 
    # undertaken
    if(control_data_type == "manta_tow"){
      
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

vectorised_seperate_close_matches <- function(close_match_rows){
  # Separate the close matching rows with a vectorized process
  close_match_rows <- my_matrix 
  
  distance <- max(close_match_rows[,3])
  
  # initialise variables to store indices of rows after seperation
  discrepancies_indices <- matrix(,ncol=3, nrow =0)
  perfect_duplicate_indices <- matrix(,ncol=3, nrow =0)
  error_indices <- matrix(,ncol=3, nrow =0)
  check_indices <- matrix(,ncol=3, nrow =0)
  
  # extracts a vector of row indices pertaining too the two dataframes 
  x_indices <- close_match_rows[,1]
  y_indices <- close_match_rows[,2]
  
  # determine if there are duplicates of the row indices. Duplicates indicate
  # that there were multiple close matches within the specified distance to the 
  # row entry and therefore need to be handled differently to unique row indexs.
  # Utilises bitwise operations for speed.
  x_dup_indices <- (duplicated(x_indices)|duplicated(x_indices, fromLast=TRUE))
  y_dup_indices <- (duplicated(y_indices)|duplicated(y_indices, fromLast=TRUE))
  dup_indices <- y_dup_indices|x_dup_indices
  non_dup_indices <- !dup_indices
  
  # These operations seperate rows that only have one close match
  perfect_duplicate_indices <- rbind(perfect_duplicate_indices, close_match_rows[non_dup_indices & close_match_rows[,3] == 0,])
  discrepancies_indices <- rbind(discrepancies_indices, close_match_rows[non_dup_indices & close_match_rows[,3] > 0,])
  
  # remove rows that have already been handled to prevent double handling 
  close_match_rows_updated <- close_match_rows[dup_indices,]
  
  # Handle rows with a one to many relationship first to ensure no mismatch and
  # rows do not end up with no viable match. This will be an iterative process
  # with a Do-While type control structure. This process can make a mistake if 
  # a row has been changed enough that it no longer matches with its previous 
  # version (If non-database export and inherently no ID) but does match with 
  # another row. This chance of error will be minimized by iteratively finding 
  # matches by the smallest distance moving towards larger. 
  
  close_match_rows_updated <- (duplicated(close_match_rows_updated[,1])|duplicated(close_match_rows_updated[,1], fromLast=TRUE))
  y_updated_dup_indices <- (duplicated(close_match_rows_updated[,2])|duplicated(close_match_rows_updated[,2], fromLast=TRUE))
  
  # condition finds rows in `close_match_rows_updated` where only one column is a duplicate
  perfect_one_to_many <- !(y_updated_dup_indices & x_updated_dup_indices) & (y_updated_dup_indices | x_updated_dup_indices) & (close_match_rows_updated[,3] == 0)
  
  # Only select one match for a row
  perfect_one_to_many_matches <- close_match_rows_updated[perfect_one_to_many,]
  x_indices <- perfect_one_to_many_matches[,1]
  y_indices <- perfect_one_to_many_matches[,2]
  x_dup_indices <- (duplicated(x_indices)|duplicated(x_indices, fromLast=TRUE))
  y_dup_indices <- (duplicated(y_indices)|duplicated(y_indices, fromLast=TRUE))
  dup_indices <- (y_dup_indices | x_dup_indices)
  non_dup_indices <- !dup_indices
  
  perfect_duplicate_indices <- rbind(perfect_duplicate_indices, perfect_one_to_many_matches[non_dup_indices,])
  check_indices <- rbind(check_indices, perfect_one_to_many_matches[dup_indices,])
  
  # remove checked rows
  close_match_rows_updated <- close_match_rows_updated[!which(close_match_rows_updated[,1] %fin% x_indices) & !which(close_match_rows_updated[,2] %fin% y_indices),]

  # condition finds rows in `close_match_rows_updated` where only one column is a duplicate
  for(i in 1:distance){
    one_to_many <- !(y_dup_indices & x_dup_indices) & (y_dup_indices | x_dup_indices) & close_match_rows_updated[,3] == i
    nonperfect_one_to_many_matches <- close_match_rows_updated[one_to_many,]
    
    # Check that the same row is not being matched to multiple. If they are 
    # them and they will be handled at the end by being treated as new rows
    x_indices <- close_match_rows_updated[one_to_many,1]
    y_indices <- close_match_rows_updated[one_to_many,2]
    discrepancies_indices <- rbind(discrepancies_indices, perfect_one_to_many_matches[!(y_updated_dup_indices | x_updated_dup_indices),])
    check_indices <- rbind(check_indices, perfect_one_to_many_matches[(y_updated_dup_indices | x_updated_dup_indices),])
    
    # remove checked rows and any that have already been matched. 
    close_match_rows_updated <- close_match_rows_updated[!which(close_match_rows_updated[,1] %fin% x_indices) & !which(close_match_rows_updated[,2] %fin% y_indices),]
    
  }
  # close_match_rows_updated[!which(x_indices %fin% close_match_rows_updated[,1]) & !which(y_indices %fin% close_match_rows_updated[,2]),]
  
  # Handle rows with a many to many relationship.
  # First find many to many relationships where there exists a perfect match. 
  # There should no long be any one to many relationships that exist
  
  is_perfect_match <- close_match_rows_updated[,3] == 0
  x_updated_dup_indices <- (duplicated(close_match_rows_updated[,1])|duplicated(close_match_rows_updated[,1], fromLast=TRUE))
  y_updated_dup_indices <- (duplicated(close_match_rows_updated[,2])|duplicated(close_match_rows_updated[,2], fromLast=TRUE))
  many_to_many <- (y_updated_dup_indices & x_updated_dup_indices) 
  perfect_many_to_many <- many_to_many & is_perfect_match
  
  # given that these are perfect duplicates it is they are either genuine 
  # unique entries with the same values or accidental duplicates. It has been 
  # determined that 2 duplicates will be treated as genuine, more than 2 are 
  # mistakes 
  if(any(isTrue(perfect_many_to_many))){
    # this table will provide the frequency of indices in `close_match_rows_updated`
    # which can then be filtered by the number of times an index is present. 
    # duplicates of two can be added to the perfect duplicate matrix, any more 
    # and they will be added to error flag matrix.
    perfect_x_match_counts <- data.frame(table(close_match_rows_updated[perfect_many_to_many, 1]))
    perfect_y_match_counts <- data.frame(table(close_match_rows_updated[perfect_many_to_many, 2]))
    
    perfect_y_mistake_matches <- perfect_y_match_counts[perfect_y_match_counts[,1] > 2,2]
    perfect_x_mistake_matches <- perfect_x_match_counts[perfect_x_match_counts[,1] > 2,2]
    perfect_y_matches <- perfect_y_match_counts[perfect_y_match_counts[,1] == 2,2]
    perfect_x_matches <- perfect_x_match_counts[perfect_x_match_counts[,1] == 2,2]
    
    mistake_duplicates <- which(close_match_rows_updated[,2] %fin% perfect_y_mistake_matches) | which(close_match_rows_updated[,1] %fin% perfect_x_mistake_matches)
    true_duplicate_entries <- which(close_match_rows_updated[,2] %fin% perfect_y_matches) | which(close_match_rows_updated[,1] %fin% perfect_x_matches)
    perfect_duplicate_indices <- c(perfect_duplicate_indices, close_match_rows_updated[true_duplicate_entries,])
    error_indices <- rbind(error_indices, close_match_rows_updated[mistake_duplicates,])
    
    # remove all duplicates handled from the remaining data set
    close_match_rows_updated <- close_match_rows_updated[!mistake_duplicates & !true_duplicate_entries,]
    
  }
  
  # Only many-to-many non perfect matches are left and need to be handled. 
  for(i in 1:distance){
    many_to_many <- (y_dup_indices & x_dup_indices) & close_match_rows_updated[,3] == i
    nonperfect_one_to_many_matches <- close_match_rows_updated[one_to_many,]
    
    # Check that the same row is not being matched to multiple. If they are 
    # them and they will be handled at the end by being treated as new rows
    x_indices <- close_match_rows_updated[many_to_many,1]
    y_indices <- close_match_rows_updated[many_to_many,2]
    
    perfect_x_match_counts <- data.frame(table(x_indices))
    perfect_y_match_counts <- data.frame(table(y_indices))
    
    discrepancy_y_matches <- perfect_y_match_counts[perfect_y_match_counts[,1] == 1,2]
    discrepancy_x_matches <- perfect_x_match_counts[perfect_x_match_counts[,1] == 1,2]
    dup_y_matches <- perfect_y_match_counts[perfect_y_match_counts[,1] > 1,2]
    dup_x_matches <- perfect_x_match_counts[perfect_x_match_counts[,1] > 1,2]
    
    multiple_matches <- which(close_match_rows_updated[,2] %fin% dup_y_matches) | which(close_match_rows_updated[,1] %fin% dup_x_matches)
    correct_match <- which(close_match_rows_updated[,2] %fin% discrepancy_y_matches) | which(close_match_rows_updated[,1] %fin% discrepancy_x_matches)
    
    discrepancies_indices <- rbind(discrepancies_indices, close_match_rows_updated[correct_match,])
    check_indices <- rbind(check_indices, close_match_rows_updated[multiple_matches,])
    
    # remove checked rows and any that have already been matched. 
    close_match_rows_updated <- close_match_rows_updated[!correct_match & !multiple_matches,]
  }
  
  # check for mistakes 
  is_mistake_present <- check_for_mistake()
  
  #INCLUDE METHOD TO FIX PROBLEM HERE 
  if(is_mistake_present){
    
  }
  
  output <- list(discrepancies_indices, perfect_duplicate_indices, error_indices, check_indices)
  return(output)
}

check_for_mistake <- function(discrepancies_indices, perfect_duplicate_indices, error_indices, check_indices){
  duplicated_vec <- base::Vectorize(duplicated)
  
  # Check that there are no duplicates between data frames
  x_indices_intersecting <- Reduce(intersect, list(discrepancies_indices[,1], perfect_duplicate_indices[,1], error_indices[,1], check_indices[,1]))
  y_indices_intersecting <- Reduce(intersect, list(discrepancies_indices[,2], perfect_duplicate_indices[,2], error_indices[,2], check_indices[,2]))
  
  # Check each column contains no duplicates 
  discrepancies_indices_dup <- (duplicated_vec(t(discrepancies_indices))|duplicated_vec(t(discrepancies_indices), fromLast=TRUE))
  perfect_duplicate_indices_dup <- (duplicated_vec(t(perfect_duplicate_indices))|duplicated_vec(t(perfect_duplicate_indices), fromLast=TRUE))
  error_indices_dup <- (duplicated_vec(t(error_indices))|duplicated_vec(t(error_indices), fromLast=TRUE))
  check_indices_dup <- (duplicated_vec(t(check_indices))|duplicated_vec(t(check_indices), fromLast=TRUE))
  
  is_intersecting_dups <- (length(y_indices_intersecting) == 0) & (length(x_indices_intersecting == 0))
  return(is_intersecting_dups | discrepancies_indices_dup | perfect_duplicate_indices_dup | error_indices_dup | check_indices_dup)
  
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

# Re-write base operator %in% faster
`%fin%` <- function(x, table) {
  stopifnot(require(fastmatch))
  fmatch(x, table, nomatch = 0L) > 0L
}

find_close_matches <- function(x, y, distance){
  # Find list of all close matches between rows in x and y within a specified 
  # distance. This distance is the number of non perfect column matches within
  # a row. returns a list of lists. Each is a vector containing the indices of
  # the rows matched and the distance from perfect. (X_index, Y_index, Distance)
  all_matches <- list()
  row_length <- length(y[i,])
  for(z in 1:nrow(x)){ 
    matches <- list()
    for(i in 1:nrow(y)){
      match_length <- length(na.omit(match(x[z,], y[i,])))
      if(match_length >= (row_length - distance)){
        match <- c(z, i,length(y[1,]) - match_length)
        matches[[length(matches)+1]] <- match
      }
    }
    if(length(matches) > 0){
      filtered_matches <- lapply(matches, function(a) Filter(Negate(is.null), a))
      all_matches[[z]] <- filtered_matches
    }
    
  }
  filtered_matches <- lapply(all_matches, function(a) Filter(Negate(is.null), a))
  
  # Currently not necessary but it may be desirable to have a single list of 
  # vectors rather than a list of lists
  # filtered_matches <- unlist(filtered_matches, recursive = FALSE)
  return(all_matches)
}

matrix_close_matches_vectorised <- function(x, y, distance){
  # Find list of all close matches between rows in x and y within a specified 
  # distance. This distance is the number of non perfect column matches within
  # a row. returns a the indices of the rows matched and the distance from
  # perfect. (X_index, Y_index, Distance). Pre-allocates memory for the matrix
  # assuming worst case scenario or maximum allocation possible. The operations 
  # would not be possible if this fails and is still faster than 
  # dynamically updating an object. 
  
  #Pre-allocate variables and memory
  x_rows <- nrow(x)
  x_cols <- ncol(x)
  y_rows <- nrow(y)
  
  num_rows <- y_rows*x_rows
  if(num_rows > 1000000000){
    num_rows <- 1000000000 
  }
  match_indices <- try(matrix(data=NA, nrow=num_rows, ncol=3))
  if(class(num_rows)[[1]]=='try-error'){
    num_rows <- 10000000
    match_indices <- matrix(data=NA, nrow=num_rows, ncol=3)
  }
  index <- 1
  
  # Iterate through each row in matrix/dataframe x and evaluate for each value 
  # in the row whether it matches the column of y. This vector of logical values
  # are then appeneded to the `matches` matrix. After Iterating over every 
  # column there will be a matrix of size (y_rows, x_cols). A perfect matching 
  # row in y will have a corresponding row in `matches` exclusively containing 
  # TRUE. Given TRUE = 1, a row sum can be utilised to determine the distance 
  # from a perfect match. Can then use a customised vectorised function to 
  # append matches to match_indices. 
  matches <- matrix(data=NA, nrow=y_rows, ncol=x_cols)
  for(z in 1:x_rows){ 
    for(i in 1:x_cols){
      matches[,i] <- x[z,i] == y[,i]
    }
    matches <- !(matches)
    num_matches <- rowSums(matches, dims = 1)
    num_matches <- ifelse(num_matches <= distance, num_matches, NA)
    nonNA <- which(!is.na(num_matches))
    nonNAvalues <- num_matches[nonNA]
    if(length(match) > 1){
      match <- store_index_vec(nonNAvalues, nonNA, z)
      match <- t(match)
      match_indices[index:(index+nrow(match)-1),] <- match
      index <- index + nrow(match)
    }
  }
  match_indices <- na.omit(match_indices)
  return(match_indices)
}

store_index_vec <- base::Vectorize(store_index)
store_index <- function(nonNAvalues, nonNA, z){
  match_indices <- c(z, nonNA, nonNAvalues)
  return(match_indices)
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
  
  # acquire the column names of the control data passed as an argument of this 
  # function and make sure they match the control data column names in the 
  # lookup table that specifies which datatype every column should be.
  
  column_names <- colnames(data_df)
  setDataType_df <- read.csv("setDataType.csv", header = TRUE)
  
  columns <- setDataType_df$column
  matched_output <- match_vector_entries(columns, column_names, "Set Data Type", correct_order = TRUE) # This returns the matching column names in the original order
  
  matched_column_names <- matched_output[[1]]
  matched_column_indices<- matched_output[[2]]
  
  # check that both sets of column names are still the same length after the 
  # matching
  if(length(columns) != length(column_names)){
    # TODO contribute to meta data report. This is not necessarily a fatal error 
    # if the data is saved or parsed as the correct type. 
    
  }
  
  # create list of column name partials grouped by desired data type. The data 
  # types will be utilised as list names. 
  matched_data_types <- setDataType_df$dataType[matched_column_indices]
  dataTypes <- c("Integer", "Numeric", "Date", "Character")
  setDataTypeList <- lapply(dataTypes, function(x) matched_column_names[which(x == matched_data_types)])
  names(setDataTypeList) <- dataTypes 
  
  # compare column names retrieved from lookup table to column names in the 
  # in the dataframe. Check and find closest matches. sets the data types for
  # all columns 
  for(i in dataTypes){
    columns <- setDataTypeList[[i]]
    if(i == "Numeric"){
      for(x in columns){data_df[[x]] <- as.numeric(data_df[[x]])}
    } else if (i == "Date") {
      for(x in columns){
        if(is.character(data_df[[x]][1])){
          data_df[[x]] <- parse_date_time(data_df[[x]], orders = c('dmy', 'ymd'))
        }
      }
    } else if (i == "Integer") {
      for(x in columns){data_df[[x]] <- as.integer(data_df[[x]])}
    } else if (i == "Character"){
      for(x in columns){data_df[[x]] <- as.character(data_df[[x]])}
    }
  }
  return(data_df)
  # } else if (i == "Logical"){
  #   apply(columns, function(x) data_df[x] <- as.logical(data_df[x]))
  # } else if (i == "Time") {
  #   apply(columns, function(x) data_df[x] <- as.time(data_df[x]))
} 


# Custom Boolean Or function written in C for speed. Works exactly the same as 
# base R operator | however NA is considered TRUE.
or4 <- cfunction(c(x="logical", y="logical"), "
    int nx = LENGTH(x), ny = LENGTH(y), n = nx > ny ? nx : ny;
    SEXP ans = PROTECT(allocVector(LGLSXP, n));
    int *xp = LOGICAL(x), *yp = LOGICAL(y), *ansp = LOGICAL(ans);
    for (int i = 0, ix = 0, iy = 0; i < n; ++i)
    {
        *ansp++ = xp[ix] || yp[iy];
        ix = (++ix == nx) ? 0 : ix;
        iy = (++iy == ny) ? 0 : iy;
    }
    UNPROTECT(1);
    return ans;
")


#create matrix of warnings so they are added to the specified XML node 
# in the metadata report in a vectorised manor. 
# warnings <- names(warnings())
# warnings_matrix <- matrix(warnings, 1,length(warnings))
# contribute_to_metadata_report(control_data_type, paste("Set Data Type", i), warnings_matrix)

match_vector_entries <- function(current_vec, target_vec, section, check_mapped = FALSE, correct_order = FALSE){
  # Compare any form two vectors and identify matching entries.
  
  out <- tryCatch(
    {
      
      # clean vector entries for easy comparison. The cleaning is done in this 
      # specific order to remove characters such as '.' that appear after
      # removing spaces or specific character from text in a CSV. 
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
      maxium_levenshtein_distance <- 4
      
      # determine the current column names and indices that match a column name 
      # in the legacy format. Count how many match. 
      matching_entries <- intersect(clean_current_vec, clean_target_vec)
      matching_entries_length <- length(matching_entries)
      perfect_matching_entries <- intersect(current_vec, target_vec)
      perfect_matching_entries_length <- length(perfect_matching_entries)
      matching_target_entries_indices <- which(clean_target_vec %in% matching_entries)
      matching_current_entries_indexes <- which(clean_current_vec %in% matching_entries)
      
      # conditional statements to be passed to error handling so a more detailed 
      # description of any failure mode can be provided. 
      is_not_matching_entries <- !((matching_entries_length == length(clean_current_vec)) || (matching_entries_length == length(clean_target_vec)))
      is_not_perfect_match <- !((perfect_matching_entries_length == length(current_vec)) || (perfect_matching_entries_length == length(target_vec)))
      
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
        
        if(check_mapped){
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
              clean_mapped_name <- gsub('[[:punct:] ]+',' ', mapped_output[x])
              clean_mapped_name <- gsub(' ', '', clean_mapped_name)
              clean_mapped_name <- gsub('\\.', '', clean_mapped_name)
              clean_mapped_name <- gsub('[)(/&]', '', clean_mapped_name)
              clean_mapped_name <- tolower(clean_mapped_name)
              clean_current_vec[mapped_index] <- clean_mapped_name
              current_vec[mapped_index] <- mapped_output[x]
            }
            nonmatching_entries <- nonmatching_entries[-mapped_name_indices]
            nonmatching_current_entry_indices <- nonmatching_current_entry_indices[-mapped_name_indices]
          }
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
            clean_current_vec[i] <- clean_target_vec[partial_name_matches]
            current_vec[i] <- target_vec[partial_name_matches]
          } else if ((length(closest_matching_indices) == 1) & (minimum_distance < maxium_levenshtein_distance)) {
            clean_current_vec[i] <- clean_target_vec[closest_matching_indices]
            current_vec[i] <- target_vec[closest_matching_indices]
          } 
        }
      } 
      
      # find list of vector of indices which indicate the position of current columns names in the legacy format. 
      # this will be used to indicate if at the end of the mapping and matching process, the program was able to 
      # correctly find all required columns. This will also then be utilised to rearrange the order of the columns
      correct_order_indices <- sapply(clean_target_vec, function(x) match(x, clean_current_vec))
      correct_order_indices <- correct_order_indices[!is.na(correct_order_indices)]
      
      original_order_indices <- sapply(clean_current_vec, function(x) match(x, clean_target_vec))
      original_order_indices <- original_order_indices[!is.na(original_order_indices)]
     
      # This means the vector of strings can be returned in the original order if 
      # it only important that the strings themselves match. Alternatively, the
      # vector of strings can also be returned in the same order. A vector of 
      # indices will be returned indicating the correct order of the input vector 
      # if needed at a later date. This also accounts for all perfect matches of 
      # the cleaned strings but not the actual string.
      
      if(correct_order & !is_not_matching_entries){
        current_vec <- c()
        clean_current_vec_filtered <- clean_current_vec[correct_order_indices]
        for(i in clean_current_vec_filtered){
          matches <- match(i, clean_target_vec)
          matches <- matches[!is.na(matches)]
          if(length(matches) == 1){
            current_vec[matches] <- target_vec[matches]
          }
        }
      } else if(!correct_order & !is_not_matching_entries) {
        current_vec <- target_vec[original_order_indices]
      } else if (correct_order & is_not_matching_entries){
        current_vec <- current_vec[correct_order_indices]
      } 
      
      # Indices should be unique and not NA. Check multiple columns weren't 
      # matched to the same column. The appropriate cut off distance may need 
      # to be tweaked overtime. For metadata report.
      duplicate_column_indices <- duplicated(current_vec)
      is_matching_indices_unique <- !any(duplicated(closest_matching_indices))
      is_column_name_na <- any(is.na(current_vec))
      
      # For metadata report.
      updated_matching_entries <- current_vec[correct_order_indices]
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
      # contribute_to_metadata_report(control_data_type, section, warnings_matrix)
      # contribute_to_metadata_report(control_data_type, section, metadata)
      
      output <- list(current_vec, correct_order_indices, original_order_indices, metadata)
      return(output)
      
    },
    error=function(cond) {
      contribute_to_metadata_report(control_data_type, section, comments)
      
    }
  )
}







