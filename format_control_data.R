# Format the new control data into the stardard legacy format 


import_data <- function(data, control_data_type, has_authorative_ID, sheet=1){
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
        
        # remove rows containing mostly NA, "" or NULL values 
        data_df <- data_df[rowSums(is.na(data_df) | data_df == "" | is.null(data_df)) < (ncol(data_df) - 2), ]
        
        # create matrix of warnings so they are added to the specified XML node 
        # in the metadata report in a vectorised mannor. 
       
        # warnings <- names(warnings())
        # warnings_matrix <- matrix(warnings, 1,length(warnings))
        # contribute_to_metadata_report(control_data_type, "Import", warnings_matrix)
        
        
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
      
      # Add required column names to legacy dataframe that no longer exist in 
      # the current version.
      current_df[,add_required_columns(control_data_type, has_authorative_ID)] <- NA
      
      # acquire column names and ensure they are formatted as characters
      current_col_names <- colnames(current_df)
      legacy_col_names <- colnames(legacy_df)
      
      # compare the column names of the legacy and current format
      matched_vector_entries <- match_vector_entries(current_col_names, legacy_col_names, control_data_type, correct_order = FALSE, check_mapped = TRUE)
      matched_col_names <- matched_vector_entries[[1]]
      matching_entry_indices <- matched_vector_entries[[2]]
      
      # rearrange columns and update the current dataframe column names with the 
      # vector found above.
      updated_current_df <- current_df[,matching_entry_indices] 
      colnames(updated_current_df) <- matched_col_names[matching_entry_indices]
      
      #Set default values of new columns based on records in file
      updated_current_df <- set_default_values(updated_current_df, control_data_type, has_authorative_ID)
      
      # set the data type of all entries to ensure that performed operations have 
      # expected output. 
      updated_current_df <- set_data_type(updated_current_df, control_data_type) 
      
      return(updated_current_df)
    },
    error=function(cond) {
      contribute_to_metadata_report(control_data_type, matrix(cond))
     
    }
  ) 
  
} 


set_default_values <- function(data_df, control_data_type, has_authorative_ID){
  lookup <- read.csv("additionalColumns.csv", header = TRUE)
  new_columns <- lookup[((lookup$type == control_data_type) & (lookup$is_mandatory == 1)), c("addition", "default")]
  for(i in new_columns[["addition"]]){
    data_df[,i] <- new_columns[new_columns[["addition"]]==i,"default"]
  }
  
  return(data_df)
}


create_metadata_report <- function(control_data_type){
    #Generate the XML template for the control data process report.
    reports_location <<- "reports\\"
    if (!dir.exists(reports_location)) {
      dir.create(reports_location)
    }
    
    #create file name systematically
    date <- as.character(Sys.Date())
    timestamp <- as.character(Sys.time())
    filename <- paste0(reports_location, "Processed Control Data Report ", date, control_data_type, ".xml", sep="")
    
    #generate template
    template <- xml_new_root("session") 
    control_data <- xml_find_all(template, "//session")
    xml_add_child(control_data, "timestamp", timestamp) 
    xml_add_child(control_data, "Notifications")
    write_xml(template, file = filename, options =c("format", "no_declaration"))
}


contribute_to_metadata_report <- function(control_data_type, data, key = "Warning"){
  # Finds desired control data node and adds a section from the information 
  # obtained in the previously executed function. 

  # finds files with current date in file name and attempts to open xml file
  file_count <- 1
  reports_location <- "reports\\"
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
  node <- xml_find_all(xml_file_data, "//Notifications")
  
  # Only add to node if it exists. Node Should always exist. 
  # A dataframe will add a key value pair for each column. A single value will  
  # directly assigned as a value. Columns with Multiple entries will form a list
  # before being assigned as a value. 
  # A matrix will pair each entry with the specified key. 
  if(length(node) > 0){
    if(is.data.frame(data)){
      xml_add_child(node, key ,data)
    } else {
      desired_nodes <- xml_find_all(xml_file_data, "//Notifications")
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


map_column_names <- function(column_names, control_data_type){
  lookup <- read.csv("mapNames.csv", header = TRUE)
  lookup <- lookup[lookup$control_data_type == control_data_type,]
  mapped_names <- lapply(column_names, function(x) lookup$target[match(x, lookup$current)])
  return(mapped_names)
}
  
add_required_columns <- function(control_data_type, has_authorative_ID){
  lookup <- read.csv("additionalColumns.csv", header = TRUE)
  new_columns <- lookup[((lookup$type == control_data_type) & (lookup$is_mandatory == 1)), 1]
  return(new_columns)
}
  
  
verify_control_dataframe <- function(new_data_df, legacy_data_df, control_data_type, section, is_new){
  
  new_data_df <- Updated_data_format
  legacy_data_df <- legacy_df
  
  ID_col <- colnames(new_data_df)[1]
  
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
  
  #verify data and flag if there are any errors 
  if(is_new){
    legacy_data_df <- verify_entries(legacy_data_df, control_data_type, ID_col) 
  }
  new_data_df <- verify_entries(new_data_df, control_data_type, ID_col)
  
  # If there is a unique ID then perfect duplicates can easily be removed.
  if(has_authorative_ID){
    
    # Find close matches of rows left without an ID and no perfect duplicates. 
    # Rows with a single close match will be considered a discrepancy and then
    # the ID checked against the all the IDs in legacy_data_df to ensure it does 
    # not already exist.
    
    # Although this system is capable of handling information without an 
    # authoritative ID it is bad practice to attempt to update or alter any
    # authoritative Ids as there is no way of guaranteeing that the ID is being 
    # assigned to the correct data entry. Therefore, the system will use data 
    # with no entries until the next database export where all data without IDs 
    # will be removed. This ensures that the authoritative IDs remain 
    # authoritative. 
    if(any(is.na(legacy_data_df[ID_col]))){
      # create new dataframe without rows that have NA ID so that they can
      # be passed through the check functions as new row entries. 
      legacy_data_df <- legacy_data_df[is.na(legacy_data_df[ID_col]),]
      
    }
    
    # Determine additional columns required by the new data format and remove 
    # from comparison
    required_columns <- add_required_columns(control_data_type, has_authorative_ID)
    required_columns <- c(required_columns, "error_flag")
   
    # save original dataframes for legacy and new data so it can be manipulated
    # without loosing data. Columns will be removed that are not going to be  
    # compared for similarity. A unique identifier for each row will be created 
    # based on data in the row and compared. 
    temp_legacy_df <- legacy_data_df[,-which(colnames(legacy_data_df) %in% required_columns)]
    temp_new_df <- new_data_df[,-which(colnames(new_data_df) %in% required_columns)]
   
    # Create a unique identifier for each row in legacy_data_df and new_data_df
    temp_legacy_df$Identifier <- apply(temp_legacy_df, 1, function(row) paste(row, collapse = "_"))
    temp_new_df$Identifier <- apply(temp_new_df, 1, function(row) paste(row, collapse = "_"))
    
    #find perfect duplicates and add to verified data df
    perfect_duplicates <- legacy_data_df[temp_legacy_df$Identifier %in% temp_new_df$Identifier, ]
    verified_data_df <- rbind(verified_data_df, perfect_duplicates)
    
    # remove identifier columns
    temp_legacy_df$Identifier <- NULL
    temp_new_df$Identifier <- NULL
    
    # find new entries based on whether the ID is present in both dataframes 
    new_entries <- anti_join(new_data_df, legacy_data_df, by=ID_col)
    
    # Determine discrepancies and store both versions of the entries
    non_discrepancy_indices <- c(perfect_duplicates[[ID_col]], new_entries[[ID_col]])
    discrepancies_new <- new_data_df[!(new_data_df[[ID_col]] %in% non_discrepancy_indices),]
    discrepancies_legacy <- legacy_data_df[!(legacy_data_df[[ID_col]] %in% non_discrepancy_indices),]
  
    # New entries need to be checked for duplicates. If there is more than one
    # duplicate it can be assumed to be an error and the error flag set. This 
    # will use a similar identifier new_entries df. This will only flag the 
    # duplicate versions of the row as an error as it still contains new 
    # information there has just been multiple instances of data entry. 
    # Additionally no new entry should be a duplicate of any "perfect duplicate" 
    # as legitimate duplicates would come from the same source and uploaded at 
    # the same time.
    new_entries$Identifier <- apply(new_entries[,2:ncol(new_entries)], 1, function(row) paste(row, collapse = "_"))
    duplicates <- duplicated(new_entries$Identifier)
    counts <- ave(duplicates, new_entries$Identifier, FUN = sum)
    new_entries$error_flag <- ifelse(counts >= 3 & duplicates, 1, new_entries$error_flag)
    perfect_duplicates$Identifier <- apply(perfect_duplicates[,2:ncol(perfect_duplicates)], 1, function(row) paste(row, collapse = "_"))
    new_entries$error_flag <- ifelse(new_entries$Identifier %in% perfect_duplicates$Identifier, 1, new_entries$error_flag)
    
    new_entries$Identifier <- NULL
    perfect_duplicates$Identifier <- NULL
    
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
  
    # Determine additional columns required by the new data format and remove 
    # from comparison
    required_columns <- add_required_columns(control_data_type, has_authorative_ID)
    required_columns <- c(required_columns, "error_flag", ID_col)
    
    # find close matching rows (distance of two) based on all columns except ID. ID is not 
    # because it will always be null if the data is exported from powerBI. 
    distance <- 2
    
    temp_new_df <- new_data_df[ , -which(names(new_data_df) %in% required_columns)]
    temp_legacy_df <- legacy_data_df[ , -which(names(legacy_data_df) %in% required_columns)]
    close_match_rows <- matrix_close_matches_vectorised(temp_legacy_df, temp_new_df, distance)
    
    seperated_close_matches <- vectorised_seperate_close_matches(close_match_rows)
    
    perfect_duplicates <- new_data_df[seperated_close_matches$perfect[,2],]
    discrepancies_legacy <- legacy_data_df[seperated_close_matches$discrepancies[,1],]
    discrepancies_new <- new_data_df[seperated_close_matches$discrepancies[,2],]
    new_entries <- rbind(new_data_df[seperated_close_matches$new[,2],], new_data_df[seperated_close_matches$check[,2],])
    verified_data_df <- rbind(verified_data_df, perfect_duplicates)
  }
  
  # Given that it is not possible to definitively know if a change / discrepancy 
  # was intentional or not both new and change entries will pass through the 
  # same validation checks and if passed will be accepted as usable and assumed to be . If failed, 
  # assumed to be a QA change. If failed,  the data will be flagged. Failed 
  # discrepancies will check the original legacy entry, which if failed will 
  # be left as is. 
 
  verified_discrepancies <- compare_discrepancies(discrepancies_new, discrepancies_legacy, control_data_type, ID_col)
  verified_data_df <- rbind(verified_data_df, verified_discrepancies)
  verified_data_df <- rbind(verified_data_df, new_entries)
  
  # merge the verified dataset
  return(verified_data_df)
  
}


compare_discrepancies <- function(discrepancies_new, discrepancies_legacy, control_data_type, ID_col){
  
  if(has_authorative_ID){
    
    correct_new_entry_IDs <- discrepancies_new[discrepancies_new$error_flag == 0, ID_col]
    output <- discrepancies_new[discrepancies_new[[ID_col]] %in% correct_new_entry_IDs,]
    discrepancies_new <- discrepancies_new[!(discrepancies_new[[ID_col]] %in% correct_new_entry_IDs),]
    discrepancies_legacy <- discrepancies_legacy[!(discrepancies_legacy[[ID_col]] %in% correct_new_entry_IDs),]
    
    correct_legacy_entry_IDs <- discrepancies_new[discrepancies_legacy$error_flag == 0, ID_col]
    output <- rbind(output, discrepancies_legacy[discrepancies_legacy[[ID_col]] %in% correct_legacy_entry_IDs,])
    discrepancies_new <- discrepancies_new[!(discrepancies_new[[ID_col]] %in% correct_legacy_entry_IDs),]
    discrepancies_legacy <- discrepancies_legacy[!(discrepancies_legacy[[ID_col]] %in% correct_legacy_entry_IDs),]
    output <- rbind(output, discrepancies_new)
  }
  
  return(output)
  
}


vectorised_seperate_close_matches <- function(close_match_rows){
  # Separate the close matching rows with a vectorized process. Duplicates in 
  # columns of `close_match_rows` indicates that there are multiple close 
  # matches between a row(s) in dataframe and and row(s) in y. This process 
  # utilised logical checks on vectors or matrices so Boolean operations can be 
  # utilsied to separate the rows. Although this does reduce readability, the 
  # reduced computational time of two or three orders of magnitude was deemed a 
  # worthy trade off. 
  
  # Order of matches handled
  # 1) Handle rows with one-one close matches 
  # 2) Handle rows with many-many perfect matches 
  # 3) Handle rows with one-many perfect matches
  # 4) Handle rows with one-many nonperfect matches
  # 5) Handle rows with many-many nonperfect matches
  # 6) Handle rows with one-one and one-many nonperfect matches
  
  # This order MATTERS. This is NOT a definitive process and therefore the order
  # needs to maximize the probability that a row from the new data set is 
  # matched with one from the previous data set. One to one matches and
  # many-many perfect matches are the most likely to be correct and therefore 
  # are removed first. It is important that the next matches handled are
  # one-many. This is to ensure a match is found for the "one", as its most 
  # likely match is one of the many with the smallest distance. Any rows with 
  # those indices can then be removed to prevent double handling. Once many-many 
  # rows have been handled, one-one or one-many relationships may have been 
  # formed and therefore can be handled repetitively until all matches have been 
  # found or no more can be found
  
  
  ### ---------- 
  #Initialise variables 
  
  x_df_col <- 1
  y_df_col <- 2
  distance <- max(close_match_rows[,3])
  
  # initialise variables to store indices of rows after seperation
  discrepancies_indices <<- matrix(,ncol=3, nrow =0)
  perfect_duplicate_indices <<- matrix(,ncol=3, nrow =0)
  error_indices <<- matrix(,ncol=3, nrow =0)
  check_indices <<- matrix(,ncol=3, nrow =0)
  

  
  ### ---------- 
  #one-one close matches
  
  close_match_rows_updated <- find_one_to_one_matches(close_match_rows)
  
  ### ---------- 
  #many-many perfect matches
  
  # Boolean logic to subset many-many perfect matches from the `close_match_rows`
  # matrix
  is_perfect_match <- close_match_rows_updated[,3] == 0
  x_updated_dup_indices <- (duplicated(close_match_rows_updated[,x_df_col])|duplicated(close_match_rows_updated[,x_df_col], fromLast=TRUE))
  y_updated_dup_indices <- (duplicated(close_match_rows_updated[,y_df_col])|duplicated(close_match_rows_updated[,y_df_col], fromLast=TRUE))
  many_to_many <- (y_updated_dup_indices & x_updated_dup_indices) 
  many_to_many_with_perfect_match <- many_to_many & is_perfect_match
  
  # many-many with perfect matches have three possible scenarios. 
  # x_df_col) A single perfect matching row with multiple close matches
  # 2) Two perfect matching rows (This is considered coincidental and both are
  #     treated as unique)
  # 3) more than two perfect matching rows (This is considered a mistake and 
  #     flagged)
  # All scenarios need to be checked against.
  if(any(many_to_many_with_perfect_match)){
    
    many_to_many_with_perfect_match_entries <- close_match_rows_updated[many_to_many_with_perfect_match,]
    
    x_dup_indices <- (duplicated(many_to_many_with_perfect_match_entries[,x_df_col])|duplicated(many_to_many_with_perfect_match_entries[,x_df_col], fromLast=TRUE))
    y_dup_indices <- (duplicated(many_to_many_with_perfect_match_entries[,y_df_col])|duplicated(many_to_many_with_perfect_match_entries[,y_df_col], fromLast=TRUE))
    
    
    many_to_many_i <- x_dup_indices & y_dup_indices
    one_to_many_i <- !(x_dup_indices & y_dup_indices) & (y_dup_indices | x_dup_indices)
    one_to_one_i <- !(x_dup_indices | y_dup_indices)
    
    
    many_to_many_e <- many_to_many_with_perfect_match_entries[many_to_many_i,]
    one_to_many_e <- many_to_many_with_perfect_match_entries[one_to_many_i,]
    one_to_one_e <- many_to_many_with_perfect_match_entries[one_to_one_i,]
    
    # separate groups of many to many matches. There should not be one to many 
    # relationship of perfect matches, if there is there are a combination of 
    # discrepancies which will not be able to be definitively determined and 
    # therefore will be set as new entries. 
    
    if(nrow(many_to_many_e)){
      
      # many_to_many_e <- mat
      my_df_colm_split <- lapply(split(many_to_many_e[,x_df_col:y_df_col], many_to_many_e[,y_df_col]), matrix, ncol=y_df_col)
     
      groups <- replicate(length(my_df_colm_split), vector(), simplify = FALSE)
      group <- 0
      output_rec_group <- rec_group(my_df_colm_split, groups, group)
      
      # Now that the many to many perfect matches are grouped, if they are
      # genuine perfect matches with no mistakes there will be as many duplicate
      # vectors as their are matching rows. Any with more than 2 matching rows 
      # as previously discussed will be removed and considered mistake 
      # duplicates. A pair of perfect duplicates will be considered a 
      # coincidence and kept 
      
      perfect_matching_many_to_many_rows <- (duplicated(output_rec_group)|duplicated(output_rec_group, fromLast=TRUE))
      too_many_matches <- unlist(lapply(output_rec_group, function(x) length(x) > 2))
      too_many_matches_indices <- unique(unlist(output_rec_group[too_many_matches]))
      
      too_few_matches <- unlist(lapply(output_rec_group, function(x) length(x) < 2))
      too_few_matches_indices <- unique(unlist(output_rec_group[too_few_matches]))
      
      correct_many_to_many <- output_rec_group[perfect_matching_many_to_many_rows & !too_few_matches & !too_many_matches]
      
      # Identify unique vectors
      unique_vectors <- unique(correct_many_to_many)
      
      # Count frequency of each unique vector
      freq_table <- table(match(correct_many_to_many, unique_vectors))
      
      correct_freq <- unique_vectors[as.numeric(names(freq_table[freq_table == 2]))]
      incorrect_freq <- unique_vectors[as.numeric(names(freq_table[freq_table != 2]))]
      
      correct_freq_indices <- unique(c(unlist(correct_freq)))
      incorrect_freq_indices <- unique(c(unlist(incorrect_freq)))
      
    }
    # update the relevant matrices if matches are found. The indices with 
    # multiple perfect matches may also have other matches that are nonperfect 
    # and therefore need to be filtered again to ensure only perfect is 
    # considered
    if(sum(perfect_y_mistake_matches, perfect_x_mistake_matches, perfect_y_matches, perfect_x_matches, perfect_single_y_matches, perfect_single_x_matches) > 0){
      
     
      mistake_duplicate_manye_indices <- unique(c(which((many_to_many_e[,y_df_col] %fin% too_many_matches_indices)), which((many_to_many_e[,y_df_col] %fin% too_few_matches_indices)), which((many_to_many_e[,y_df_col] %fin% incorrect_freq_indices))))
      mistake_duplicate_close_match_rows_indices <- unique(c(which((close_match_rows_updated[,y_df_col] %fin% too_many_matches_indices)), which((close_match_rows_updated[,y_df_col] %fin% too_few_matches_indices)), which((close_match_rows_updated[,y_df_col] %fin% incorrect_freq_indices))))
      mistake_duplicates <- many_to_many_e[mistake_duplicate_manye_indices,]
      many_to_many_e <- many_to_many_e[-mistake_duplicate_manye_indices,]
      many_to_many_indices <- unique(c(which(close_match_rows_updated[,y_df_col] %fin% correct_freq_indices)))
      
      one_to_many_indices <- unique(c(which(close_match_rows_updated[,y_df_col] %fin% one_to_many_e[,y_df_col]), which(close_match_rows_updated[,x_df_col] %fin% one_to_many_e[,x_df_col])))
      x_dup_one_to_many_indices <- (duplicated(one_to_many_e[,x_df_col])|duplicated(one_to_many_e[,x_df_col], fromLast=TRUE))
      y_dup_one_to_many_indices <- (duplicated(one_to_many_e[,y_df_col])|duplicated(one_to_many_e[,y_df_col], fromLast=TRUE))
      correct_one_to_many_e <- one_to_many_e[!(x_dup_one_to_many_indices | y_dup_one_to_many_indices),]
      incorrect_one_to_many_e <- one_to_many_e[(x_dup_one_to_many_indices | y_dup_one_to_many_indices),]
      
      one_to_one_indices <- unique(c(which(close_match_rows_updated[,y_df_col] %fin% one_to_one_e[,y_df_col]), which(close_match_rows_updated[,x_df_col] %fin% one_to_one_e[,x_df_col])))
      
      perfect_duplicate_indices <- rbind(perfect_duplicate_indices, one_to_one_e, correct_one_to_many_e, many_to_many_e)
      error_indices <- rbind(error_indices, mistake_duplicates, incorrect_one_to_many_e)
      
      # remove rows that have already been handled to prevent double handling.
      close_match_rows_updated <- close_match_rows_updated[-unique(c(one_to_one_indices, one_to_many_indices, many_to_many_indices, mistake_duplicate_close_match_rows_indices)),]
    }
  }
  
  ### ---------- 
  #Re-check one-one close matches
  close_match_rows_updated <- find_one_to_one_matches(close_match_rows_updated)
  
  ### ---------- 
  #one-many perfect matches
  
  x_updated_dup_indices <- (duplicated(close_match_rows_updated[,x_df_col])|duplicated(close_match_rows_updated[,x_df_col], fromLast=TRUE))
  y_updated_dup_indices <- (duplicated(close_match_rows_updated[,y_df_col])|duplicated(close_match_rows_updated[,y_df_col], fromLast=TRUE))
  
  # condition finds rows in `close_match_rows_updated` where only one column is a duplicate
  perfect_one_to_many <- !(y_updated_dup_indices & x_updated_dup_indices) & (y_updated_dup_indices | x_updated_dup_indices) & (close_match_rows_updated[,3] == 0)
  
  # Only select one match for a row
  perfect_one_to_many_matches <- close_match_rows_updated[perfect_one_to_many,]
  x_indices <- perfect_one_to_many_matches[,x_df_col]
  y_indices <- perfect_one_to_many_matches[,y_df_col]
  x_dup_indices <- (duplicated(x_indices)|duplicated(x_indices, fromLast=TRUE))
  y_dup_indices <- (duplicated(y_indices)|duplicated(y_indices, fromLast=TRUE))
  dup_indices <- (y_dup_indices | x_dup_indices)
  non_dup_indices <- !dup_indices
  
  perfect_duplicate_indices <- rbind(perfect_duplicate_indices, perfect_one_to_many_matches[non_dup_indices,])
  check_indices <- rbind(check_indices, perfect_one_to_many_matches[dup_indices,])
  
  # remove checked rows
  close_match_rows_updated <- close_match_rows_updated[-unique(c(which(close_match_rows_updated[,x_df_col] %fin% x_indices), which(close_match_rows_updated[,y_df_col] %fin% y_indices))),]

  ### ---------- 
  #Re-check one-one close matches
  close_match_rows_updated <- find_one_to_one_matches(close_match_rows_updated)
  
  ### ---------- 
  #one-many nonperfect matches
  
  # Iterates starting with matches of the closest distance to ensure they are 
  # given priority. 
  for(i in 1:distance){
    x_dup_indices <- (duplicated(close_match_rows_updated[,x_df_col])|duplicated(close_match_rows_updated[,x_df_col], fromLast=TRUE))
    y_dup_indices <- (duplicated(close_match_rows_updated[,y_df_col])|duplicated(close_match_rows_updated[,y_df_col], fromLast=TRUE))
    
    # condition finds rows in `close_match_rows_updated` where only one column is a duplicate
    one_to_many <- !(y_dup_indices & x_dup_indices) & (y_dup_indices | x_dup_indices) & close_match_rows_updated[,3] == i
    
    # Check that the same row is not being matched to multiple. If they are 
    # them and they will be handled at the end by being treated as new rows
    matches <- close_match_rows_updated[one_to_many,]
    if(!is.null(nrow(matches))){
      match_x_dup_indices <- (duplicated(matches[,x_df_col])|duplicated(matches[,x_df_col], fromLast=TRUE))
      match_y_dup_indices <- (duplicated(matches[,y_df_col])|duplicated(matches[,y_df_col], fromLast=TRUE))
      discrepancies_indices <- rbind(discrepancies_indices, matches[!(match_y_dup_indices | match_x_dup_indices),])
      check_indices <- rbind(check_indices, matches[(match_y_dup_indices | match_x_dup_indices),])
      
      # remove checked rows and any that have already been matched. 
      close_match_rows_updated <- close_match_rows_updated[-unique(c(which(close_match_rows_updated[,x_df_col] %fin% matches[,x_df_col]), which(close_match_rows_updated[,y_df_col] %fin% matches[,y_df_col]))),]
    }
  }
  
  ### ---------- 
  #Re-check one-one close matches
  close_match_rows_updated <- find_one_to_one_matches(close_match_rows_updated)
  
  ### ---------- 
  #many-many nonperfect matches

  x_dup_indices <- (duplicated(close_match_rows_updated[,x_df_col])|duplicated(close_match_rows_updated[,x_df_col], fromLast=TRUE))
  y_dup_indices <- (duplicated(close_match_rows_updated[,y_df_col])|duplicated(close_match_rows_updated[,y_df_col], fromLast=TRUE))
  
  # Only many-to-many non perfect matches are left and need to be handled. 
  for(i in 1:distance){
    many_to_man_with_nonperfect_match <- (y_dup_indices & x_dup_indices) & close_match_rows_updated[,3] == i
    many_to_many_with_nonperfect_match_entries <- close_match_rows_updated[many_to_man_with_nonperfect_match,]
    
    x_dup_indices <- (duplicated(many_to_many_with_nonperfect_match_entries[,x_df_col])|duplicated(many_to_many_with_nonperfect_match_entries[,x_df_col], fromLast=TRUE))
    y_dup_indices <- (duplicated(many_to_many_with_nonperfect_match_entries[,y_df_col])|duplicated(many_to_many_with_nonperfect_match_entries[,y_df_col], fromLast=TRUE))
    
    
    many_to_many_i <- x_dup_indices & y_dup_indices
    one_to_many_i <- !(y_updated_dup_indices & x_updated_dup_indices) & (y_updated_dup_indices | x_updated_dup_indices)
    one_to_one_i <- !(y_updated_dup_indices | x_updated_dup_indices)
    
    
    many_to_many_e <- many_to_many_with_perfect_match_entries[many_to_many_i,]
    one_to_many_e <- many_to_many_with_perfect_match_entries[one_to_many_i,]
    one_to_one_e <- many_to_many_with_perfect_match_entries[one_to_one_i,]
    
    if(sum(discrepancy_x_matches, discrepancy_y_matches, dup_y_matches, dup_x_matches) > 0){
      
      
      many_to_many_indices <- unique(c(which(close_match_rows_updated[,y_df_col] %fin% many_to_many_e[,y_df_col]), which(close_match_rows_updated[,x_df_col] %fin% many_to_many_e[,x_df_col])))
      one_to_many_indices <- unique(c(which(close_match_rows_updated[,y_df_col] %fin% one_to_many_e[,y_df_col]), which(close_match_rows_updated[,x_df_col] %fin% one_to_many_e[,x_df_col])))
      one_to_one_indices <- unique(c(which(close_match_rows_updated[,y_df_col] %fin% one_to_one_e[,y_df_col]), which(close_match_rows_updated[,x_df_col] %fin% one_to_one_e[,x_df_col])))
      
      perfect_duplicate_indices <- rbind(perfect_duplicate_indices, one_to_one_e)
      check_indices <- rbind(check_indices, one_to_many_e, many_to_many_e)
      
      # remove rows that have already been handled to prevent double handling.
      close_match_rows_updated <- close_match_rows_updated[-unique(c(one_to_one_indices, one_to_many_indices, many_to_many_indices)),]
    }
  }
  
  ### ---------- 
  #one-one and one-many nonperfect matches 
  
  
  # Handling one to many relationship or many to many with discrepancies may  
  # have left a number of one to one or one to many rows. This will be iterative 
  # until none remain or no further matches can be found. Anything left will be assigned to  
  # new entry to ensure that it is processed correctly.
  is_unmatched <- nrow(close_match_rows_updated) > 0 
  while(is_unmatched){
    count <- nrow(close_match_rows_updated)
    x_dup_indices <- (duplicated(close_match_rows_updated[,x_df_col])|duplicated(close_match_rows_updated[,x_df_col], fromLast=TRUE))
    y_dup_indices <- (duplicated(close_match_rows_updated[,y_df_col])|duplicated(close_match_rows_updated[,y_df_col], fromLast=TRUE))
    dup_indices <- y_dup_indices|x_dup_indices
    non_dup_indices <- !dup_indices
    
    # These operations seperate rows that only have one close match
    discrepancies_indices <- rbind(discrepancies_indices, close_match_rows_updated[non_dup_indices & close_match_rows_updated[,3] > 0,])
    
    # remove rows that have already been handled to prevent double handling 
    close_match_rows_updated <- close_match_rows_updated[dup_indices,]
    
    
    # condition finds rows in `close_match_rows_updated` where only one column is a duplicate
    for(i in 1:distance){
      if(nrow(close_match_rows_updated) == 0){
        break
      }
      x_dup_indices <- (duplicated(close_match_rows_updated[,x_df_col])|duplicated(close_match_rows_updated[,x_df_col], fromLast=TRUE))
      y_dup_indices <- (duplicated(close_match_rows_updated[,y_df_col])|duplicated(close_match_rows_updated[,y_df_col], fromLast=TRUE))
      one_to_many <- !(y_dup_indices & x_dup_indices) & (y_dup_indices | x_dup_indices) & close_match_rows_updated[,3] == i
      
      # Check that the same row is not being matched to multiple. If they are 
      # them and they will be handled at the end by being treated as new rows
      matches <- close_match_rows_updated[one_to_many,]
      if(nrow(matches) > 0){
        match_x_dup_indices <- (duplicated(matches[,x_df_col])|duplicated(matches[,x_df_col], fromLast=TRUE))
        match_y_dup_indices <- (duplicated(matches[,y_df_col])|duplicated(matches[,y_df_col], fromLast=TRUE))
        discrepancies_indices <- rbind(discrepancies_indices, matches[!(match_y_dup_indices | match_x_dup_indices),])
        check_indices <- rbind(check_indices, matches[(match_y_dup_indices | match_x_dup_indices),])
        
        # remove checked rows and any that have already been matched. 
        close_match_rows_updated <- close_match_rows_updated[-unique(c(which(close_match_rows_updated[,x_df_col] %fin% matches[,x_df_col]), which(close_match_rows_updated[,y_df_col] %fin% matches[,y_df_col]))),]
        
      }
      is_unmatched <- !((count - nrow(close_match_rows_updated) == 0) | nrow(close_match_rows_updated) == 0)
      
    }
  }
  # check for mistakes 
  is_mistake_present <- check_for_mistake()
  
  #INCLUDE METHOD TO FIX PROBLEM HERE 
  if(is_mistake_present){
    
  }
  
  output <- list(discrepancies_indices, perfect_duplicate_indices, error_indices, check_indices)
  names(output) <- c("discrepancies", "perfect", "error", "check") 
  return(output)
}

check_for_mistake <- function(discrepancies_indices, perfect_duplicate_indices, error_indices, check_indices){
  x_df_col <- 1
  y_df_col <- 2
  duplicated_vec <- base::Vectorize(duplicated)
  
  # Check that there are no duplicates between data frames
  x_indices_intersecting <- Reduce(intersect, list(discrepancies_indices[,x_df_col], perfect_duplicate_indices[,x_df_col], error_indices[,x_df_col], check_indices[,x_df_col]))
  y_indices_intersecting <- Reduce(intersect, list(discrepancies_indices[,y_df_col], perfect_duplicate_indices[,y_df_col], error_indices[,y_df_col], check_indices[,y_df_col]))
  
  # Check each column contains no duplicates 
  discrepancies_indices_dup <- (duplicated_vec(t(discrepancies_indices))|duplicated_vec(t(discrepancies_indices), fromLast=TRUE))
  perfect_duplicate_indices_dup <- (duplicated_vec(t(perfect_duplicate_indices))|duplicated_vec(t(perfect_duplicate_indices), fromLast=TRUE))
  error_indices_dup <- (duplicated_vec(t(error_indices))|duplicated_vec(t(error_indices), fromLast=TRUE))
  check_indices_dup <- (duplicated_vec(t(check_indices))|duplicated_vec(t(check_indices), fromLast=TRUE))
  
  is_intersecting_dups <- (length(y_indices_intersecting) == 0) & (length(x_indices_intersecting == 0))
  return(c(is_intersecting_dups, discrepancies_indices_dup, perfect_duplicate_indices_dup, error_indices_dup, check_indices_dup))
  
}

verify_entries <- function(data_df, control_data_type, ID_col){
  data_df <- verify_integers_positive(data_df)
  data_df <- verify_reef(data_df)
  data_df <- verify_percentages(data_df)
  
  #verify long and lat separately
  data_df <- verify_lat_lng(data_df, max_val=160, min_val=138, columns=c("Longitude", "Start Lng", "End Lng"), ID_col)
  data_df <- verify_lat_lng(data_df, max_val=-5, min_val=-32, columns=c("Latitude", "Start Lat", "End Lat"), ID_col)
  
  if (control_data_type == "manta_tow") {
    
    data_df <- verify_tow_date(data_df)
    data_df <- verify_coral_cover(data_df)
    data_df <- verify_scar(data_df)
    
  } else if (control_data_type == "cull") {
    
    data_df <- verify_voyage_dates(data_df)
    
  } else if (control_data_type == "RHISS") {
    
    data_df <- verify_RHISS(data_df)
    
  } 
  data_df <- verify_na_null(data_df, control_data_type, ID_col)
  data_df$error_flag <- as.integer(data_df$error_flag)
  return(data_df)
}

verify_lat_lng <- function(data_df, max_val, min_val, columns, ID_col){
  for (col in columns) {
    if (col %in% colnames(data_df)) {
      values <- data_df[[col]]
      out_of_range <- values < min_val | values > max_val
      
      # Set any NA values to TRUE as the check was unable to be completed 
      # correctly
      out_of_range <- ifelse(is.na(out_of_range),TRUE,out_of_range)
      data_df[["error_flag"]] <- data_df[["error_flag"]] | out_of_range
      
      if (any(out_of_range)) {
        grandparent <- as.character(sys.call(sys.parent()))[1]
        parent <- as.character(match.call())[1]
        warning <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have inappropriate LatLong coordinates:", 
                         toString(data_df[out_of_range, 1]), "and the following indexes", toString((1:nrow(data_df))[out_of_range]))
        message(warning)
        
        # Append the warning to an existing matrix 
        warning_matrix <- matrix(warning)
        contribute_to_metadata_report(control_data_type, warning_matrix)
        
      }
      
    }
  }
  return(data_df)
}


verify_scar <- function(data_df) {
  # check that columns in RHISS data contain expected values according to metadata
  valid_scar <- c("a", "p", "c")
  check_valid_scar <- data_df$`Feeding Scars` %in% valid_scar
  
  data_df[["error_flag"]] <- data_df[["error_flag"]] | !check_valid_scar 
  if (any(!check_valid_scar )) {
    grandparent <- as.character(sys.call(sys.parent()))[1]
    parent <- as.character(match.call())[1]
    warning <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have invalid COTS scar:",
                     toString(data_df[!check_valid_scar , 1]), "Their respective row indexes are:", toString((1:nrow(data_df))[!check_valid_scar ]))
    message(warning)
    
    # Append the warning to an existing matrix 
    warning_matrix <- matrix(warning)
    contribute_to_metadata_report(control_data_type, warning_matrix)
    
  }
  return(data_df)
}

verify_tow_date <- function(data_df){
  # Approximate a tow date based on vessel and voyage if it does not exist. 
  
  tow_date <- data_df[["Tow date"]]
  incomplete_dates <- unique(which(is.na(tow_date)))
  if (length(incomplete_dates) > 0){
    incomplete_date_rows <- data_df[incomplete_dates,]
    vessel_voyage <- unique(incomplete_date_rows[,which(names(incomplete_date_rows) %in% c("Vessel", "Voyage"))])
    for(i in 1:nrow(vessel_voyage)){
      data_df_filtered <- data_df[(data_df$Vessel == vessel_voyage[i,1]) & (data_df$Voyage == vessel_voyage[i,2]),]
      is_any_voyage_date_correct <- any(!is.na(data_df_filtered$`Tow date`))
      if(is_any_voyage_date_correct){
        correct_date <- data_df_filtered[!(is.na(data_df_filtered$`Tow date`) | is.null(data_df_filtered$`Tow date`)),][1,"Tow date"]
        data_df[(data_df$Vessel == vessel_voyage[i,1]) & (data_df$Voyage == vessel_voyage[i,2]) & (is.na(data_df$`Tow date`)), "Tow date"] <- correct_date
      } else if(!is_any_voyage_date_correct & !is_any_survey_date_correct) {
        data_df[(data_df$Vessel == vessel_voyage[i,1]) & (data_df$Voyage == vessel_voyage[i,2]) & (is.na(data_df$`Tow date`)), c("error_flag")] <- 1
        
      }
    }
    
  }

  
  
  post_estimation_tow_dates <- data_df[["Tow date"]]
  na_present <- (is.na(post_estimation_tow_dates) | is.null(post_estimation_tow_dates)) & (is.na(tow_date) | is.null(tow_date))
  dated_estimated <- !(is.na(post_estimation_tow_dates) | is.null(post_estimation_tow_dates)) & (is.na(tow_date) | is.null(tow_date))
  
  if (any(dated_estimated | na_present)) {
    grandparent <- as.character(sys.call(sys.parent()))[1]
    parent <- as.character(match.call())[1]
    warning <- c()
    if (any(dated_estimated)) {
      warning1 <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have their tow date estimated from their vessel",
                       toString(data_df[dated_estimated , 1]), "Their respective row indexes are:", toString((1:nrow(data_df))[dated_estimated]))
      message(warning1)
      warning <- c(warning, warning1)
    }
    if (any(na_present)) {
      warning2 <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have no tow date",
                        toString(data_df[dated_estimated , 1]), "Their respective row indexes are:", toString((1:nrow(data_df))[dated_estimated]))
      message(warning2)
      warning <- c(warning, warning2)
    }  
    # Append the warning to an existing matrix 
    warning_matrix <- t(matrix(c(warning)))
    contribute_to_metadata_report(control_data_type, warning_matrix)
    
  }
  return(data_df)
}


verify_RHISS <- function(data_df) {
  # check that columns in RHISS data contain expected values according to metadata
  valid_tide <- c("L", "M", "H")
  check_tide <- data_df$`Tide` %in% valid_tide
  
  cols <- c("Slime Height (cm)", "Entangled/Mat-Like Height (cm)", "Filamentous Height (cm)", "Leafy/Fleshy Height (cm)", "Tree/Bush-Like Height (cm)")
  valid_macroalgae <- c("A", "B", "C", "0", "1", "2", "3")
  is_valid_macroalgae <- apply(data_df[, cols], 2, function(x) !(x %in% valid_macroalgae))
  check_macroalgae <- rowSums(is_valid_macroalgae) > 0
  
  valid_descriptive_bleach_severity <- c("Totally bleached white", "pale/fluoro (very light or yellowish)", "None", "Bleached only on upper surface", "Pale (very light)/Focal bleaching", "Totally bleached white/fluoro", "Recently dead coral lightly covered in algae")
  bcols <- c("Mushroom Bleach Severity", "Massive Bleach Severity", "Encrusting Bleach Severity", "Vase/Foliose Bleach Severity", "Plate/Table Bleach Severity", "Bushy Bleach Severity", "Branching Bleach Severity")
  is_valid_descriptive_bleach_severity <- apply(data_df[, bcols], 2, function(x) !(x %in% valid_descriptive_bleach_severity))
  check_descriptive_bleach_severity <- rowSums(is_valid_descriptive_bleach_severity) > 0
  
  bleached_severity <- data_df$`Bleached Average Severity Index (calculated via matrix)`
  bleached_severity <- ifelse(is.na(bleached_severity),TRUE,bleached_severity)
  check_bleach_severity <- bleached_severity >= 1 & bleached_severity <= 8
  
  check <- !check_tide | check_macroalgae | !check_bleach_severity | check_descriptive_bleach_severity
  if (any(check )) {
    grandparent <- as.character(sys.call(sys.parent()))[1]
    parent <- as.character(match.call())[1]
    if (any(!check_tide)) {
      warning1 <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have invalid tide values:",
                       toString(data_df[!check_tide , 1]), "Their respective row indexes are:", toString((1:nrow(data_df))[!check_tide]))
      message(warning1)
    }
    if (any(check_macroalgae)) {
      warning2 <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have invalid macroalgae values:",
                       toString(data_df[check_macroalgae, 1]), "Their respective row indexes are:", toString((1:nrow(data_df))[check_macroalgae]))
      message(warning2)
    }
    if (any(!check_bleach_severity)) {
      warning3 <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have invalid bleach severity values:",
                       toString(data_df[!check_bleach_severity, 1]), "Their respective row indexes are:", toString((1:nrow(data_df))[!check_bleach_severity]))
      message(warning3)
    }
    if (any(check_descriptive_bleach_severity)) {
      warning4 <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have invalid descriptive beach severity:",
                       toString(data_df[check_descriptive_bleach_severity , 1]), "Their respective row indexes are:", toString((1:nrow(data_df))[check_descriptive_bleach_severity ]))
      message(warning4)
    }
    
    # Append the warning to an existing matrix 
    warning_matrix <- t(matrix(c(warning1,warning2,warning3,warning4)))
    contribute_to_metadata_report(control_data_type, warning_matrix)
    
  }
  
  
  data_df[["error_flag"]] <- data_df[["error_flag"]] | check 
  return(data_df)
}


verify_percentages <- function(data_df) {
  # Check that all percentages in a row are between 0 and 100
  perc_cols <- grep("%", colnames(data_df))
  if(length(perc_cols) > 0){
    perc_cols_vals <- data_df[, perc_cols]
    col_check <- apply(perc_cols_vals, 2, function(x) x < 0 | x > 100)
    check <- rowSums(col_check) > 0
    check <- ifelse(is.na(check),TRUE,check)
    data_df[["error_flag"]] <- data_df[["error_flag"]] | check
    if (any(check)) {
      grandparent <- as.character(sys.call(sys.parent()))[1]
      parent <- as.character(match.call())[1]
      warning <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have percentages in an invalid format:",
                       toString(data_df[check , 1]), "Their respective row indexes are:", toString((1:nrow(data_df))[check]))
      message(warning)
      
      # Append the warning to an existing matrix 
      warning_matrix <- matrix(warning)
      contribute_to_metadata_report(control_data_type, warning_matrix)
      
    }
  }
  return(data_df)
}


verify_na_null <- function(data_df, control_data_type, ID_col) {
  # check if there are any values that are NA or NULL and flag those rows as an 
  # error. This does not include new additional columns as they are assigned a
  # default value at the end of the verification process or ID column.
  
  exempt_cols <- which(names(data_df) %in% c("error_flag", ID_col, add_required_columns(control_data_type, has_authorative_ID)))
  na_present <- apply(data_df[, -exempt_cols], 2, function(x) is.na(x) | is.null(x) | x == "")
  check <- rowSums(na_present) > 0
  check <- ifelse(is.na(check),TRUE,check)
  data_df[["error_flag"]] <- data_df[["error_flag"]] | check
  if (any(check)) {
    grandparent <- as.character(sys.call(sys.parent()))[1]
    parent <- as.character(match.call())[1]
    warning <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have missing data:",
                     toString(data_df[check , 1]), "Their respective row indexes are:", toString((1:nrow(data_df))[check]))
    message(warning)
    
    # Append the warning to an existing matrix 
    warning_matrix <- matrix(warning)
    contribute_to_metadata_report(control_data_type, warning_matrix)
    
  }
  return(data_df)
}

verify_integers_positive <- function(data_df) {
  # R function that verifys all integers are positive values as they represent 
  # real quantities. Note: Whole numbers are not integers, they must be declared
  # as such. All relevant columns were set as integers in the set_data_type 
  # function
  
    is_integer <- is.integer(data_df[1,])
    if(any(is_integer)){
      col_check <- apply(data_df[,is_integer], 2, function(x) x < 0)
      check <- rowSums(col_check) > 0
      check <- ifelse(is.na(check),TRUE,check)
      data_df[, "error_flag"] <- data_df[, "error_flag"] | check
     
      if (any(check)) {
        grandparent <- as.character(sys.call(sys.parent()))[1]
        parent <- as.character(match.call())[1]
        warning <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have non-positive integer values:",
                         toString(data_df[check , 1]), "Their respective row indexes are:", toString((1:nrow(data_df))[check]))
        message(warning)
        
        # Append the warning to an existing matrix 
        warning_matrix <- matrix(warning)
        contribute_to_metadata_report(control_data_type, warning_matrix)
      }
    }
  return(data_df)
}

remove_leading_spaces <- function(data_df) {
  # R function that removes leading and trailing spaces from all entries in a 
  # data frame column
  cols <- colnames(data_df)
  for (col_name in cols) {
    is_character <- is.character(data_df[[col_name]])
    if(any(is_character)){
      data_df[is_character, col_name] <- gsub("^\\s+|\\s+$", "", data_df[is_character, col_name])
    } 
  }
  return(data_df)
}


verify_coral_cover <- function(data_df) {
  accepted_values <- c("1-", "2-", "3-", "4-", "5-", "1+", "2+", "3+", "4+", "5+", "0")
  
  # Identify possible corrections based on common mistakes
  possible_corrections <- data.frame(
    Old_Value = c("-1", "-2", "-3", "-4", "-5","+1", "+2", "+3", "+4", "+5"),
    New_Value = c("1-", "2-", "3-", "4-", "5-", "1+", "2+", "3+", "4+", "5+")
  )
  
  # Substitute similar characters for correct ones
  data_df[["Hard Coral"]] <- gsub("—|–|−", "-", data_df[["Hard Coral"]])
  data_df[["Soft Coral"]] <- gsub("—|–|−", "-", data_df[["Soft Coral"]])
  data_df[["Recently Dead Coral"]] <- gsub("—|–|−", "-", data_df[["Recently Dead Coral"]])
  
  # Loop through possible corrections and replace values
  for (i in 1:nrow(possible_corrections)) {
    old_value <- possible_corrections$Old_Value[i]
    new_value <- possible_corrections$New_Value[i]
    data_df$`Hard Coral`[data_df$`Hard Coral` == old_value] <- new_value
    data_df$`Soft Coral`[data_df$`Soft Coral` == old_value] <- new_value
    data_df$`Recently Dead Coral`[data_df$`Recently Dead Coral` == old_value] <- new_value
  }
  
  hc_check <- data_df$`Hard Coral` %in% accepted_values
  sc_check <- data_df$`Soft Coral` %in% accepted_values
  rdc_check <- data_df$`Recently Dead Coral` %in% accepted_values
  
  error <- !hc_check | !sc_check | !rdc_check
  
  data_df[["error_flag"]] <- data_df[["error_flag"]] | error
  if (any(error)) {
    grandparent <- as.character(sys.call(sys.parent()))[1]
    parent <- as.character(match.call())[1]
    warning <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have invalid coral cover values:",
                     toString(data_df[error , 1]), "Their respective row indexes are:", toString((1:nrow(data_df))[error]))
    message(warning)
    
    # Append the warning to an existing matrix 
    warning_matrix <- matrix(warning)
    contribute_to_metadata_report(control_data_type, warning_matrix)
  }
  return(data_df)
}


verify_cots_scars <- function(data_df) {
  # check that the cots scars are an expected metric
  
  valid_values <- c("a", "p", "c")
  check <- data_df$`COTS Scars` %in% valid_values
  data_df[["error_flag"]] <- data_df[["error_flag"]] | !check
  return(data_df)
}

verify_reef <- function(data_df){
  # Check that the reef ID is in one of the correct standard formats with regex.
  # Look for most similar reef ID if it is not. Am not checking for a match 
  # because that would mean no new reefs would be accepted and I believe that 
  # the reef input is restricted to existing reefs so it is unlikley to be a typo
  
  reef_id <- data_df[["Reef ID"]]
  correct_reef_id_format <- grepl("^(1[1-9]|2[0-9])-\\d{3}[a-z]?$", reef_id)
  data_df[,"error_flag"] <- data_df[,"error_flag"] | !correct_reef_id_format
  if (any(!correct_reef_id_format)) {
    grandparent <- as.character(sys.call(sys.parent()))[1]
    parent <- as.character(match.call())[1]
    warning <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have invalid Reef IDs:",
                     toString(data_df[!correct_reef_id_format , 1]), "Their respective row indexes are:", toString((1:nrow(data_df))[!correct_reef_id_format]))
    message(warning)
    
    # Append the warning to an existing matrix 
    warning_matrix <- matrix(warning)
    contribute_to_metadata_report(control_data_type, warning_matrix)
  }
  return(data_df)
}

verify_voyage_dates <- function(data_df){
  # Check that voyage dates of observation are within in voyage dates and that 
  # none of the dates are NA. If Voyage dates are NA set start and end to min 
  # and max observation date. Check that voyage dates associated with a vessels 
  # voyage are unique (There should only be on departure and return date)
  voyage_start <- data_df[["Voyage Start"]]
  voyage_end <- data_df[["Voyage End"]]
  
  incomplete_dates <- unique(c(which(is.na(voyage_end)), which(is.na(voyage_start))))
  if (length(incomplete_dates) > 0){
    incomplete_date_rows <- data_df[incomplete_dates,]
    vessel_voyage <- unique(incomplete_date_rows[,which(names(incomplete_date_rows) %in% c("Vessel", "Voyage"))])
    for(i in 1:nrow(vessel_voyage)){
      data_df_filtered <- data_df[(data_df$Vessel == vessel_voyage[i,1]) & (data_df$Voyage == vessel_voyage[i,2]),]
      is_any_voyage_date_correct <- any(!is.na(data_df_filtered$`Voyage Start`) & !is.na(data_df_filtered$`Voyage End`))
      is_any_survey_date_correct <- any(!is.na(data_df_filtered$`Survey Date`))
      if(is_any_voyage_date_correct){
        correct_dates <- data_df_filtered[!is.na(data_df_filtered),][1,c("Voyage Start", "Voyage End")]
        data_df[(data_df$Vessel == vessel_voyage[i,1]) & (data_df$Voyage == vessel_voyage[i,2]) & (is.na(data_df$`Voyage Start`)), "Voyage Start"] <- correct_dates[1]
        data_df[(data_df$Vessel == vessel_voyage[i,1]) & (data_df$Voyage == vessel_voyage[i,2]) & (is.na(data_df$`Voyage End`)), "Voyage End"] <- correct_dates[2]
      } else if(!is_any_voyage_date_correct & is_any_survey_date_correct){
        incomplete_date_rows_filtered <- incomplete_date_rows[(incomplete_date_rows$Vessel == vessel_voyage[i,1]) & (incomplete_date_rows$Voyage == vessel_voyage[i,2]),]
        estimate_start <- min(incomplete_date_rows_filtered$`Survey Date`)
        estimate_end <- min(incomplete_date_rows_filtered$`Survey Date`)
        data_df[(data_df$Vessel == vessel_voyage[i,1]) & (data_df$Voyage == vessel_voyage[i,2]) & (is.na(data_df$`Voyage Start`)), c("Voyage Start")] <-  estimate_start
        data_df[(data_df$Vessel == vessel_voyage[i,1]) & (data_df$Voyage == vessel_voyage[i,2]) & (is.na(data_df$`Voyage End`)), c("Voyage End")] <-  estimate_end
      } else if(!is_any_voyage_date_correct & !is_any_survey_date_correct) {
        data_df[(data_df$Vessel == vessel_voyage[i,1]) & (data_df$Voyage == vessel_voyage[i,2]) & (is.na(data_df$`Voyage Start`) | is.na(data_df$`Voyage End`)), "error_flag"] <- 1
      }
    }
   
  }
  
  
  post_voyage_start <- data_df[["Voyage Start"]]
  post_voyage_end <- data_df[["Voyage End"]]
  na_present <- ((is.na(post_voyage_start) | is.null(post_voyage_start)) | (is.na(post_voyage_end) | is.null(post_voyage_end))) & ((is.na(voyage_start) | is.null(voyage_start)) | (is.na(voyage_end) | is.null(voyage_end)))
  dated_estimated <- !((is.na(post_voyage_start) | is.null(post_voyage_start)) | (is.na(post_voyage_end) | is.null(post_voyage_end))) & ((is.na(voyage_start) | is.null(voyage_start)) | (is.na(voyage_end) | is.null(voyage_end)))
  
  if (any(dated_estimated | na_present)) {
    grandparent <- as.character(sys.call(sys.parent()))[1]
    parent <- as.character(match.call())[1]
    warning <- c()
    if (any(dated_estimated) & !any(na_present)) {
      warning1 <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have their voyage dates estimated from their vessel",
                        toString(data_df[dated_estimated , 1]), "Their respective row indexes are:", toString((1:nrow(data_df))[dated_estimated]))
      message(warning1)
      warning <- c(warning, warning1)
    }
    if (any(na_present)) {
      warning2 <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have voyage dates",
                        toString(data_df[dated_estimated , 1]), "Their respective row indexes are:", toString((1:nrow(data_df))[dated_estimated]))
      message(warning2)
      warning <- c(warning, warning2)
    }  
    # Append the warning to an existing matrix 
    warning_matrix <- t(matrix(warning))
    contribute_to_metadata_report(control_data_type, warning_matrix)
    
  }
  
  data_df$error_flag <- data_df$error_flag | (data_df$`Survey Date` < data_df$`Voyage Start` | data_df$`Survey Date` > data_df$`Voyage End`)
  
  vessel_voyage <- unique(data_df[,which(names(data_df) %in% c("Vessel", "Voyage"))])
  for (i in 1:nrow(vessel_voyage)){
    filtered_data_df <- data_df[(data_df$Vessel == vessel_voyage[i,1]) & (data_df$Voyage == vessel_voyage[i,2]),]
    start_dates <- unique(filtered_data_df$`Voyage Start`) 
    end_dates <- unique(filtered_data_df$`Voyage End`)
    if(length(start_dates) > 1){
      mf_start <- names(sort(table(start_dates), decreasing = TRUE)[1])
      data_df[(data_df$Vessel == vessel_voyage[i,1]) & (data_df$Voyage == vessel_voyage[i,2]), "Voyage Start"] <- mf_start
    }
    if(length(end_dates) > 1){
      mf_end <- names(sort(table(end_dates), decreasing = TRUE)[1])
      data_df[(data_df$Vessel == vessel_voyage[i,1]) & (data_df$Voyage == vessel_voyage[i,2]), "Voyage End"] <- mf_end
    }
  }
  
  return(data_df)
}

find_one_to_one_matches <- function(close_match_rows){
  
  x_df_col <- 1
  y_df_col <- 2
  
  # Determine duplicates of the row indices.
  x_dup_indices <- (duplicated(close_match_rows[,x_df_col])|duplicated(close_match_rows[,x_df_col], fromLast=TRUE))
  y_dup_indices <- (duplicated(close_match_rows[,y_df_col])|duplicated(close_match_rows[,y_df_col], fromLast=TRUE))
  dup_indices <- y_dup_indices|x_dup_indices
  non_dup_indices <- !dup_indices
  
  # Add rows matches with only a single to relevant matrix.
  perfect_duplicate_indices <- rbind(perfect_duplicate_indices, close_match_rows[non_dup_indices & close_match_rows[,3] == 0,])
  discrepancies_indices <- rbind(discrepancies_indices, close_match_rows[non_dup_indices & close_match_rows[,3] > 0,])
  
  # remove rows that have already been handled to prevent double handling.
  close_match_rows_updated <- close_match_rows[dup_indices,]
  return(close_match_rows_updated)
}


rec_group <- function(m2m_split, groups, group){
  # That is recursive with the purpose of grouping sets of matching rows so that 
  # it can be determined whether or not they are mistakes or coincidental 
  # perfect matches.  
  group <- group + 1
  stack <- m2m_split[[group]][,1]
  names <- names(m2m_split)
  for(i in 1:length(names)){
    if(identical(stack,m2m_split[[names[i]]][,1])){
      groups[[group]] <- c(groups[[group]], m2m_split[[names[i]]][1,2])
    }
    
  }
  if(length(m2m_split) != group){
    return(rec_group(m2m_split, groups, group))
  } else {
    return(groups)
  }
}

# Re-write base operator %in% faster
`%fin%` <- function(x, table) {
  stopifnot(require(fastmatch))
  fmatch(x, table, nomatch = 0L) > 0L
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
    if(length(nonNAvalues) >= 1){
      match <- store_index_vec(nonNAvalues, nonNA, z)
      match <- t(match)
      match_indices[index:(index+nrow(match)-1),] <- match
      index <- index + nrow(match)
    }
  }
  match_indices <- na.omit(match_indices)
  return(match_indices)
}


store_index <- function(nonNAvalues, nonNA, z){
  match_indices <- c(z, nonNA, nonNAvalues)
  return(match_indices)
}
store_index_vec <- base::Vectorize(store_index)

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

get_file_and_line <- function() {
  # can get the information of where warnings were generated approximately
  
  frame <- sys.frame(n = 1)
  info <- rlang::trace_back(frame, x = FALSE)
  file <- attr(info, "file")
  line <- attr(info, "line")
  return(list(file = file, line = line))
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
  matched_output <- match_vector_entries(columns, column_names, correct_order = TRUE) # This returns the matching column names in the original order
  
  matched_column_names <- matched_output[[1]]
  matched_column_indices<- matched_output[[2]]
  
  # check that both sets of column names are still the same length after the 
  # matching
  if(length(matched_column_names) != length(column_names)){
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
  output_df <- data_df
  for(i in dataTypes){
    columns <- setDataTypeList[[i]]
    if(i == "Numeric"){
      for(x in columns){output_df[[x]] <- as.numeric(data_df[[x]])}
    } else if (i == "Date") {
      for(x in columns){
        if(is.character(data_df[[x]][1])){
          output_df[[x]] <- parse_date_time(data_df[[x]], orders = c('dmy', 'ymd', '%d/%b/%Y %I:%M:%S %p', '%Y/%b/%d %I:%M:%S %p'))
        }
      }
    } else if (i == "Integer") {
      for(x in columns){output_df[[x]] <- as.integer(data_df[[x]])}
    } else if (i == "Character"){
      for(x in columns){output_df[[x]] <- as.character(data_df[[x]])}
    }
    
  }
  
  # Identify which rows have been coerced to NA and listed in metadata report. 
  original_has_na <- apply(data_df, 2, function(x) is.na(x))
  conversion_has_na <- apply(output_df, 2, function(x) is.na(x))
  coerced_na <- rowSums(original_has_na) < rowSums(conversion_has_na)
  
  if(any(coerced_na)){
  
    grandparent <- as.character(sys.call(sys.parent()))[1]
    parent <- as.character(match.call())[1]
    indexes <- (1:length(coerced_na))
    warning <- paste("Warning in", parent , "within", grandparent, "- The rows with the following IDs have had value(s) coerced to NA:",
                     paste(data_df[coerced_na, 1], collapse = ", "), "and the following indexes", paste(indexes[coerced_na], collapse = ", "))
    message(warning)
    
    # Append the warning to an existing matrix 
    warning_matrix <- matrix(warning)
    contribute_to_metadata_report(control_data_type, warning_matrix)
  }
  
  
  # remove trailing and leading spaces from strings for comparison. 
  output_df <- remove_leading_spaces(output_df)
  
  return(output_df)
} 


# Custom Boolean Or function written in C for speed. Works exactly the same as 
# base R operator | however NA is considered TRUE. 

#REMOVED TEMPORARILY TO UNTIL FUNCTION IS MORE STABLE

# or4 <- cfunction(c(x="logical", y="logical"), "
#     int nx = LENGTH(x), ny = LENGTH(y), n = nx > ny ? nx : ny;
#     SEXP ans = PROTECT(allocVector(LGLSXP, n));
#     int *xp = LOGICAL(x), *yp = LOGICAL(y), *ansp = LOGICAL(ans);
#     for (int i = 0, ix = 0, iy = 0; i < n; ++i)
#     {
#         *ansp++ = xp[ix] || yp[iy];
#         ix = (++ix == nx) ? 0 : ix;
#         iy = (++iy == ny) ? 0 : iy;
#     }
#     UNPROTECT(1);
#     return ans;
# ")


#create matrix of warnings so they are added to the specified XML node 
# in the metadata report in a vectorised manor. 
# warnings <- names(warnings())
# warnings_matrix <- matrix(warnings, 1,length(warnings))
# contribute_to_metadata_report(control_data_type, paste("Set Data Type", i), warnings_matrix)

match_vector_entries <- function(current_vec, target_vec, control_data_type = NULL, check_mapped = FALSE, correct_order = FALSE){
  # Compare any form two vectors and identify matching entries. There are a 
  # number of pre-defined transformations that will occur if check_mapped is set
  # to true. 
  
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
      perfect_matching_current_entries_indices <- which(current_vec %in% perfect_matching_entries)
      
      # finds the target entries that have not been mapped or matched.
      nonmatched_clean_target_entries <- clean_target_vec[which(!(target_vec %in% intersect(current_vec, target_vec)))]
      nonmatched_target_entries <- target_vec[which(!(target_vec %in% intersect(current_vec, target_vec)))]
      
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
        nonmatching_current_entry_indices <- nonmatching_current_entry_indices[-perfect_matching_current_entries_indices]
        nonmatching_entries <- current_vec[nonmatching_current_entry_indices]
        
        if(check_mapped){
          # check if there is a pre-defined mapping to a non-matching column name.
          mapped_output <- map_column_names(current_vec, control_data_type)
          mapped_name_indices <- which(!is.na(mapped_output))
          
          #control statements below utilised to prevent errors
          if(is.list(mapped_output)){
            mapped_output <- unlist(mapped_output)
          } 
          
          # include pre-defined mappings found and remove them from non-matching 
          # vector
          if (length(na.omit(mapped_name_indices)) > 0){
            for(x in mapped_name_indices){ 
              clean_mapped_name <- gsub('[[:punct:] ]+',' ', mapped_output[x])
              clean_mapped_name <- gsub(' ', '', clean_mapped_name)
              clean_mapped_name <- gsub('\\.', '', clean_mapped_name)
              clean_mapped_name <- gsub('[)(/&]', '', clean_mapped_name)
              clean_mapped_name <- tolower(clean_mapped_name)
              clean_current_vec[x] <- clean_mapped_name
              current_vec[x] <- mapped_output[x]
            }
             
            updated_nonmatching_indices <- which(!(current_vec %in% intersect(current_vec, target_vec)))
            nonmatching_current_entry_indices <- updated_nonmatching_indices
            
            # finds the target entries that have not been mapped or matched.
            nonmatched_clean_target_entries <- clean_target_vec[which(!(target_vec %in% intersect(current_vec, target_vec)))]
            nonmatched_target_entries <- target_vec[which(!(target_vec %in% intersect(current_vec, target_vec)))]
          }
        }
        
        # find closest match within a specified distance with levenshtein
        # distances or matching partial strings contained within column 
        # names for non-matched target entries. 
        for(i in nonmatching_current_entry_indices){
          entry <- clean_current_vec[i]
          levenshtein_distances <- adist(entry , nonmatched_clean_target_entries)
          minimum_distance <- min(levenshtein_distances)
          closest_matching_indices <- which(levenshtein_distances==minimum_distance)
          
          partial_name_matches <- grep(entry, nonmatched_clean_target_entries)
          if(length(partial_name_matches) == 1){
            clean_current_vec[i] <- nonmatched_clean_target_entries[partial_name_matches]
            current_vec[i] <- nonmatched_target_entries[partial_name_matches]
          } else if ((length(closest_matching_indices) == 1) & (minimum_distance < maxium_levenshtein_distance)) {
            clean_current_vec[i] <- nonmatched_clean_target_entries[closest_matching_indices]
            current_vec[i] <- nonmatched_target_entries[closest_matching_indices]
          } 
        }
      } 
      
      # find list of vector of indices which indicate the position of current columns names in the legacy format. 
      # this will be used to indicate if at the end of the mapping and matching process, the program was able to 
      # correctly find all required columns. This will also then be utilised to rearrange the order of the columns
      correct_order_indices <- match(clean_target_vec, clean_current_vec)
      correct_order_indices <- correct_order_indices[!is.na(correct_order_indices)]
      
      original_order_indices <- match(clean_current_vec, clean_target_vec)
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
    
      # finds the target entries that have not been mapped or matched and 
      # creates a warning 
      nonmatched_target_entries <- target_vec[which(!(target_vec %in% intersect(current_vec, target_vec)))]
      if(length(nonmatched_target_entries) > 0){
      
        grandparent <- as.character(sys.call(sys.parent()))[1]
        parent <- as.character(match.call())[1]
        warning <- paste("Warning in", parent , "within", grandparent, "- The following target entries were not matched:", paste(nonmatched_target_entries, collapse = ", "))
        message(warning)
        
        # Append the warning to an existing matrix 
        warning_matrix <- matrix(warning)
        contribute_to_metadata_report(control_data_type, warning_matrix)
      }
      
      output <- list(current_vec, correct_order_indices, original_order_indices)
      return(output)
      
    },
    error=function(cond) {
      contribute_to_metadata_report(control_data_type, matrix(cond))
      
    }
  )
}







