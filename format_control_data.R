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
        data_df <- add_required_columns(data_df, control_data_type)
        
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
      
      # create comments variable to store points of interest.
      comments <- ""
      
      # acquire column names and ensure they are formatted as characters
      current_col_names <- colnames(current_df)
      legacy_col_names <- colnames(legacy_df)
      
      # compare the column names of the legacy and current format
      matched_vector_entries <- check_vector_entries_match(current_col_names, legacy_col_names)
      matched_col_names <- matched_vector_entries[[1]]
      matching_entry_indices <- matched_vector_entries[[2]]
      metadata <- matched_vector_entries[[3]]
      
      if(any(is.na(matched_col_names)) || any(is.na(matching_entry_indices))){
        comments <- paste0(comments,"matching_entry_indices or matched_col_names has returned NA, indicating a fatal error in check_vector_entries_match")
      }
      
      # Update the current dataframe column names with the vector found above.
      colnames(current_df) <- matched_col_names
      
      # rearrange columns
      updated_current_df <- current_df[matching_entry_indices]
      

      #Determine initial error flags based on returned metadata
      initial_error_flag <- rep(NA, nrow(updated_current_df))
      if (!is_not_matching_column_names & !is_not_matching_column_names_updated){
        comments <- paste0(comments,"One or more columns in legacy format were 
                           not matched and exceeded the allowable levenshtein 
                           distance to fuzzy match an avaliable column.")
        initial_error_flag <- rep(1, nrow(updated_current_df))
      }
      if(size_legacy_df > size_curent_df){
        comments <- paste0(comments,"Insufficient number of columns to match
                           legacy formatting.")
        initial_error_flag <- rep(1, nrow(updated_current_df))
      }
        
      # if error flag column does not exist create it   
      if(!"error_flag"  %in% colnames(updated_current_df)){
        updated_current_df["error_flag"] <- initial_error_flag
      }
      
      # if serious error is determined, set all error flags to TRUE as the data
      # is unusable 
      if(1 %in% initial_error_flag){
        updated_current_df["error_flag"] <- initial_error_flag
      }
      
      
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
  
add_required_columns <- function(df, control_data_type){
  lookup <- read.csv("additionalColumns.csv", header = TRUE)
  new_columns <- lookup[lookup$type == control_data_type, 1]
  df[new_columns] <- NA
  return(df)
}
  
  
verify_row_entries <- function(new_data_df, legacy_data_df, control_data_type, section){
  
  # set the data type of all entries to ensure that performed operations have 
  # expected output
  legacy_data_df <- set_data_type(legacy_data_df, control_data_type) 
  new_data_df <- set_data_type(new_data_df, control_data_type) 
  
  
  # seperate new entries and previously processed entries based on datetimes 
  # in nominated column ("seperation_column_name").
  if((control_data_type == "manta_tow") | (control_data_type == "RHIS")){
    seperation_column_name <- "date"
  } else if (control_data_type == "cull"){
    seperation_column_name <- "Voyage Start"
  }
  seperated_row_entries <- seperate_row_entries(new_data_df, legacy_data_df, seperation_column_name)
  
  previous_entries <- seperated_row_entries[1]
  new_entries <- seperated_row_entries[2]
  
  if(control_data_type == "manta_tow"){
    verified_previous_entries <- handle_previously_processed_row_entries(previous_entries)
    verified_new_entries <- handle_new_row_entries(new_entries)
  } else if (control_data_type == "cull"){
    
  } else if (control_data_type == "RHIS"){
    
  }
  
  
  # merge the verified dataset
  
  return(verified_data_df)
  
}


handle_previously_processed_row_entries <- function(previously_processed_row_entries_df){
  
  
}
  

seperate_row_entries <- function(new_data_df, legacy_data_df, seperation_column_name){
  # check that the specified seperation column name can be found with partial 
  # string match
  column_names <- colnames(legacy_data_df)
  if (any(grepl(seperation_column_name, column_names))){
    seperation_column_name <- column_names[grep(seperation_column_name, column_names)]
  }
  most_recent_date <- max(legacy_data_df$seperation_column_name)
  
  # Check that no new records were added to the most recent date
  is_most_recent_date_complete <- length(new_data_df[new_data_df$seperation_column_name > most_recent_date,]) == length(legacy_data_df[legacy_data_df$seperation_column_name > most_recent_date])
  new_entries <- new_data_df[new_data_df$seperation_column_name > most_recent_date,]
  previous_entries <- new_data_df[new_data_df$seperation_column_name < most_recent_date,]
  
  # New entries should have NA if the data was previously processed. If all 
  # entries are NA then this is the first time the dataset has been processed 
  # with this pipeline and it will be determined when evaluating row 
  # discrepancies whether the  entry is new. 
  if(!is_most_recent_date_complete){
    is_all_entries_NA <- all(is.na(new_data_df$seperation_column_name)) 
    new_entries_most_recent_day <- previous_entries[(previous_entries$seperation_column_name == most_recent_date) && (is.na(previous_entries$seperation_column_name)),]
    # Check that the new entries do have NA
    if((length(new_entries_most_recent_day) > 0) && (!is_all_entries_NA)){
      new_entries <- rbind(new_entries, new_entries_most_recent_day)
      previous_entries <- previous_entries[-((previous_entries$seperation_column_name == most_recent_date) && (is.na(previous_entries$seperation_column_name))),]
    }
  }
  output <- list(previous_entries, new_entries)
  return(output)
}


  
handle_new_row_entries <- function(new_row_entries_df){
  
  
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
  # molecularity and flexibility. 
  
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
    columns <- match_vector_entries(columns, column_names)[[1]]
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


check_vector_entries_match <- function(current_vec, target_vec){
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
    contribute_to_metadata_report(control_data_type, "Comparison", warnings_matrix)
    contribute_to_metadata_report(control_data_type, "Comparison", metadata)
    
    output <- list(current_vec, updated_matching_entry_indices, metadata)
    
    return(output)
  
  },
    error=function(cond) {
      contribute_to_metadata_report(control_data_type, section, comments)
      
  }
  )
}




