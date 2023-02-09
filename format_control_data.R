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


compare_control_data_format <- function(current_df, legacy_df){
  # Compare the format of any form of control data provided by GBRMPA with its 
  # respective legacy format. Necessary changes are made if the input is not in
  # legacy format. Changes and errors encounted are recorded for record keeping.
  # current_df: Dataframe
  # legacy_df: Dataframe
  # output: List containing two dataframes
  # Error handling is setup so that the error will be captured and recorded. 
  # However, this will interrupt the workflow as this indicates the information  
  # provided is insufficient is required to be correct to proceed. 
  out <- tryCatch(
    {
      
      # acquire column names and ensure they are formatted as characters
      current_df_col_names <- colnames(as.character(current_df))
      legacy_df_col_names <- colnames(as.character(legacy_df))
      
      
      # clean column names for easy comparison. The cleaning is done in this 
      # specific order to remove characters such as '.' that appear after
      # removing spaces or specific character from text ina CSV. 
      clean_current_col_names <- gsub('[[:punct:] ]+',' ',current_df_col_names)
      clean_legacy_col_names <- gsub('[[:punct:] ]+',' ',legacy_df_col_names)
      clean_current_col_names <- gsub(' ', '',current_df_col_names)
      clean_legacy_col_names <- gsub(' ', '',legacy_df_col_names)
      clean_current_col_names <- gsub('\\.', '', clean_current_col_names)
      clean_legacy_col_names <- gsub('\\.', '', clean_legacy_col_names)
      clean_current_col_names <- gsub('[)(/&]', '', clean_current_col_names)
      clean_legacy_col_names <- gsub('[)(/&]', '', clean_legacy_col_names)
      clean_current_col_names <- tolower(clean_current_col_names)
      clean_legacy_col_names <- tolower(clean_legacy_col_names)
      
      # Collect information for metadata report and define variables 
      size_legacy_df <- length(legacy_df_col_names)
      size_curent_df <- length(current_df_col_names)
      is_not_matching_column_names <- NA
      is_not_matching_column_names_updated <- NA
      updated_nonmatching_column_names_str <- NA
      is_matching_indices_unique <- NA 
      is_column_name_na <- NA   
      updated_df <- NA
      
      # Set the maximum distance for fuzzy string matching
      maxium_levenshtein_distance <- 5
     
      # determine the current column names and indices that match a column name 
      # in the legacy format. Count how many match. 
      matching_columns <- intersect(clean_current_col_names, clean_legacy_col_names)
      matching_columns_length <- length(matching_columns)
      matching_legacy_column_indexes <- which(clean_legacy_col_names %in% matching_columns)
      matching_current_column_indexes <- which(clean_current_col_names %in% matching_columns)
      
      # conditional statements to be passed to error handling so a more detailed 
      # description of any failure mode can be provided. 
      is_not_matching_column_names <- !(matching_columns_length == length(clean_legacy_col_names))
      
      # Map legacy column names to current column names that are not a perfect
      # match. This occurs by matching partial strings contained within column 
      # names, check lookup table for pre-defined column name mapppings or find 
      # closest match within a specified distance with levenshtein distances.
      # Comparisons will be performed with vector of cleaned column names but 
      # the original vector of column names with uncleaned text will be utilsied 
      # to store the column names
      if(is_not_matching_column_names){
        closest_matching_indices <- c()
        
        nonmatching_current_column_indices <- 1:length(current_df_col_names)
        nonmatching_current_column_indices <- nonmatching_current_column_indices[-matching_current_column_indexes]
        nonmatching_column_names <- clean_current_col_names[nonmatching_current_column_indices]
        
        # check if there is a pre-defined mapping to a non-matching column name.
        mapped_output <- map_column_names(nonmatching_column_names)
        mapped_name_indices <- which(!is.na(mapped_output))
        
        #control statements below utilised to prevent errors
        if(is.list(mapped_output)){
          mapped_output <- unlist(mapped_output)
        }
        
        # include pre-defined mappings found and remove them from non-matching 
        # vector
        if (length(na.omit(mapped_name_indices)) > 0){
          for(x in mapped_name_indices){ 
            mapped_index <- nonmatching_current_column_indices[x]
            current_df_col_names[mapped_index] <- mapped_output[x]
            clean_mapped_name <- gsub('[[:punct:] ]+',' ', mapped_output[x])
            clean_mapped_name <- gsub(' ', '', clean_mapped_name)
            clean_mapped_name <- gsub('\\.', '', clean_mapped_name)
            clean_mapped_name <- gsub('[)(/&]', '', clean_mapped_name)
            clean_mapped_name <- tolower(clean_mapped_name)
            clean_current_col_names[mapped_index] <- clean_mapped_name
          }
          nonmatching_column_names <- nonmatching_column_names[-mapped_name_indices]
          nonmatching_current_column_indices <- nonmatching_current_column_indices[-mapped_name_indices]
        }
        
        # find closest match within a specified distance with levenshtein
        # distances or matching partial strings contained within column 
        # names.
        for(i in nonmatching_current_column_indices){
          column_name <- clean_current_col_names[i]
          levenshtein_distances <- adist(column_name , clean_legacy_col_names)
          minimum_distance <- min(levenshtein_distances)
          closest_matching_indices <- which(levenshtein_distances==minimum_distance)
          
          partial_name_matches <- grep(column_name, clean_legacy_col_names)
          if(length(partial_name_matches) == 1){
            current_df_col_names[i] <- legacy_df_col_names[partial_name_matches]
            clean_current_col_names[i] <- clean_legacy_col_names[partial_name_matches]
          } else if ((length(closest_matching_indices) == 1) & (minimum_distance < maxium_levenshtein_distance)) {
            current_df_col_names[i] <- legacy_df_col_names[closest_matching_indices]
            clean_current_col_names[i] <- clean_legacy_col_names[closest_matching_indices]
          } 
          
          
        }
        # Indices should be unique and not NA. Check multiple columns weren't 
        # matched to the same column. The appropriate cut off distance may need 
        # to be tweaked overtime.
        duplicate_column_indices <- duplicated(current_df_col_names)
        is_matching_indices_unique <- !any(duplicated(closest_matching_indices))
        is_column_name_na <- any(is.na(current_df_col_names))
        
      }
      # Update the current dataframe column names with the vector found above.
      colnames(current_df) <- current_df_col_names
     
      
      # find list of vector of indices which indicate the position of current columns names in the legacy format. 
      # this will be used to indicate if at the end of the mapping and matching process, the program was able to 
      # correctly find all required columns. This will also then be utilised to rearrange the order of the columns 
      updated_matching_column_indices <- sapply(clean_legacy_col_names, function(x) match(x, clean_current_col_names))
      updated_matching_column_indices <- updated_matching_column_indices[!is.na(updated_matching_column_indices)]
      
      #for metadata report
      updated_matching_column_names <- clean_current_col_names[updated_matching_column_indices]
      updated_nonmatching_column_indices <- which(!clean_current_col_names %in% clean_legacy_col_names)
      updated_nonmatching_column_names <- clean_current_col_names[updated_nonmatching_column_indices]
      
      #Can use this to include extra columns not in the original legacy format
      #updated_df <- current_df[,c(updated_matching_column_indices, updated_nonmatching_column_indices)]
      
      # rearrange columns
      updated_current_df <- current_df[updated_matching_column_indices]
      
      #for metadata report
      is_not_matching_column_names_updated <- !(length(updated_matching_column_names) == length(legacy_df_col_names))
      updated_nonmatching_column_names_str <- paste0(updated_nonmatching_column_names, collapse=', ')
    
      
      
      
      # remove extra columns
      # create list to store data and error flags. 
      metadata <- data.frame(size_legacy_df,
                       size_curent_df,
                       is_not_matching_column_names, 
                       is_not_matching_column_names_updated,
                       updated_nonmatching_column_names_str,
                       is_matching_indices_unique, 
                       is_column_name_na) 
      
      
    },
    error=function(cond) {
      metadata <- data.frame(size_legacy_df,
                             size_curent_df,
                             is_not_matching_column_names, 
                             is_not_matching_column_names_updated,
                             updated_nonmatching_column_names_str,
                             is_matching_indices_unique, 
                             is_column_name_na, cond) 
    },
    warning=function(cond) {
      metadata <- data.frame(size_legacy_df,
                             size_curent_df,
                             is_not_matching_column_names, 
                             is_not_matching_column_names_updated,
                             updated_nonmatching_column_names_str,
                             is_matching_indices_unique,
                             is_column_name_na, cond) 
    }, 
    finally={
      
      output <- list(updated_current_df, metadata)
      return(output)
      
    }
  ) 
  
}

add_manta_tow_specific_formatting <- function(current_df, legacy_df){
  
}

heading_error_handling <- function(Updated_data_format, control_data_type, section){
 
  out <- tryCatch(
    {
      # The function above outputs a list containing the dataframe in the correct 
      # formatting and error flags. The code below seperates these into appropriate
      # variables
      
      
      
      # create comments variable to store points of interest.
      comments <- ""
      new_data_df <- Updated_data_format[1]
      metadata <- Updated_data_format[2]
      if(is.na(new_data_df)){
        comments <- paste0(comments,"new_data_df has returned NA, indicating 
                           a fatal error in compare_control_data_format.")
      }
      
      
      #Determine initial error flags based on returned metadata
      initial_error_flag <- rep(0, nrow(new_data_df))
      if (!is_not_matching_column_names & !is_not_matching_column_names_updated){
        comments <- paste0(comments,"One or more columns in legacy format were 
                           not matched and exceeded the allowable levenshtein 
                           distance to fuzzy match an avaliable column.")
        initial_error_flag <- rep(1, nrow(new_data_df))
      }
      if(size_legacy_df > size_curent_df){
        comments <- paste0(comments,"Insufficient number of columns to match
                           legacy formatting.")
        initial_error_flag <- rep(1, nrow(new_data_df))
      }
      metadata['comments'] <- comments
      contribute_to_metadata_report <- function(control_data_type, section, metadata)
        
      # if error flag column does not exist create it   
      if(!"error_flag"  %in% colnames(dat)){
        new_data_df["error_flag"] <- initial_error_flag
      }
      
      # if serious error is determined, set all error flags to TRUE as the data
      # is unusable 
      if(1 %in% initial_error_flag){
        new_data_df["error_flag"] <- initial_error_flag
      }
      
      
      return(output)
    },
    error=function(cond) {
     
    },
    warning=function(cond) {
      
    },
  ) 
  
}


create_metadata_report <- function(count){
    #Generate the XML template for the control data process report.
    reports_location <- "D:\\COTS\\Reusable Digital Workflows\\reusable_digital_workflow\\reports\\"
    
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
  
  
  
  
  
  
  
  









