# Format the new control data into the stardard legacy format 

install.packages("readxl")
install.packages("sets")
install.packages("XML")
install.packages("methods")
install.packages("xml2")

library(tools)
library(readxl)
library(sets)
library(XML)
library(methods)
library(xml2)

import_data <- function(data){
    out <- tryCatch(
      {
        #Opens file and stores information into dataframe. Different file types
        #require different functions to read data
        file_extension <- file_ext(data)
        if (file_extension == 'xlsx'){
          data_df <- read_excel(data)
        } else if (file_extension == 'csv'){
          data_df <- read.csv(data)
        } else {
          data_df <- read.table(file=data, header=TRUE)
        }
        return(data_df)
      },
      error=function(cond) {
        message(paste("Cannot read from:", data))
        message("Original error message:")
        message(cond)
        return(NULL)
      },
      warning=function(cond) {
        message(paste("File caused a warning:", data))
        message("Original warning message:")
        message(cond)
       
      },
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
      # acquire column names 
      current_df_col_names <- colnames(current_df)
      legacy_df_col_names <- colnames(legacy_df)
      
      
      size_legacy_df <- length(legacy_df_col_names)
      size_curent_df <- length(current_df_col_names)
                            
      
      # Set the maximum distance for fuzzy string matching
      maxium_levenshtein_distance <- 4
      
      # determine the current column names and indices that match a column name 
      # in the legacy format. Count how many match. 
      matching_columns <- intersect(current_df_col_names, legacy_df_col_names)
      matching_columns_length <- length(matching_columns)
      matching_column_indexes <- which(legacy_df_col_names %in% matching_columns)
      
      # conditional statements to be passed to error handling so a more detailed 
      # description of any failure mode can be provided. 
      is_not_matching_column_names <- !(matching_columns_length == length(current_df_col_names))
      
      # Find closest matching columns in legacy data with levenshtein distances
      # then update the vector column names
      if(is_not_matching_column_names){
        nonmatching_column_indices <- 1:length(legacy_df_col_names)[-matching_column_indexes]
        closest_match_indexs <- c()
        for(i in nonmatching_column_indices){
          column_name <- current_df_col_names[i]
          levenshtein_distances <- adist(column_name , legacy_df_col_names)
          closest_matching_index <- which(min(levenshtein_distances))
          if (closest_match_index < maxium_levenshtein_distance) {
            current_df_col_names[i] <- legacy_df_col_names[closest_matching_index]
            
          }
        }
       
        # Indices should be unique and not NA. Check multiple columns weren't 
        # matched to the same column. The appropriate cut off distance may need 
        # to be tweaked overtime.
        duplicate_column_indices <- duplicated(current_df_col_names)
        is_matching_indices_unique <- !any(duplicated(closest_matching_indices))
        is_column_name_na <- is.na(current_df_col_names)
        
      }
      
      # Update the current dataframe column names with the vector found above.
      colnames(current_df) <- current_df_col_names
      
      # rearrange columns into correct order where append non matching or 
      # additional columns appear at the end. 
      updated_matching_column_names <- intersect(current_df_col_names, legacy_df_col_names)
      updated_matching_column_indices <- match(updated_matching_column_names, legacy_df_col_names)
      updated_nonmatching_column_indices <- outersect(current_df_col_names, legacy_df_col_names)
      updated_df <- current_df[,c(updated_matching_column_indices, updated_nonmatching_column_indices)]
      
      updated_nonmatching_column_names <- current_df_col_names[updated_nonmatching_column_indices]
      is_not_matching_column_names_updated <- !(length(updated_matching_column_names) == length(current_df_col_names))
      
      # create list to store data and error flags. 
      metadata <- data.frame(size_legacy_df,
                       size_curent_df,
                       is_not_matching_column_names, 
                       is_not_matching_column_names_updated,
                       updated_nonmatching_column_names,
                       is_matching_indices_unique, 
                       is_column_name_na) 
      
      output <- list(updated_df, metadata)
      return(output)
    },
    error=function(cond) {
      metadata <- data.frame(size_legacy_df,
                             size_curent_df,
                             is_not_matching_column_names, 
                             is_not_matching_column_names_updated,
                             updated_nonmatching_column_names,
                             is_matching_indices_unique, 
                             is_column_name_na, cond) 
      contribute_to_metadata_report(metadata)
    },
    warning=function(cond) {
      metadata <- data.frame(size_legacy_df,
                             size_curent_df,
                             is_not_matching_column_names, 
                             is_not_matching_column_names_updated,
                             updated_nonmatching_column_names,
                             is_matching_indices_unique, 
                             is_column_name_na, cond) 
      contribute_to_metadata_report(metadata)
    },
  ) 
  
}



heading_error_handling(Updated_data_format){
 
  out <- tryCatch(
    {
      # The function above outputs a list containing the dataframe in the correct 
      # formatting and error flags. The code below seperates these into appropriate
      # variables
      new_data_df <- Updated_data_format[1]
      metadata <- Updated_data_format[2]
      
      #Determine initial error flags based on returned metadata
      initial_error_flag <- rep(1, nrow(new_data_df))
      if(is_not_matching_column_names & !is_not_matching_column_names_updated){
        #All columns were matched
        contribute_to_metadata_report()
      } else if (!is_not_matching_column_names & !is_not_matching_column_names_updated){
        #Matches weren't found
        contribute_to_metadata_report()
        initial_error_flag <- rep(0, nrow(new_data_df))
      }
      if(size_legacy_df > size_curent_df){
        #not enough information
        contribute_to_metadata_report()
        initial_error_flag <- rep(0, nrow(new_data_df))
      } else if(size_legacy_df > size_curent_df){
        #Extra column
        contribute_to_metadata_report()
      }
      
      metadata <- data.frame(size_legacy_df,
                             size_curent_df,
                             is_not_matching_column_names, 
                             updated_nonmatching_column_indices,
                             is_matching_indices_unique, 
                             is_column_name_na, cond) 
      
      
      
      
      return(output)
    },
    error=function(cond) {
     
    },
    warning=function(cond) {
      
    },
  ) 
  
}

create_metadata_report <- function(){
  
  #create file name systematically
  date <- as.character(Sys.Date())
  timestamp <- as.character(Sys.time())
  filename <- paste0("Processed Control Data Report ", date, ".xml", sep="")
  
  #generate template
  template <- xml_new_root("session") 
  control_data <- xml_find_all(template, "//session")
  xml_add_child(control_data, "timestamp", timestamp)  
  xml_add_child(control_data, "info", sessionInfo()[-c(13,12,11,9,8)])  
  xml_add_child(control_data, "control_data")
  control_data <- xml_find_all(template, "//control_data")
  xml_add_child(control_data, "manta_tow")  
  xml_add_child(control_data, "cull") 
  xml_add_child(control_data, "RHISS")
  write_xml(template, file = filename, options =c("format", "no_declaration"))
  
  utils::sessionInfo()
}

contribute_to_metadata_report <- function(){
  
}