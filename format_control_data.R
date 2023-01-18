# Format the new control data into the stardard legacy format 

install.packages("readxl")
install.packages("sets")

library("tools")
library("readxl")
library("sets")

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
  # output: Dataframe
  # Error handling is setup so that the error will be captured and recorded. 
  # However, this will interrupt the workflow as this indicates the information  
  # provided is insufficient is required to be correct to proceed. 
  out <- tryCatch(
    {
      # acquire column names 
      current_df_col_names <- colnames(current_df)
      legacy_df_col_names <- colnames(legacy_df)
      
      # Set the maximum distance for fuzzy string matching
      maxium_levenshtein_distance <- 4
      
      # determine the current column names and indices that match a column name 
      # in the legacy format. Count how many match. 
      matching_columns <- intersect(current_df_col_names, legacy_df_col_names)
      matching_columns_length <- length(matching_columns)
      matching_column_indexes <- which(legacy_df_col_names %in% matching_columns)
      
      # conditional statements to be passed to error handling so a more detailed 
      # description of any failure mode can be provided. 
      is_matching_column_names <- !(matching_columns_length == length(legacy_df_col_names))
      
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

      # create list to store data and error flags. 
      metadata <- list(length(legacy_df_col_names),
                       length(current_df_col_names),
                       is_column_names_matching, 
                       current_df_col_names[updated_nonmatching_column_indices],
                       is_matching_indices_unique, 
                       is_column_name_na) 
      
      output <- list(updated_df, metadata)
      return(output)
    },
    error=function(cond) {
      metadata <- list(length(legacy_df_col_names),
                       length(current_df_col_names),
                       is_column_names_matching, 
                       current_df_col_names[updated_nonmatching_column_indices],
                       is_matching_indices_unique, 
                       is_column_name_na, cond) 
      contribute_to_metadata_report(metadata)
    },
    warning=function(cond) {
      metadata <- list(length(legacy_df_col_names),
                       length(current_df_col_names),
                       is_column_names_matching, 
                       current_df_col_names[updated_nonmatching_column_indices],
                       is_matching_indices_unique, 
                       is_column_name_na, cond) 
      contribute_to_metadata_report(metadata)
    },
  ) 
  
}


