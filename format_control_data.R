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
  #Compare the format of any form of control data provided by GBRMPA with its 
  #respective legacy format. Necessary changes are made if the input is not in
  #legacy format. Changes and errors encounted are recorded for record keeping.
  #current_df: Dataframe
  #legacy_df: Dataframe
  #output: List
  
  out <- tryCatch(
    {
      current_df_col_names <- colnames(current_df)
      legacy_df_col_names <- colnames(legacy_df)
      
      matching_columns <- intersect(current_df_col_names, legacy_df_col_names)
      matching_columns_length <- length(matching_columns)
      
      #conditional statements
      is_fewer_columns <- length(current_df_col_names) < length(legacy_df_col_names)
      is_more_columns <- length(current_df_col_names) > length(legacy_df_col_names)
      is_matching_column_names <- !(matching_columns_length == length(legacy_df_col_names))
      
      if(is_matching_column_names){
        #not all columns match
      }
      if(is_fewer_columns){
        #fewer columns than needed
        
      } else if (is_more_columns){
        #more columns than needed
      } 
     
      return(data_df)
    },
    error=function(cond) {
      contribute_to_metadata_report(cond)
    },
    warning=function(cond) {
      contribute_to_metadata_report(cond)
    },
  ) 
  
}


