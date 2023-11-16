
main <- function(new_path, configuration_path, leg_path = NULL) {
  
# Initialize -------------------------------------------------------------
library("tools")
library("installr")
library("readxl")
library("sets")
library("XML")
library("methods")
library("xml2")
library("rio")
library("dplyr")
library("stringr")
library("fastmatch")
library("lubridate")
library("rlang")
library("inline")
library("purrr")
library("jsonlite")
  
  
# Check if optional_arg is provided
if (!missing(leg_path)) {
  is_legacy_data_available <- 1
} else {
  is_legacy_data_available <- 0
}
  
  
configuration <- fromJSON(configuration_path)

# Create new report. If the file cannot be created due to file name issues a new
# file name will be created.
file_count <- 1
trywait <- 0
report_attempt <- try(create_metadata_report(control_data_type))
while ((class(report_attempt)[[1]]=='try-error')&(trywait<=(10))){
  print(paste('retrying in ', trywait, 'second(s)')) 
  Sys.sleep(trywait) 
  trywait <- trywait+1 
  report_attempt <- try(create_metadata_report(control_data_type))
}
if (trywait>(10)) print(paste('Cannot create metadata report'))  
trywait <- 0

# The following code returns a dataframe after recieving the path 
# to a CSV, XLSX or TXT file. This can be adapted to use an explorer to choose 
# the file. The sheet index variable refer to the sheet the data is located on
# in the XLSX files and is irrelevant for CSV as it is considered "Flat". 
#Defaults to sheet index 1. 

new_data_df <- import_data(new_path, configuration)
if(is_legacy_data_available){
  legacy_df <- import_data(leg_path, configuration)
  if("error_flag" %in% colnames(legacy_df)){
    is_new <- 0
  } else {
    is_new <- 1
    legacy_df["error_flag"] <- 0
  }
}
# Check if the new data has an authoritative ID. All rows of a database export 
# will have one and no rows from a powerBI export will. There should be no 
# scenario where only a portion of rows have IDs
has_authorative_ID <-  !any(is.na(new_data_df[,1]))
assign("has_authorative_ID", has_authorative_ID, envir = .GlobalEnv) 

transformed_data_df <- transform_data_structure(new_data_df, configuration$mappings$transformations, configuration$mappings$new_fields)
legacy_df <- set_data_type(legacy_df, configuration$mappings$data_type_mappings) 
formatted_data_df <- set_data_type(transformed_data_df, configuration$mappings$data_type_mappings) 

verified_data_df <- verify_entries(formatted_data_df, configuration)
if(is_new){
  legacy_df <- verify_entries(legacy_df, configuration) 
}

# flag non-genuine duplicates that are mistakes
verified_data_df <- flag_duplicates(verified_data_df)

# separate entries and update any rows that were changed on accident. 
if(is_legacy_data_available){
  verified_data_df <- separate_control_dataframe(verified_data_df, legacy_df, configuration$metadata$control_data_type)
}




}

leg_path <- file.choose()
new_path <- file.choose()

main(new_path, configuration_path, leg_path)









