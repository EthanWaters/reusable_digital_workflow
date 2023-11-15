
install.packages("readxl")
install.packages("xlsx")
install.packages("sets")
install.packages("XML")
install.packages("methods")
install.packages("xml2")
install.packages("rio")
install.packages('installr')
install.packages('dplyr')
install.packages('stringr')
install.packages('inline')
install.packages('rlang')
install.packages("jsonlite")

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

main <- function(){


# Initialize -------------------------------------------------------------
leg_path <- file.choose()
new_path <- file.choose()

json_file <- "D:\\COTS\\Reusable Digital Workflows\\reusable_digital_workflow\\configuration_files\\manta_tow.json"
configuration <- fromJSON(json_file)

control_data_options_list <- c("cull", "manta_tow", "RHISS")
control_data_type_choice <- menu(control_data_options_list, title = "Select control data type:")
control_data_type <- control_data_options_list[control_data_type_choice]
leg_sheet_index <- control_data_type_choice + 1

skip_descrepancies_check_options_list <- c("No", "Yes")
skip_descrepancies_check <- menu(skip_descrepancies_check_options_list, title = "Skip checking discrepancies?")

if(skip_descrepancies_check == "No"){
  skip_descrepancies_check <- 1
} else {
  skip_descrepancies_check <- 0
}

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

legacy_df <- import_data(leg_path, control_data_type, has_authorative_ID, leg_sheet_index)
new_data_df <- import_data(new_path, control_data_type, has_authorative_ID, leg_sheet_index)
if("error_flag" %in% colnames(legacy_df)){
  is_new <- 0
} else {
  is_new <- 1
  legacy_df["error_flag"] <- 0
}


# Check if the new data has an authoritative ID. All rows of a database export 
# will have one and no rows from a powerBI export will. There should be no 
# scenario where only a portion of rows have IDs
has_authorative_ID <-  !any(is.na(new_data_df[,1]))
assign("has_authorative_ID", has_authorative_ID, envir = .GlobalEnv) 

transformed_data_df <- transform_data_structure(new_data_df, configuration$mappings$transformations, configuration$mappings$new_fields)
legacy_df <- set_data_type(legacy_df, configuration$mappings$data_type_mappings) 
formatted_data_df <- set_data_type(transformed_data_df, configuration$mappings$data_type_mappings) 

}

main()

