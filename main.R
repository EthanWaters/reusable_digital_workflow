
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

main <- function(){


# Initialize -------------------------------------------------------------
leg_path <- file.choose()
new_path <- file.choose()

control_data_options_list <- c("cull", "manta_tow", "RHISS")
control_data_type_choice <- menu(control_data_options_list, title = "Select control data type:")
control_data_type <- control_data_options_list[control_data_type_choice]
leg_sheet_index <- control_data_type_choice + 1

skip_descrepancies_check_options_list <- c("No", "Yes")
skip_descrepancies_check <- menu(skip_descrepancies_check_options_list, title = "Skip checking discrepancies?")

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

# Format Dataframe Columns ------------------------------------------------

# The column formatting of New data will be compared with a legacy data set that 
# is deemed to be in the ideal target format. Any necessary changes will be made 
# and recorded. This will be executed irrespective of data set provided.

Updated_data_format <- format_control_data(new_data_df, legacy_df, control_data_type, section)
legacy_df <- set_data_type(legacy_df, control_data_type) 

# Find Row Discrepancies --------------------------------------------------

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

ID_col <- colnames(Updated_data_format)[1]
verified_data_df <- verify_entries(new_data_df, control_data_type, ID_col)
if(is_new){
  legacy_df <- verify_entries(legacy_df, control_data_type, ID_col) 
}

# separate entries and update any rows that were changed on accident. 
if(!skip_descrepancies_check){
  verified_data_df <- separate_control_dataframe(verified_data_df, legacy_df, control_data_type, section)
}


# Assign Nearest Sites ----------------------------------------------------
nearest_sites_cull <- assign_nearest_sites(geospatial_sites, nearest_site_algorithm, verified_data_df)



}

main()

