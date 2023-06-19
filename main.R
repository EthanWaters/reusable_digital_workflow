
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

main <- function(leg_path, leg_sheet_index ,new_path, control_data_type, geospatial_sites, nearest_site_algorithm, is_powerBI_export){


# Initialize -------------------------------------------------------------
  
assign("is_powerBI_export", is_powerBI_export, envir = .GlobalEnv) 
  
# utilised to indicate the relevant node in XML report based on section of 
# code executed.
section <- "Import"
  
# Create new report. If the file cannot be created due to file name issues a new
# file name will be created.
file_count <- 1
report_attempt <- try(create_metadata_report(file_count))
while ((class(report_attempt)[[1]]=='try-error')&(trywait<=(10))){
  print(paste('retrying in ', trywait, 'second(s)')) 
  Sys.sleep(trywait) 
  trywait <- trywait+1 
  report_attempt <- try(create_metadata_report(file_count))
}
if (trywait>(10)) print(paste('Cannot create metadata report'))  
trywait <- 0

# The following code returns a dataframe after recieving the path 
# to a CSV, XLSX or TXT file. This can be adapted to use an explorer to choose 
# the file. The sheet index variable refer to the sheet the data is located on
# in the XLSX files and is irrelevant for CSV as it is considered "Flat". 
#Defaults to sheet index 1. 


legacy_df <- import_data(leg_path, control_data_type, is_powerBI_export, leg_sheet_index)
new_data_df <- import_data(new_path, control_data_type, is_powerBI_export, leg_sheet_index)
legacy_df <- set_data_type(legacy_df, control_data_type) 
test <- set_data_type(legacy_df, control_data_type) 

# Format Dataframe Columns ------------------------------------------------

section <- 'Format'

# The column formatting of New data will be compared with a legacy data set that 
# is deemed to be in the ideal target format. Any necessary changes will be made 
# and recorded. This will be executed irrespective of data set provided.
Updated_data_format <- format_control_data(new_data_df, legacy_df, control_data_type, section)
Updated_data_format <- set_data_type(Updated_data_format, control_data_type) 

# Find Row Discrepancies --------------------------------------------------

# Finds discrepancies in previously processed data and the new data input and handles them appropriately. 
verified_data_df <- verify_control_dataframe(Updated_data_format, legacy_df, control_data_type, section, is_new)


# Assign Nearest Sites ----------------------------------------------------

nearest_sites_cull <- assign_nearest_sites(geospatial_sites, nearest_site_algorithm, verified_data_df)



}

main()

