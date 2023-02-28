
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

library("tools")
library("installr")
library("xlsx")
library("readxl")
library("sets")
library("XML")
library("methods")
library("xml2")
library("rio")
library("dplyr")
library("stringr")

main <- function(leg_path, new_cull, new_manta_tow, geospatial_sites, nearest_site_algorithm, is_powerBI_export){


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
cull_sheet_index <- 3
manta_tow_sheet_index <- 4
RHIS_sheet_index <- 5

cull_legacy_df <- import_data(leg_path, 'cull', is_powerBI_export, cull_sheet_index)
new_cull_data_df <- import_data(cull_new1, 'cull', is_powerBI_export, cull_sheet_index)

manta_tow_legacy_df <- import_data(leg_path, 'manta_tow', is_powerBI_export, manta_tow_sheet_index)
new_manta_tow_data_df <- import_data(manta_tow_new1, 'manta_tow', is_powerBI_export, manta_tow_sheet_index)

RHIS_legacy_df <- import_data(leg_path, 'RHIS', is_powerBI_export, RHIS_sheet_index)
new_RHIS_data_df <- import_data(RHIS_new1, 'RHIS', is_powerBI_export, RHIS_tow_sheet_index)
  



# Format Dataframe Columns ------------------------------------------------

section <- 'Format'

# The column formatting of New data will be compared with a legacy data set that 
# is deemed to be in the ideal target format. Any necessary changes will be made 
# and recorded. This will be executed irrespective of data set provided.
Updated_cull_data_format <- format_control_data(new_cull_data_df, cull_legacy_df, 'cull', section)
Updated_manta_tow_data_format <- format_control_data(new_manta_tow_data_df, manta_tow_legacy_df, 'manta_tow', section)
Updated_RHIS_data_format <- format_control_data(new_RHIS_data_df, RHIS_legacy_df, 'RHIS', section)


# Find Row Discrepancies --------------------------------------------------

# Finds discrepancies in previously processed data and the new data input and handles them appropriately. 
verified_new_cull_data_df <- verify_control_dataframe(Updated_cull_data_format, cull_legacy_df, 'cull', section, is_new)
verified_new_manta_tow_data_df <- verify_control_dataframe(Updated_manta_tow_data_format, manta_tow_legacy_df, 'manta_tow', section, is_new)
verified_new_RHIS_data_df <- verify_control_dataframe(Updated_RHIS_data_format, RHIS_legacy_df, 'RHIS', section, is_new)


# Assign Nearest Sites ----------------------------------------------------

nearest_sites_cull <- assign_nearest_sites(geospatial_sites, nearest_site_algorithm)
nearest_sites_manta_tow <- assign_nearest_sites(geospatial_sites, nearest_site_algorithm)
nearest_sites_RHIS <- assign_nearest_sites(geospatial_sites, nearest_site_algorithm)



}

main()
