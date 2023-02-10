
install.packages("readxl")
install.packages("xlsx")
install.packages("sets")
install.packages("XML")
install.packages("methods")
install.packages("xml2")
install.packages("rio")
install.packages('installr')
install.packages('dplyr')

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

main <- function(leg_path, new_cull, new_manta_tow, geospatial_sites, nearest_site_algorithm){


# Initialize -------------------------------------------------------------

  
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

# The following code returns a dataframe after recieving the path 
# to a CSV, XLSX or TXT file. This can be adapted to use an explorer to choose 
# the file. The sheet index variable refer to the sheet the data is located on
# in the XLSX files and is irrelevant for CSV as it is considered "Flat". 
#Defaults to sheet index 1. 
cull_sheet_index <- 3
manta_tow_sheet_index <- 4
RHIS_sheet_index <- 5

cull_legacy_df <- import_data(leg_path, 'cull', cull_sheet_index)
new_cull_data_df <- import_data(cull_new1, 'cull', cull_sheet_index)

manta_tow_legacy_df <- import_data(leg_path, 'manta_tow', manta_tow_sheet_index)
new_manta_tow_data_df <- import_data(manta_tow_new1, 'manta_tow', manta_tow_sheet_index)

RHIS_legacy_df <- import_data(leg_path, 'RHIS', RHIS_sheet_index)
new_RHIS_data_df <- import_data(RHIS_new1, 'RHIS', RHIS_tow_sheet_index)
  



# Format Dataframe Columns ------------------------------------------------

section <- 'structure'

# The column formatting of New data will be compared with a legacy data set that 
# is deemed to be in the ideal target format. Any necessary changes will be made 
# and recorded. This will be executed irrespective of data set provided.
Updated_cull_data_format <- compare_control_data_format(new_cull_data_df, cull_legacy_df)
Updated_manta_tow_data_format <- compare_control_data_format(new_manta_tow_data_df, manta_tow_legacy_df)
Updated_RHIS_data_format <- compare_control_data_format(new_RHIS_data_df, RHIS_legacy_df)


# Handle Errors & Generate Metadata Report --------------------------------

# Dictate whether or not the data is in a usable format based on the error flags
# received.
new_cull_data_target_format_df <- heading_error_handling(Updated_cull_data_format, 'cull', section )
new_manta_tow_data_target_format_df <- heading_error_handling(Updated_manta_tow_data_format, 'manta_tow', section)
new_RHIS_data_target_format_df <- heading_error_handling(Updated_RHIS_data_format, 'RHIS', section)


# Finds discrepancies in previously processed data and the new data input and handles them appropriately. 
cull_discrepancies_output <- verify_row_entries(new_cull_data_df)
manta_tow_discrepancies_output <- verify_row_entries(new_manta_tow_data_df)
RHIS_discrepancies_output <- verify_row_entries(new_RHIS_data_df)


# Find Row Discrepancies --------------------------------------------------

# prrevious output seperated into appropriate variables
cull_discrepancies <- cull_discrepancies_output[1]
manta_tow_discrepancies <- manta_tow_discrepancies_output[1]
RHIS_discrepancies <- RHIS_discrepancies_output[1]

consistent_previous_cull_data_df <- cull_discrepancies_output[2] 
consistent_previous_manta_tow_data_df <- manta_tow_discrepancies_output[2] 
consistent_previous_RHIS_data_df <- RHIS_discrepancies_output[2] 

only_new_cull_data_df <- cull_discrepancies_output[3] 
only_new_manta_tow_data_df <- manta_tow_discrepancies_output[3] 
only_new_RHIS_data_df <- RHIS_discrepancies_output[3] 



# Verify Row Entry Format -------------------------------------------------

# Verify that the new row entries meet all data requirements before being
# accepted for further processing. The functions are data set specific as they 
# all have different requirements.
verified_new_cull_data_df <- verify_new_cull_suitablility(only_new_cull_data_df)
verified_new_manta_tow_data_df <- verify_new_manta_tow_suitablility(only_new_manta_tow_data_df)
verified_new_RHIS_data_df <- verify_new_RHIS_suitablility(only_new_RHIS_data_df)

# Verify that the row entries with highlighted discrepancies still meet all data 
# requirements before being accepted for further processing. Determine if
# changes are quality assurance or mistakes. The functions are data set
# specific as they all have different requirements.
verified_previous_cull_data_df <- handle_cull_discrepancies(cull_discrepancies)
verified_previous_manta_tow_data_df <- handle_manta_tow_discrepancies(manta_tow_discrepancies)
verified_previous_RHIS_data_df <- handle_RHIS_discrepancies(RHIS_discrepancies)

# Combine both verified data sets to form a dataframe containing all processed 
# data to date. 
verified_control_data_df <- rbind(verified_previous_cull_data_df, verified_new_cull_data_df)
verified_manta_tow_data_df <- rbind(verified_previous_manta_tow_data_df, verified_new_manta_tow_data_df)
verified_RHIS_data_df <- rbind(verified_previous_RHIS_data_df, verified_new_RHIS_data_df)


# Assign Nearest Sites ----------------------------------------------------

nearest_sites_cull <- assign_nearest_sites(geospatial_sites, nearest_site_algorithm)
nearest_sites_manta_tow <- assign_nearest_sites(geospatial_sites, nearest_site_algorithm)
nearest_sites_RHIS <- assign_nearest_sites(geospatial_sites, nearest_site_algorithm)



}

main()
