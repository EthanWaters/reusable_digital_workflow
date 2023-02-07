

main <- function(new_cull, previous_cull, cull_legacy, new_manta_tow, previous_manta_tow, manta_tow_legacy, new_RHISS, previous_RHISS, RHISS_legacy, geospatial_sites, nearest_site_algorithm){


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
# the file. 
cull_legacy_df <- import_data(cull_legacy, 'cull', 3)
new_cull_data_df <- import_data(new_cull, 'cull', 3)

manta_tow_legacy_df <- import_data(manta_tow_legacy, 'manta_tow', 4)
new_manta_tow_data_df <- import_data(new_manta_tow, 'manta_tow', 4)

RHISS_legacy_df <- import_data(RHISS_legacy, 'RHISS', 5)
new_RHISS_data_df <- import_data(new_RHISS, 'RHISS', 5)
  



# Format Dataframe Columns ------------------------------------------------

section <- 'structure'

# The column formatting of New data will be compared with a legacy data set that 
# is deemed to be in the ideal target format. Any necessary changes will be made 
# and recorded. This will be executed irrespective of data set provided.
Updated_cull_data_format <- compare_control_data_format(new_cull_data_df, cull_legacy_df)
Updated_manta_tow_data_format <- compare_control_data_format(new_manta_tow_data_df, manta_tow_legacy_df)
Updated_RHISS_data_format <- compare_control_data_format(new_RHISS_data_df, RHISS_legacy_df)


# Handle Errors & Generate Metadata Report --------------------------------

# Dictate whether or not the data is in a usable format based on the error flags
# received.
new_cull_data_target_format_df <- heading_error_handling(Updated_cull_data_format, 'cull', section )
new_manta_tow_data_target_format_df <- heading_error_handling(Updated_manta_tow_data_format, 'manta_tow', section)
new_RHISS_data_target_format_df <- heading_error_handling(Updated_RHISS_data_format, 'RHISS', section)


# Finds discrepancies in previously processed data and the new data input. 
# Outputs a list containing three data frames. These data frames are for new row 
# entries, old row entries that are perfect duplicates and old row entries which 
# have changed.
cull_discrepancies_output <- find_cull_discrepancies(previous_cull_df, new_cull_data_target_format_df)
manta_tow_discrepancies_output <- find_manta_tow_discrepancies(previous_manta_tow_df, new_manta_tow_data_target_format_df)
RHISS_discrepancies_output <- find_RHISS_discrepancies(previous_RHISS_df, new_RHISS_data_target_format_df)


# Find Row Discrepancies --------------------------------------------------

# prrevious output seperated into appropriate variables
cull_discrepancies <- cull_discrepancies_output[1]
manta_tow_discrepancies <- manta_tow_discrepancies_output[1]
RHISS_discrepancies <- RHISS_discrepancies_output[1]

consistent_previous_cull_data_df <- cull_discrepancies_output[2] 
consistent_previous_manta_tow_data_df <- manta_tow_discrepancies_output[2] 
consistent_previous_RHISS_data_df <- RHISS_discrepancies_output[2] 

only_new_cull_data_df <- cull_discrepancies_output[3] 
only_new_manta_tow_data_df <- manta_tow_discrepancies_output[3] 
only_new_RHISS_data_df <- RHISS_discrepancies_output[3] 



# Verify Row Entry Format -------------------------------------------------

# Verify that the new row entries meet all data requirements before being
# accepted for further processing. The functions are data set specific as they 
# all have different requirements.
verified_new_cull_data_df <- verify_new_cull_suitablility(only_new_cull_data_df)
verified_new_manta_tow_data_df <- verify_new_manta_tow_suitablility(only_new_manta_tow_data_df)
verified_new_RHISS_data_df <- verify_new_RHISS_suitablility(only_new_RHISS_data_df)

# Verify that the row entries with highlighted discrepancies still meet all data 
# requirements before being accepted for further processing. Determine if
# changes are quality assurance or mistakes. The functions are data set
# specific as they all have different requirements.
verified_previous_cull_data_df <- handle_cull_discrepancies(cull_discrepancies)
verified_previous_manta_tow_data_df <- handle_manta_tow_discrepancies(manta_tow_discrepancies)
verified_previous_RHISS_data_df <- handle_RHISS_discrepancies(RHISS_discrepancies)

# Combine both verified data sets to form a dataframe containing all processed 
# data to date. 
verified_control_data_df <- rbind(verified_previous_cull_data_df, verified_new_cull_data_df)
verified_manta_tow_data_df <- rbind(verified_previous_manta_tow_data_df, verified_new_manta_tow_data_df)
verified_RHISS_data_df <- rbind(verified_previous_RHISS_data_df, verified_new_RHISS_data_df)


# Assign Nearest Sites ----------------------------------------------------

nearest_sites_cull <- assign_nearest_sites(geospatial_sites, nearest_site_algorithm)
nearest_sites_manta_tow <- assign_nearest_sites(geospatial_sites, nearest_site_algorithm)
nearest_sites_RHISS <- assign_nearest_sites(geospatial_sites, nearest_site_algorithm)



}

main()
