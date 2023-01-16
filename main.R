

main <- function(new_cull, previous_cull, cull_legacy, new_manta_tow, previous_manta_tow, manta_tow_legacy, new_RHISS, previous_RHISS, RHISS_legacy, geospatial_sites, nearest_site_algorithm){

    
cull_legacy_df <- import_data(cull_legacy)
previous_cull_df <- import_data(previous_cull)
new_cull_data_df <- import_data(new_cull)

manta_tow_legacy_df <- import_data(manta_tow_legacy)
previous_manta_tow_df <- import_data(previous_manta_tow)
new_manta_tow_data_df <- import_data(new_manta_tow)

RHISS_legacy_df <- import_data(RHISS_legacy)
previous_RHISS_df <- import_data(previous_RHISS)
new_RHISS_data_df <- import_data(new_RHISS)
  
compare_cull_data_format_output <- compare_control_data_format(new_cull_data_df, cull_legacy)
compare_manta_tow_data_format_output <- compare_control_data_format(new_manta_tow_data_df, manta_tow_legacy)
compare_RHISS_data_format_output <- compare_control_data_format(new_RHISS_data_df, RHISS_legacy)

new_cull_data_df <- compare_cull_data_format_output[1]
new_manta_tow_data_df <- compare_manta_tow_data_format_output[1]
new_RHISS_data_df <- compare_RHISS_data_format_output[1]

errors_cull <- compare_cull_data_format_output[2]
errors_manta_tow <- compare_manta_tow_data_format_output[2]
errors_RHISS <- compare_RHISS_data_format_output[2]

new_control_data_target_format_df <- heading_error_handling(errors, new_control_data_df)

cull_discrepancies_output <- find_cull_discrepancies(previous_control_data_df, new_control_data_target_format_df)

row_discrepancies <- find_row_discrepancies_output[1]
consistent_previous_control_data_df <- find_row_discrepancies_output[1] 
only_new_control_data_df <- find_row_discrepancies_output[1] 

verified_new_control_data_df <- verify_new_data_suitablility(only_new_control_data_df)
verified_previous_control_data_df <- handle_row_discrepancies(row_discrepancie)
verified_control_data_df <- rbind(verified_previous_control_data_df, verified_new_control_data_df)

nearest_sites <- assign_nearest_sites(geospatial_sites, nearest_site_algorithm)


}

main()
