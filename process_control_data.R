
main <- function(configuration_path, new_path = NULL, kml_path = NULL, leg_path = NULL) {
  
  # Initialize -------------------------------------------------------------
  source("source.R")
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
  library("sf")
  library("raster")
  library("terra")
  library("units")
  library("tidyverse")
  library("tidyr")
  library("lwgeom")
  library("stars")
  library("stringr")
      
  configuration <- fromJSON(configuration_path)
  
  most_recent_report_path <- find_recent_file(configuration$metadat$output_directory$reports, configuration$metadat$control_data_type, "json")
  most_recent_leg_path <- find_recent_file(configuration$metadat$output_directory$control_data, configuration$metadat$control_data_type, "csv")
  most_recent_new_path <- find_recent_file(configuration$metadat$input_directory$control_data, configuration$metadat$control_data_type, "csv")
  most_recent_kml_path <- find_recent_file(configuration$metadat$input_directory$spatial_data, configuration$metadat$control_data_type, "kml")
  most_recent_serialised_spatial_path <- find_recent_file(configuration$metadat$output_directory$spatial_data, "site_regions", "rds")
  
  previous_report <- fromJSON(most_recent_report_path)
  
  if (is.null(new_path)) {
    new_path <- most_recent_new_path
  }
  
  # Attempt to use legacy data where possible. 
  if (is.null(leg_path)) {
    if(is.null(most_recent_leg_path)){
      is_new <- 1
      is_legacy_data_available <- 0
    } else {
      leg_path <- most_recent_leg_path
      is_new <- 0
      is_legacy_data_available <- 1
    }
  } else {
    is_new <- 0
    is_legacy_data_available <- 1
  }
  
  # Reduce computation time by only assigning sites to raster pixels when needed.
  # If the previous output utilised the most up-to-date kml file, then there 
  # will be a saved serialised version of the raster data that can be utilised 
  # instead of calculating it again as this is by far the most time consuming 
  # part of the process. 
  if (is.null(kml_path)) {
    kml_path <- most_recent_kml_path
    if(kml_path == previous_report$input$kml_path){
      calculate_site_rasters <- 0
      serialised_spatial_path <- most_recent_serialised_spatial_path
    } else {
      calculate_site_rasters <- 1
      serialised_spatial_path <- NULL
    }
  } else {
    calculate_site_rasters <- 1
    serialised_spatial_path <- NULL
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
  
  new_data_df <- rio::import(new_path)
  if(is_legacy_data_available){
    legacy_df <- rio::import(leg_path)
    if("error_flag" %in% colnames(legacy_df)){
      is_new <- 0
    } else {
      is_new <- 1
      legacy_df["error_flag"] <- 0
    }
  }
  # Check if the new data has an authoritative ID. All rows of a database export 
  # will have one and no rows from a powerBI export will. There should be no 
  # scenario where only a portion of rows have IDs. Even if an ID is present it 
  # should be ensured that it is authoritative before altering the configuration 
  # files to preference the use of the ID for separation rather than checking 
  # for differences manually. 
  
  has_authorative_ID <-  !any(is.na(new_data_df[[configuration$metadata$ID_col]])) & configuration$metadata$is_ID_preferred
  assign("has_authorative_ID", has_authorative_ID, envir = .GlobalEnv) 
  
  transformed_data_df <- transform_data_structure(new_data_df, configuration$mappings$transformations, configuration$mappings$new_fields)
  if(is_legacy_data_available){
    legacy_df <- set_data_type(legacy_df, configuration$mappings$data_type_mappings) 
  }
  formatted_data_df <- set_data_type(transformed_data_df, configuration$mappings$data_type_mappings) 
  
  verified_data_df <- verify_entries(formatted_data_df, configuration)
  if(is_new){
    legacy_df <- verify_entries(legacy_df, configuration) 
  }
  
  # flag non-genuine duplicates that are mistakes
  verified_data_df <- flag_duplicates(verified_data_df)
  
  tryCatch({
    if(configuration$metadata$assign_sites){
      if(configuration$metadata$control_data_type == "manta_tow"){
        verified_data_df <- assign_nearest_method_c(kml_path, configuration$metadata$control_data_type, calculate_site_rasters, spatia_path, raster_size=0.0005, x_closest=1, is_standardised=0, save_rasters=0)
      } else {
        verified_data_df$`Nearest Site` <- site_names_to_numbers(verified_data_df$`Site Name`)
      }
        
    }
  }, error = function(e) {
    print(paste("Error assigning sites:", conditionMessage(e)))
  })
  
  # separate entries and update any rows that were changed on accident. 
  tryCatch({
    if(is_legacy_data_available){
      verified_data_df <- separate_control_dataframe(verified_data_df, legacy_df, configuration$metadata$control_data_type)
    }
  }, error = function(e) {
    print(paste("Error seperating control data. All data has been treated as new entries.", conditionMessage(e)))
  })
  
  # Save workflow output
  tryCatch({
    if (!dir.exists(configuration$metadata$output_directory)) {
      dir.create(configuration$metadata$output_directory, recursive = TRUE)
    }
    write.csv(verified_data_df, paste(configuration$metadata$output_directory, "\\",configuration$metadata$control_data_type,"_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".csv", sep = ""), row.names = FALSE)
  }, error = function(e) {
    print(paste("Error saving data - Data saved in source directory", conditionMessage(e)))
    write.csv(verified_data_df, paste(configuration$metadata$control_data_type,"_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".csv", sep = ""), row.names = FALSE)

  })
  
  
}



args <- commandArgs(trailingOnly = TRUE)
new_path <- args[1]
configuration_path <- args[2]
kml_path <- args[3]
if(length(args) >= 4){
  leg_path <- args[4]
} else{
  leg_path <- NULL
}

main(new_path, configuration_path, kml_path, leg_path)






