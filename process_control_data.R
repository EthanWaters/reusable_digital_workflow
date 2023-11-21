
main <- function(new_path, configuration_path, kml_path, leg_path = NULL) {
  
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
  library("sp")
  library("rgdal")
  library("parallel")
  library("raster")
  library("terra")
  library("units")
  library("tidyverse")
  library("tictoc")
  library("tidyr")
  library("ggplot2")
  library("lwgeom")
  library("stars")
  library("stringr")
  library("fasterize")
    
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
  # scenario where only a portion of rows have IDs. Even if an ID is present it 
  # should be ensured that it is authoritative before altering the configuration 
  # files to preference the use of the ID for separation rather than checking 
  # for differences manually. 
  
  has_authorative_ID <-  !any(is.na(new_data_df[[configuration$metadata$ID_col]])) & configuration$metadata$is_ID_preferred
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
  
  
  kml_layers <- st_layers(kml_path)
  layer_names <- kml_layers["name"]
  layer_names_vec <- unlist(kml_layers["name"])
  kml_data <- setNames(lapply(layer_names_vec, function(i)  st_read(kml_file, layer = i)), layer_names_vec)
  crs <- projection(kml_data[[1]])
  
  tryCatch({
    if(configuration$metadata$assign_sites){
      verified_data_df <- assign_nearest_method_c(kml_data, verified_data_df, layer_names_vec, crs, raster_size=0.0005, x_closest=1, is_standardised=1, save_rasters=1)
    }
  }, error = function(e) {
    print(paste("Error assigning sites:", conditionMessage(e)))
  })
  
  # extract non-spatial attributes and export them 
  verified_data_df <- as.data.frame(verified_data_df)
  write.csv(verified_data_df, paste("Output\\",configuration$metadata$control_data_type,"_", Sys.Date(), ".csv", sep = ""), row.names = FALSE)
  
}

leg_path <- file.choose()
new_path <- file.choose()

main(new_path, configuration_path, kml_path, leg_path)






