
main <- function(new_path, configuration_path = NULL, kml_path = NULL, leg_path = NULL) {
  tryCatch({
    # Initialize -------------------------------------------------------------

    source("source.R")
    library("tools")
    library("installr")
    library("sets")
    library("methods")
    library("rio")
    library("dplyr")
    library("stringr")
    library("fastmatch")
    library("lubridate")
    library("rlang")
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
    library("future")
    library("furrr")
    library("foreach")
    library("doParallel")
    library("digest")
    
    keyword <- get_file_keyword(new_path) 

    if (is.null(configuration_path)) {
      configuration_path <- find_recent_file("configuration_files/", paste("research_",keyword, sep=""), "json")
      configuration <- fromJSON(configuration_path)
    }
    
    most_recent_kml_path <- find_recent_file(configuration$metadata$input_directory$spatial_data, "sites", "kml")
    most_recent_report_path <- find_recent_file(configuration$metadata$output_directory$reports, configuration$metadata$control_data_type, "json")
    most_recent_leg_path <- find_recent_file(configuration$metadata$output_directory$control_data_unaggregated, configuration$metadata$control_data_type, "csv")
    serialised_spatial_path <- find_recent_file(configuration$metadata$output_directory$spatial_data, "site_regions", "rds")
    
    previous_kml_path <- NULL
    if(!is.null(most_recent_report_path)){
      previous_report <- fromJSON(most_recent_report_path)
      previous_kml_path <- previous_report$inputs$kml_path
    } 

    # Attempt to use legacy data where possible. 
    is_new <- 0
    is_legacy_data_available <- 1
    if (is.null(leg_path)) {
      leg_path <- most_recent_leg_path
      if(is.null(most_recent_leg_path)){
        is_new <- 1
        is_legacy_data_available <- 0
      } else {
        leg_path <- most_recent_leg_path
      }
    } 
    
    if (is.null(kml_path)) {
      kml_path <- most_recent_kml_path
    }
    
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
    
    # create list to export as json file with metadata about workflow
    metadata_json_output <- list()    
    
    # timestamp workflow 
    metadata_json_output[["timestamp"]] = Sys.time()
    
    # record inputs to workflow
    metadata_json_output[["inputs"]] = list(
      kml_path = kml_path,
      new_path = new_path,
      leg_path = leg_path, 
      configuration_path = configuration_path,
      serialised_spatial_path = serialised_spatial_path
    )
    
    # record inputs to workflow
    metadata_json_output[["decisions"]] = list(
      has_authorative_ID = has_authorative_ID,
      is_legacy_data_available = is_legacy_data_available,
      is_new = is_new
    )
    
    # save metadata json file 
    json_data <- toJSON(metadata_json_output, pretty = TRUE)
    writeLines(json_data, file.path(getwd(), configuration$metadata$output_directory$reports, paste(configuration$metadata$control_data_type, "_Report_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".json", sep = "")))
    
    # configuration <- update_config_file(new_data_df, configuration_path)
    
    transformed_data_df <- map_data_structure(new_data_df, configuration$mappings$transformations, configuration$mappings$new_fields)
    if(is_legacy_data_available){
      legacy_df <- set_data_type(legacy_df, configuration$mappings$data_type_mappings)
    }
    formatted_data_df <- set_data_type(transformed_data_df, configuration$mappings$data_type_mappings) 
    
    
    verified_data_df <- verify_entries(formatted_data_df, configuration)
    if(is_new && is_legacy_data_available){
      legacy_df <- verify_entries(legacy_df, configuration) 
    }
    
    # flag non-genuine duplicates that are mistakes
    verified_data_df <- flag_duplicates(verified_data_df)
    
    tryCatch({
      if(configuration$metadata$assign_sites){
        if(configuration$metadata$control_data_type == "manta_tow"){
          verified_data_df <- assign_nearest_site_method_c(verified_data_df, kml_path, configuration$metadata$control_data_type, previous_kml_path, serialised_spatial_path, configuration$metadata$output_directory$spatial_data, raster_size=0.0005, x_closest=1, is_standardised=0, save_spatial_as_raster=0)
        } else {
          verified_data_df$`Nearest Site` <- site_names_to_numbers(verified_data_df$`Site Name`)
        }
        verified_data_df$`Nearest Site` <- ifelse(is.na(verified_data_df$`Nearest Site`), -1, verified_data_df$`Nearest Site`)
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
    
    # Save workflow outputc
    tryCatch({
      if (!dir.exists(configuration$metadata$output_directory$control_data_unaggregated)) {
        dir.create(configuration$metadata$output_directory$control_data_unaggregated, recursive = TRUE)
      }
      
      output_directory <- configuration$metadata$output_directory$control_data_unaggregated
      data_type <- configuration$metadata$control_data_type
      timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
      file_name <- paste(data_type, "_", timestamp, ".csv", sep = "")
      output_path <- file.path(output_directory, file_name)
      write.csv(verified_data_df, output_path, row.names = FALSE)
      
    }, error = function(e) {
      print(paste("Error saving data - Data saved in source directory", conditionMessage(e)))
      write.csv(verified_data_df, paste(configuration$metadata$control_data_type,"_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".csv", sep = ""), row.names = FALSE)
  
    })
    
    if(configuration$metadata$control_data_type == "manta_tow"){
      verified_aggregated_df <- aggregate_manta_tows_site_resolution_research(verified_data_df)  
    } else if (configuration$metadata$control_data_type == "cull") {
      verified_aggregated_df <- aggregate_culls_site_resolution_research(verified_data_df) 
    }
    
    tryCatch({
      if (!dir.exists(configuration$metadata$output_directory$control_data_aggregated)) {
        dir.create(configuration$metadata$output_directory$control_data_aggregated, recursive = TRUE)
      }
      
      output_directory <- configuration$metadata$output_directory$control_data_aggregated
      data_type <- configuration$metadata$control_data_type
      timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
      file_name <- paste(data_type, "_", timestamp, ".csv", sep = "")
      output_path <- file.path(output_directory, file_name)
      write.csv(verified_aggregated_df, output_path, row.names = FALSE)
      
    }, error = function(e) {
      print(paste("Error saving data - Data saved in source directory", conditionMessage(e)))
      write.csv(verified_aggregated_df, paste(configuration$metadata$control_data_type,"_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".csv", sep = ""), row.names = FALSE)
      
    })
  }, error = function(e) {
    print(paste("Critical Error in workflow could not be resolved:", conditionMessage(e)))
  })
}


args <- commandArgs(trailingOnly = TRUE)
new_path <- args[1]

# Initialize optional arguments as NULL
configuration_path <- NULL
kml_path <- NULL
leg_path <- NULL

# Loop through the arguments to find optional ones
for (i in 2:length(args)) {
  if (startsWith(args[i], "--config=")) {
    new_path <- sub("--config=", "", args[i])
  } else if (startsWith(args[i], "--kml=")) {
    kml_path <- sub("--kml=", "", args[i])
  } else if (startsWith(args[i], "--leg=")) {
    leg_path <- sub("--leg=", "", args[i])
  }
}

main(configuration_path, aggregate , new_path, kml_path, leg_path)



