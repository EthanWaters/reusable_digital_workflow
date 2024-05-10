
main <- function(configuration_path, connection_string, new_files) {
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
  library("foreach")
  library("doParallel")
  library("DBI")
  library("RMySQL")
  
  configuration_path <- "D:\\COTS\\on_water_PWA\\cots_on_water_pwa_draft\\back_end\\reusable_digital_workflow\\configuration_files\\app_manta_tow_config.json"
  connection_string <- "MySQL://root:csiro@127.0.0.1:3306/cotscontrolcentre"
  components <- unlist(strsplit(connection_string, "://|:|@|/", perl = TRUE))
  
  # Extract the individual components of connection string
  username <- components[2]
  password <- components[3]
  hostname <- components[4]
  port <- components[5]
  database_name <- components[6]
  
  new_files <- c("Input/control_data/TAB#3 COTS_Surveillance_2024_1_22_9_9_7.json", "Input/control_data/TAB#5 COTS_Surveillance_2024_1_20_17_49_58.json", "Input/control_data/TAB#6 COTS_Surveillance_2024_1_20_17_49_21.json")
  
  configuration <- fromJSON(configuration_path)
  control_data_type <- configuration$metadata$control_data_type
  calculate_site_rasters <- 0
  
  new_data_df <- fromJSON(new_files[1])
  for(i in 2:length(new_files)){
    new_data_df <- rbind(new_data_df, fromJSON(new_files[i]))
  }
  
  # create geometry initially if manta tow so that start and end point 
  # coordinates can be derived from the geospatial line
  if(control_data_type == "manta_tow"){
    new_data_df$Geometry <- sf::st_as_sfc(new_data_df$Geometry)
    new_data_df <- sf::st_sf(new_data_df)
  } 

  con <- DBI::dbConnect(RMySQL::MySQL(), user = username,password = password,host = hostname,port = as.integer(port),dbname = database_name, load_data_local_infile = TRUE)
  legacy_df <- get_app_data_database(con, configuration$metadata$control_data_type)
  
  serialised_spatial_path <- find_recent_file(configuration$metadata$input_directory$serialised_spatial_path, "site", "rds")

  transformed_data_df <- map_data_structure(new_data_df, configuration$mappings$transformations, configuration$mappings$new_fields)

  # Convert column names of both legacy (app data) and new data (json export) to
  # conventional legacy names in original research workflow to use other functions 
  app_to_research_config <- fromJSON(configuration$metadata$input_directory$app_to_research_names)
  legacy_df <- map_all_fields(legacy_df, legacy_df, app_to_research_config$mappings$transformations)
  transformed_data_df <- map_all_fields(transformed_data_df, transformed_data_df, app_to_research_config$mapping$transformations)
  
  coral_cover_cols <- which(colnames(transformed_data_df) %in% c("Hard Coral", "Soft Coral", "Recently Dead Coral"))
  if (length(coral_cover_cols) > 0){
    for(i in coral_cover_cols){
      transformed_data_df[,i] <- get_coral_cover(transformed_data_df[,i])
    }
  }
  
  if ("Feeding Scars" %in% colnames(transformed_data_df)){
    transformed_data_df[["Feeding Scars"]] <- get_feeding_scar_from_description(transformed_data_df[["Feeding Scars"]])
  }

    
  # assign site and reef information if they are missing
  tryCatch({
    transformed_data_df <- assign_site_and_reef(transformed_data_df, serialised_spatial_path, control_data_type)
    transformed_data_df$`Reef ID` <- get_reef_label(transformed_data_df$Reef)
  }, error = function(e) {
    print(paste("Error assigning sites:", conditionMessage(e)))
  })
  
  legacy_df <- set_data_type(legacy_df, app_to_research_config$mappings$data_type_mappings) 
  formatted_data_df <- set_data_type(transformed_data_df, app_to_research_config$mappings$data_type_mappings) 
  verified_data_df <- verify_entries(formatted_data_df, configuration)
  verified_data_df <- flag_duplicates(verified_data_df)
  
  ### AGGREGATION 
  if (control_data_type == "manta_tow"){
    verified_data_df <- aggregate_manta_tows_site_resolution(verified_data_df)
  } else if (control_data_type == "cull") { 
    verified_data_df <- aggregate_culls_site_resolution(verified_data_df)
  }
  
  verified_new_df <- separate_new_control_app_data(verified_data_df, legacy_df, control_data_type)
  verified_new_df <- map_all_fields(verified_new_df, verified_new_df, app_to_research_config$mapping$reverse_transformation)
  dbSendQuery(con, "SET GLOBAL local_infile = true;")
  for (i in 1:nrow(verified_new_df)) {
    entry <- verified_new_df[i,]
    tryCatch({
      if(control_data_type != "RHIS"){
        
        # Append to Vessel and voyage table if needed
        vessel_id <- append_to_vessel(con, entry)
        
        # append new vessels 
        append_to_table_unique()
        
        voyage_id <- append_to_voyage(con, vessel_id, entry)
        reef_id <- dbGetQuery(con, paste("SELECT id FROM reef WHERE reef_label = ", get_reef_label(entry$reef_name), sep = ""))$id
        site_id <- append_to_site(con, entry, reef_id)
        
      }
      # Append to Data table
      append_to_data(con, control_data_type, vessel_id, voyage_id, site_id, entry)
    }, error = function(e) {
      print(paste("Error uploading entry",i , ":", conditionMessage(e)))
    })
    
  }
  
  dbDisconnect(con)
}
  
  
  

args <- commandArgs(trailingOnly = TRUE)
configuration_path <- args[1]
connection_string <- args[2]
new_files <- args[-c(1:2)]

main(configuration_path, connection_string, new_files)



