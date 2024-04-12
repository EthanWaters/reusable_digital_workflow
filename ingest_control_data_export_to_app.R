
main <- function(configuration_path, db_host, db_port, db_name, db_user, db_password, new_files) {
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
  
  db_host <- "127.0.0.1"
  db_port <- "3306"
  db_name <- "cotscontrolcentre"
  db_user <- "root"
  db_password <- "csiro"
  
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

  con <- dbConnect(RMariaDB::MariaDB(), dbname = db_name, user = db_user, password = db_password, host = db_host, port = db_port)
  legacy_df <- get_app_data_database(con, configuration$metadata$control_data_type)
  
  serialised_spatial_path <- find_recent_file(configuration$metadata$input_directory$serialised_spatial_path, "site", "rds")
  
  # create list to export as json file with metadata about workflow
  metadata_json_output <- list()    
  metadata_json_output[["timestamp"]] = Sys.time()
  metadata_json_output[["inputs"]] = list(
    configuration_path = configuration_path,
    serialised_spatial_path = serialised_spatial_path
  )
  
  # record inputs to workflow
  metadata_json_output[["decisions"]] = list(
    has_authorative_ID = has_authorative_ID,
    is_legacy_data_available = is_legacy_data_available,
    is_new = is_new, 
    calculate_site_rasters = calculate_site_rasters
  )
  
  # save metadata json file 
  json_data <- toJSON(metadata_json_output, pretty = TRUE)
  writeLines(json_data, file.path(getwd(), configuration$metadata$output_directory$reports, paste(configuration$metadata$control_data_type, "_Report_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".json", sep = "")))
  
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
  
  verified_new_df_test <- separate_new_control_app_data(verified_data_df, legacy_df, control_data_type)
  ### CHECK 
  
  ### WRITE TO SQL SERVER
  
}
  
  
  

args <- commandArgs(trailingOnly = TRUE)
configuration_path <- args[1]
db_host <- args[2]
db_port <- args[3]
db_name <- args[4]
db_user <- args[5]
db_password <- args[6]
new_files <- args[-c(1:6)]

main(configuration_path, db_host, db_port, db_name, db_user, db_password, new_files)



