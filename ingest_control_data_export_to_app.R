
main <- function(script_dir, configuration_path, serialised_spatial_path, connection_string, new_files) {
   
  tryCatch({
    
   # Initialize -------------------------------------------------------------
    setwd(dirname(script_dir))
    source("source.R")
    library("tools")
    library("installr")
    library("rio")
    library("dplyr")
    library("sf")
    library("stars")
    library("lwgeom")
    library("terra")
    library("raster")
    library("stringr")
    library("lubridate")
    library("rlang")
    library("jsonlite")
    library("tidyverse")
    library("tidyr")
    library("stringr")
    library("DBI")
    library("RMySQL")
 
    
    base::message(configuration_path)
    base::message(serialised_spatial_path)
    base::message(connection_string)
    base::message(new_files)
    base::message(class(new_files))
    components <- unlist(strsplit(connection_string, "://|:|@|/", perl = TRUE))

    # Extract the individual components of connection string
    username <- components[2]
    password <- components[3]
    hostname <- components[4]
    port <- components[5]
    database_name <- components[6]
    
    
    configuration <- fromJSON(configuration_path)
    control_data_type <- configuration$metadata$control_data_type
    calculate_site_rasters <- 0
    
    new_data_df <- fromJSON(new_files[1])
    if (length(new_files) > 1){
      for(i in 2:length(new_files)){
        file_path <- new_files[i]
        new_data_df <- rbind(new_data_df, fromJSON(file_path))
      }
    }

    voyage_dates <- get_voyage_dates_strings(new_data_df$CrownOfThornsStarfishVoyageTitle)
    # create geometry initially if manta tow so that start and end point 
    # coordinates can be derived from the geospatial line
    if(control_data_type == "manta_tow"){
      new_data_df$Geometry <- sf::st_as_sfc(new_data_df$Geometry)
      new_data_df <- sf::st_sf(new_data_df)
    } 
    
    con <- DBI::dbConnect(RMySQL::MySQL(), user = username,password = password,host = hostname,port = as.integer(port),dbname = database_name, load_data_local_infile = TRUE)
    legacy_df <- get_app_data_database(con, configuration$metadata$control_data_type)
    
    base::message("Mapping data structure...")
    transformed_data_df <- map_data_structure(new_data_df, configuration$mappings$transformations, configuration$mappings$new_fields)
    base::message("Mapping data structure complete")

    coral_cover_cols <- which(colnames(transformed_data_df) %in% c("hard_coral", "soft_coral", "recently_dead_coral"))
    if (length(coral_cover_cols) > 0){
      for(i in coral_cover_cols){
        transformed_data_df[,i] <- get_coral_cover(transformed_data_df[,i])
      }
    }
    
    if ("scars" %in% colnames(transformed_data_df)){
      transformed_data_df[["scars"]] <- get_feeding_scar_from_description(transformed_data_df[["scars"]])
    }
  
    base::message("Assign missing site and reef info...")
    # assign site and reef information if they are missing
    tryCatch({
      transformed_data_df <- assign_missing_site_and_reef(transformed_data_df, serialised_spatial_path, control_data_type)
      transformed_data_df$`reef_label` <- get_reef_label(transformed_data_df$reef_name)
    }, error = function(e) {
      print(paste("Error assigning sites:", conditionMessage(e)))
    })
    
    base::message("Completed assigning missing site and reef info...")
    
    base::message("Setting data type ...")
    legacy_df <- set_data_type(legacy_df, configuration$mappings$data_type_mappings) 
    formatted_data_df <- set_data_type(transformed_data_df, configuration$mappings$data_type_mappings)
    base::message("Completed Setting data type ...")
    
    base::message("Performing verification of entries ...")
    
    verified_data_df <- verify_entries(formatted_data_df, configuration)
    verified_data_df <- flag_duplicates(verified_data_df)
    base::message("Completed verification of entries ...")
    
    ### AGGREGATION 
    verified_new_df <- separate_new_control_app_data(verified_data_df, legacy_df)
    verified_new_df$start_date <- voyage_dates$start_date
    verified_new_df$stop_date <- voyage_dates$stop_date
    base::message("Aggregating...")
    if (control_data_type == "manta_tow"){
      verified_df <- aggregate_manta_tows_site_resolution_app(verified_new_df)
    } else if (control_data_type == "cull") { 
      verified_df <- aggregate_culls_site_resolution_app(verified_new_df)
    }

    base::message("Completed aggregating...")
    base::message("Saving to database...")
    tryCatch({
      if(control_data_type != "RHIS"){
        
        vessel_df <- data.frame(
          name = verified_df$vessel_name,
          short_name = get_vessel_short_name(verified_df$vessel_name)
        )
        
        append_to_table_unique(con, "vessel", vessel_df)
        vessel_ids <- get_id_by_row(con, "vessel", vessel_df)
        verified_df$vessel_id <- vessel_ids
        
        voyage_df <- data.frame(
          vessel_voyage_number = as.character(verified_df$vessel_voyage_number),
          start_date = as.character(verified_df$start_date),
          stop_date = as.character(verified_df$stop_date),
          vessel_id = verified_df$vessel_id
        )

        voyage_df <- voyage_df %>%
          group_by(vessel_voyage_number, vessel_id) %>%
          mutate(
            start_date = as.character(names(sort(table(start_date), decreasing = TRUE))[1]),
            stop_date = as.character(names(sort(table(stop_date), decreasing = TRUE))[1])
          )
        
        append_to_table_unique(con, "voyage", voyage_df)
        voyage_ids <- get_id_by_row(con, "voyage", voyage_df)
        verified_df$voyage_id <- voyage_ids
 
        reef_df <- data.frame(
          reef_label = verified_df$reef_label
        )
        reef_ids <- get_id_by_row(con, "reef", reef_df)
        verified_df$reef_id <- reef_ids
        
        site_df <- data.frame(
          name = verified_df$site_name,
          reef_id = verified_df$reef_id
        )
        
        site_ids <- get_id_by_row(con, "site", site_df)
        site_to_append_df <- site_df[is.na(site_ids),]
        site_to_append_df <- na.omit(site_to_append_df)
        append_to_table_unique(con, "site", site_to_append_df)
        site_ids <- get_id_by_row(con, "site", site_df)
        verified_df$site_id <- site_ids
      } 
      
      column_names <- dbListFields(con, control_data_type)
      data_df <- verified_df[,which(colnames(verified_df) %in% column_names)]
      data_df <- na.omit(data_df)
      data_df <- data_df[!(data_df$error_flag == 1),]
      data_df <- as.data.frame(data_df, check.name = FALSE)
      append_to_table_unique(con, control_data_type, data_df)
      
    }, error = function(e) {
      print(paste("Error uploading entry",i , ":", conditionMessage(e)))
    })
    dbDisconnect(con)
  }, error = function(e) {

    print(paste("Error:", conditionMessage(e)))
  })  
}


args <- commandArgs(trailingOnly = TRUE)

for (i in 1:length(args)) {
  args[i] <- gsub("\\\\", "\\\\\\\\", args[i])
}

script_dir <- args[1]
configuration_path <- args[2]
serialised_spatial_path <- args[3]
connection_string <- args[4]
new_files <- args[-c(1:4)]

main(script_dir, configuration_path, serialised_spatial_path, connection_string, new_files)



