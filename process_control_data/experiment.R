

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
library("gmailr")

name <- "TS_AIMS_NESP_Torres_Strait_Features_V1b_with_GBR_Features.shp"
location <- "D:\\COTS\\COTS_tow_prediction_interface\\gbr"
path <- file.path(location, name)
shapefile <- st_read(path)


reef_id_pattern <- "\\b(1[0-9]|2[0-9]|10)-\\d{3}[a-z]?\\b"
row_reef_ids  <- sapply(str_extract(row_reef_ids, reef_id_pattern), toString)
polygon_ids <- sapply(str_extract(shapefile_filtered$LOC_NAME_S, reef_id_pattern), toString)

index_reefs <- match(reef_ids, polygon_ids)
shapefile_filtered <- shapefile_filtered[index_reefs,]

simplified_shapefile_filtered <- simplify_reef_polyogns_rdp(shapefile_filtered)
simplified_kml <- simplify_kml_polyogns_rdp(kml_data)

location_save <- "D:\\COTS\\on_water_PWA\\cots_on_water_pwa_draft\\spatial\\"
save_path <- file.path(location_save, paste("all_reefs_simplified",".geojson", sep = ""))
st_write(merged_sf, save_path, driver = "GeoJSON")


st_write(simplified_kml, "test2.geojson", driver = "GeoJSON")

geojson_string <- geojson_sf(simplified_kml, precision = 6, remove_empty = TRUE)
merged_sf <- st_sf(do.call(rbind, simplified_kml))


for (i in 1:length(simplified_kml)){
  save_path <- file.path(location_save, paste(gsub("/", "", names(simplified_kml[i])),".geojson", sep = ""))
  st_write(simplified_kml[[i]], save_path, driver = "GeoJSON")
  
}



cull_df <- rio::import("Output\\control_data\\cull_20231212_073208.csv")
manta_tow_df <- rio::import("Output\\control_data\\manta_tow_20231211_125937.csv") 

merged_data <- merge(manta_tow_df, cull_df[, c("Vessel", "Voyage", "Voyage End", "Voyage Start")], by = c("Vessel", "Voyage"), all.x = TRUE)
colnames <- colnames(manta_tow_df)
colnames <- c(colnames, "Voyage Start", "Voyage End")
manta_output <- merged_data[,colnames]
manta_output <-  manta_output[!duplicated(manta_output), ]

write.csv(manta_output, "Output\\control_data\\manta_tow_20231211_125937v2.csv", row.names = FALSE)
write.csv(cull_df, "Output\\control_data\\cull_20231212_073208v2c.csv", row.names = FALSE)

