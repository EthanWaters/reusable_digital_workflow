source("source.R")
library("sf")

name <- "TS_AIMS_NESP_Torres_Strait_Features_V1b_with_GBR_Features.shp"
location <- "D:\\COTS\\COTS_tow_prediction_interface\\gbr"
path <- file.path(location, name)
shapefile <- st_read(path)

simplified_shapefile <- simplify_shp_polyogns_rdp(shapefile)

location_save <- "D:\\COTS\\on_water_PWA\\cots_on_water_pwa_draft"
save_path <- file.path(location_save, "pwa_reefs_geojson_simplified.geojson")
st_write(simplified_shapefile, save_path, driver = "GeoJSON")
