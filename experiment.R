

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

location_save <- "D:\\COTS\\on_water_PWA\\cots_on_water_pwa_draft"
save_path <- file.path(location_save, "pwa_reefs_geojson_simplified.geojson")
st_write(simplified_shapefile_filtered, save_path, driver = "GeoJSON")

