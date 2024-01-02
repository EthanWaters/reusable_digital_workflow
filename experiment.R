

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
save_path <- file.path(location_save, "pwa_reefs_geojson.geojson")
st_write(shapefile_filtered, save_path, driver = "GeoJSON")



simplify_reef_polyogns_rdp <- function(shapefile){
  # simplify all reef polygons stored in a list that was retrieved from the kml
  # file with the Ramer-Douglas-Peucker algorithm
  
  
    reef_geometries <- shapefile_filtered[,"geometry"]
    simplified_shapefile_filtered <- shapefile_filtered
    reef_geometries_updated <- reef_geometries
    for(i in 1:length(reef_geometries)){
      # A vast majority of reef_geometries at this level are polygons but 
      # occasionally they are geometrycollections and require iteration. 
      
        site_polygon <- reef_geometries[i,1][[1]][[1]]
        polygon_points <- site_polygon[[1]]
        approx_polygon_points <- polygon_rdp(polygon_points)
        site_polygon[[1]] <- approx_polygon_points
        simplified_shapefile_filtered[i,"geometry"][[1]][[1]] <- site_polygon
    }
    
    return(simplified_kml_data)
}

polygon_rdp <- function(polygon_points, epsilon=0.00001) {
  # adaptation of the Ramer-Douglas-Peucker algorithm. The original algorithm 
  # was developed for a use with a line not a ploygon. Remove the last point 
  # temporarily, perform the algorithm and then return the value.
  
  line_points <- polygon_points[1:nrow(polygon_points)-1,]
  approx_line_points <- rdp(line_points, epsilon)
  polygon <- rbind(approx_line_points, approx_line_points[1,])
  return(polygon)
}

rdp <- function(points, epsilon=0.00001) {
  # Ramer-Douglas-Peucker algorithm s
  if (nrow(points) <= 2) {
    return(points)
  }
  
  dmax <- 0
  index <- 0
  end <- nrow(points)
  
  # Find the point with the maximum distance
  for (i in 2:(end - 1)) {
    d <- perpendicularDistance(points[i,], points[1,], points[end,])
    if (d > dmax) {
      index <- i
      dmax <- d
    }
  }
  
  
  result <- matrix(nrow = 0, ncol = ncol(points))
  # If max distance is greater than epsilon, recursively simplify
  if (dmax > epsilon) {
    recursive1 <- rdp(points[1:index,], epsilon)
    recursive2 <- rdp(points[(index):end,], epsilon)
    result <- rbind(result, rbind(recursive1[1:nrow(recursive1) - 1,], recursive2))
  } else {
    result <- rbind(points[1,], points[end,])
  }
  
  return(result)
}

