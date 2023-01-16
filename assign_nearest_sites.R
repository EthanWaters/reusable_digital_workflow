# Assign nearest site to tow data

assign_nearest_sites <- function(geospatial_sites, nearest_site_algorithm){
  #Delegates responsability of assigning nearest site to the desired algorithm
  
  if(nearest_site_algorithm == 1){
    nearest_site_algorithm_one(geospatial_sites)
  } else if(nearest_site_algorithm == 2){
    nearest_site_algorithm_two(geospatial_sites)
  } else if(nearest_site_algorithm == 3){
    nearest_site_algorithm_three(geospatial_sites)
  } else {
    nearest_site_algorithm_one(geospatial_sites)
  }
}

nearest_site_algorithm_one <- function(data){
  out <- tryCatch(
    {
      
    },
    error=function(cond) {
      
    },
    warning=function(cond) {
      
    },
  )    
  return(data_df)
}


