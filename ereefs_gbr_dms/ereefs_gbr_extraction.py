import xarray as xr
import s3fs
import geopandas as gpd
from rasterio import features
from affine import Affine
import numpy as np

# Parameters
zarr_path = "s3://gbr-dms-data-public/aims-ereefs-biogeochem-baseline-monthly/data.zarr"
shapefile_path = "D:\COTS\COTS_tow_prediction_interface\gbr\TS_AIMS_NESP_Torres_Strait_Features_V1b_with_GBR_Features.shp"
variables = ['TOTAL_NITROGEN', 'Chl_a_sum', 'PhyL_Chl', 'PhyS_Chl', 'Oxy_sat', 'MA_N_pr', 'Secchi', 'Kd_490', 'DOR_P', 'DOR_N', 'DOR_C', 'salt', 'temp', 'TN', 'TC', 'TP']
date1 = '2010-11-30'
date2 = '2019-03-01'
depths = [-1,-2]
output_filename = "ereerfs_aggregations_biogeochemistry_monthly_shortlist_variables_OCT_MAR.csv"


def main(zarr_path, shapefile_path, variables, date1, date2, depths, filename):

    fs = s3fs.S3FileSystem(anon=True)
    data = xr.open_dataset(s3fs.S3Map(zarr_path, s3=fs), engine="zarr")
    data_output = data.sel(time=slice(date1, date2), k=depths)
    data_output = data_output[variables]
    data_output_shapefile = add_shape_coord_from_data_array(data_output, shapefile_path, "reef_id")
    data_output_shapefile_filtered = remove_na(data_output_shapefile)
    reef_ids = data_output_shapefile_filtered.reef_id
    data_labels = ids_to_labels(reef_ids, shapefile_path, chunk_size=100)
    data_output_shapefile_filtered["reef_label"] = data_labels
    data_output_shapefile_filtered.to_dataframe().to_csv("output\\" + filename)



def transform_from_latlon(lat, lon):
    lat = np.asarray(lat)
    lon = np.asarray(lon)
    trans = Affine.translation(lon[0], lat[0])
    scale = Affine.scale(lon[1] - lon[0], lat[1] - lat[0])
    return trans * scale

def rasterize(shapes, coords, latitude='latitude', longitude='longitude', data_type=float, fill=np.nan, **kwargs):
  
    transform = transform_from_latlon(coords[latitude], coords[longitude])
    out_shape = (len(coords[latitude]), len(coords[longitude]))
    raster = features.rasterize(shapes, out_shape=out_shape,
                                fill=fill, transform=transform,
                                dtype=data_type, **kwargs)
    spatial_coords = {latitude: coords[latitude], longitude: coords[longitude]}
    return xr.DataArray(raster, coords=spatial_coords, dims=(latitude, longitude))

def add_shape_coord_from_data_array(xr_da, shp_path, coord_name):
    # 1. read in shapefile
    shp_gpd = gpd.read_file(shp_path)

    # 2. create a list of tuples (shapely.geometry, id)
    #    this allows for many different polygons within a .shp file (e.g. States of US)
    shapes = [(shape, n) for n, shape in enumerate(shp_gpd.geometry)]
    
    xr_da[coord_name] = rasterize(shapes, xr_da.coords, longitude='longitude', latitude='latitude', data_type = float)

    return xr_da

def ids_to_labels(reef_ids, shp_path, chunk_size=10000):
    shp_gpd = gpd.read_file(shp_path)

    reef_labels = {reef_label: n for n, reef_label in enumerate(shp_gpd.LOC_NAME_S)}

    # Determine the total number of chunks needed
    total_chunks = len(reef_ids) // chunk_size + 1
    output_labels = reef_ids.copy()
    output_labels = output_labels.astype(str)
    # Iterate over chunks
    for chunk_num in range(total_chunks):
        start_idx = chunk_num * chunk_size
        end_idx = min((chunk_num + 1) * chunk_size, len(reef_ids))
       
        xr_chunk = reef_ids[slice(start_idx, end_idx)] 
        for label, id_val in reef_labels.items():
            
            xr_chunk = xr_chunk.where(xr_chunk != id_val, other=label)

        output_labels[slice(start_idx, end_idx)] = xr_chunk
        
    return output_labels
    
    
def remove_na(data, chunk_size=10000):
    
    # Determine the total number of chunks needed
    total_chunks = len(data) // chunk_size + 1
    output_data = data.copy()
    output_data = output_data.astype(str)
   
    # Iterate over chunks
    for chunk_num in range(total_chunks):
        start_idx = chunk_num * chunk_size
        end_idx = min((chunk_num + 1) * chunk_size, len(data))
       
        xr_chunk = data[slice(start_idx, end_idx)] 
        xr_chunk.where(xr_chunk.reef_id.notnull(), drop=True)
        
        output_data[slice(start_idx, end_idx)] = xr_chunk
        
    return output_data

if __name__ == "__main__":
    main(zarr_path, shapefile_path, variables, date1, date2, depths, output_filename)

    
