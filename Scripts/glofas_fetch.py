
import cdsapi
import pandas as pd
import xarray as xr
import geopandas as gpd
import os
from glob import glob
from shapely.geometry import Point


datapath = "your_glofas_path"
jrc_database ="your_jrc_db_path"
onshore_path = "your_onshore_path"
disc_path = "your_processed_discharge_path"
pecd_code = ["your pecd_code"]
code = "your country code"

#===============================================================================================================
# pecd_code = [ "AT01", "AT02", "AT03"]
# code = 'AT'
# pecd_code = [ "CH00"]
# code = 'CH'
# pecd_code = [ "LV00"]
# code = 'LV'
# pecd_code = [ "PT01","PT02"]
# code = 'PT'
# pecd_code = [ "RO01", "RO02", "RO03"]
# code = "RO"

#===========================================fetch and read Glofas original data============================================
REQUEST_YEAR=["2015","2016","2017","2018","2019","2020",
                "2021", "2022", "2023","2024"]
REQUEST_MONTH=["01","02","03","04","05","06","07","08","09","10","11","12"]

for year in REQUEST_YEAR:
    for month in REQUEST_MONTH:
        dataset = "cems-glofas-historical"
        request = {
            "system_version": ["version_4_0"],
            "hydrological_model": ["lisflood"],
            "product_type": ["consolidated"],
            "variable": ["river_discharge_in_the_last_24_hours"],
            "hyear": year,
            "hmonth": month,
            "hday": [
                "01", "02", "03","04", "05", "06", "07", "08", "09", "10", "11", "12",
                "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24",
                "25","26","27","28","29","30","31"
            ],
            "data_format": "netcdf4",
            "download_format": "unarchived",
            "area": [72, -25, 34, 45]
        }

        client = cdsapi.Client(url="https://ewds.climate.copernicus.eu/api")
        
        client.retrieve(dataset, request, os.path.join(datapath,f"{year}_{month}_00utc.nc"))
        print(f"Retrieve {year}_{month} successfully") 


def read_cdf(files, dim):

    def process_one_path(path):
        cdf_path=path
        ds = xr.open_dataset(cdf_path, engine="netcdf4")
        ds_copy = ds.load()  
        ds.close()          
        return ds_copy

    paths=sorted(glob(files))
    dataset = [process_one_path(path) for path in paths]

    return xr.concat(dataset, dim=dim)

data = read_cdf(f"{datapath}/*.nc", "valid_time")


#===========================================read Glofas disc data============================================
def sjoin_gdf(gdf1, gdf2):
    gdf=gdf1.sjoin(gdf2)
    if 'index_right' in gdf.columns:
        gdf=gdf.drop(columns='index_right')

    if 'index_left' in gdf.columns:
        gdf=gdf.drop(columns='index_left')

    return gdf

def extract_values(row, variable_name, ds):
    lat, lon = row['Latitude'], row['Longitude']
    val = ds[variable_name].sel(latitude=lat, longitude=lon, method='nearest').values
    return val

def reshape_values(var, time_range, grids):
        reshape_dict={}
        i=0
        for row in grids[var]:
            reshape_dict[f"hror_{i}"]=row
            i=i+1
        df_reshaped=pd.DataFrame(reshape_dict, index=pd.to_datetime(time_range))

        return df_reshaped


location=jrc_database[jrc_database['type'] == "HROR"]
location['geometry'] = location.apply(lambda row: Point(row['lon'], row['lat']),axis=1)

onshore = gpd.read_file(onshore_path)
pecd=onshore[onshore['level']=='PECD2']
zone = pecd[pecd['id'].isin(pecd_code)]
loc_geo=gpd.GeoDataFrame(location,geometry='geometry',crs=zone.crs)
zone_hror = loc_geo[loc_geo['country_code']==code]
hror_point=pd.concat([zone_hror['lat'], zone_hror['lon']], axis=1)
var = 'dis24'
hror_point[var]= hror_point.apply(lambda row: extract_values(row, var, data), axis=1)
data_local=reshape_values(var, pd.date_range(data['valid_time'].values[0], data['valid_time'].values[-1], freq='d').shift(-1), hror_point)
data_local.to_csv(disc_path)
