""""
Collection of simple script to assist in downloading remote sensing data from satelite mission. This script is used 
within the LTER-LIFE digital twin infrastructure (https://lter-life.nl/en).

Modules contain functions for accessing data from copernicus data hub 

"""

import datetime
import json
import requests 
import pandas as pd
import re
import os
import platform
import glob
import numpy as np
from geojson import Polygon, Feature, FeatureCollection, dump
import geopandas as gpd
import folium

def get_copernicus_access_token(username, password, refresh_token = None, storage_refresh_token = False):
    """
    Get token for accessing copernicus data service. With possibility in refreshing token generated. 
    Author: Stanley N, Qing, Z, LTER-LIFE
    Date: 26-3-2024
    Updated Date: 18-6-2024

    -----------------
    Example: 
    access_response = dtSat.get_access_token(username="<>", password="<>")
    access_token = access_response["access_token"]
    refresh_token = access_response["refresh_token"]
    """

    if refresh_token:
        print("refreshing access token...")
        data = {
            "client_id": "cdse-public",
            "grant_type": "refresh_token",
            "refresh_token" : refresh_token
        }
    else:
        data = {
            "client_id": "cdse-public",
            "username": username,
            "password": password,
            "grant_type": "password",
        }
    try:
        r = requests.post(
            "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
            data=data,
        )
        r.raise_for_status()
    except Exception as e:
        raise Exception(
            f"Access token creation failed. Reponse from the server was: {r.json()}"
        )
    print(f"Authentication with code: {r.status_code}")
    if storage_refresh_token:
        with open("./.copernicus_token.json", "w") as f:
            json.dump(r.json(), f)
    return r.json()


def get_sentinel_catalogue(start_date, end_date, data_collection, aoi, product_type="S2MSI1C", cloudcover=10.0, 
                          max_results = None):

    """
    Get collection of dataset available with a specific data collection type for a given date and region
    Author: Stanley N, Qing, Z, LTER-LIFE
    Date: 26-3-2024 

    -----------------
    Example: 
    start_date = "2022-06-01"
    end_date = "2022-06-10"
    data_collection = "SENTINEL-2"
    aoi = "POLYGON((4.6 53.1, 4.9 53.1, 4.9 52.8, 4.6 52.8, 4.6 53.1))'" 
    catalogue_response = dtSat.get_catalogue(start_date, end_date, data_collection, aoi)

    """

    ## do some check of argument validity here 
    format = "%Y-%m-%d"
    start_date_dt = datetime.datetime.strptime(start_date, format).date()
    end_date_dt = datetime.datetime.strptime(end_date, format).date()

    assert end_date_dt > start_date_dt, "End date must be greater than Start date."

    copernicus_catalogue = ["SENTINEL-2", "SENTINEL-3", "LANDSAT-8"]
    assert data_collection in copernicus_catalogue, "data collection type not presence in catalogue/not implemented yet. "

    endpoint_url = (f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=Collection/Name eq '{data_collection}'"
                    f" and Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover' and att/OData.CSC.DoubleAttribute/Value lt {cloudcover})"
                    f" and Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'productType' and att/OData.CSC.StringAttribute/Value eq '{product_type}')"
                    f" and OData.CSC.Intersects(area=geography'SRID=4326;{aoi})"
                    f" and ContentDate/Start gt {start_date}T00:00:00.000Z"
                    f" and ContentDate/Start lt {end_date}T00:00:00.000Z&$orderby=ContentDate/Start desc")
    
    if max_results is not None: 
        if (max_results>0) & (max_results<=1000): endpoint_url += '&$top={}'.format(max_results)
    
    try:
        r = requests.get(endpoint_url)
    except Exception as e:
        raise Exception(
            f"Request for data catalogue {data_collection} failed. Try with another collection. "
        )

    results = r.json()
    
    while '@odata.nextLink' in results:
        r = requests.get(results['@odata.nextLink'])
        results = r.json()
        print("Found {} more scenes".format(len(results['value'])))
 
    
    return results


def data_sentinel_request_by_id(access_response, catalogue_response, countid = 1, dir_path = None):
    
    access_token = access_response["access_token"]
    refresh_token = access_response["refresh_token"]

    access_response["time_now"] = datetime.datetime.now()
    timediff = access_response["time_now"] - access_response["time_generated"] 
    if timediff.total_seconds()/60 > 10:
        print("yes")
        data = {
            "client_id": "cdse-public",
            "grant_type": "refresh_token",
            "refresh_token" : refresh_token
        }

        r = requests.post(
            "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
            data=data,
        )

        access_token = r.json()["access_token"] 
        access_response["time_regenerated"] = datetime.datetime.now() 

        r.raise_for_status()

    headers = {"Authorization": f"Bearer {access_token}"}
    session = requests.Session()
    session.headers.update(headers)
    
    lookup_tiles = catalogue_response["value"]
    unique_date = [get_date(lookup_tiles[i]["OriginDate"]) for i in range(len(lookup_tiles))]
    unique_idx = [unique_date.index(i) for i in set(unique_date)]
    unique_tiles = []
    for i in unique_idx:
        unique_tiles.append(lookup_tiles[i])

    # tile = unique_tiles[countid]
    tile = lookup_tiles[countid]
    endpoint_url = f"https://zipper.dataspace.copernicus.eu/odata/v1/Products(" + tile['Id'] + ")/$value"
    print(endpoint_url)

    try:
        response = session.get(endpoint_url, headers=headers, stream=True)
    except Exception as e:
        raise Exception("Data download could not be executed now. Try again later! ")
    
    if dir_path is None or dir_path == "": 
        print("data is stored in folder name 'inputdir' " + os.getcwd())
        os.makedirs("./inputdir/")
        file_store_path = "./inputdir/" + tile['Name']+".zip" 
    else: 
        inputdir = os.path.abspath(dir_path)
        ## ?? make path OS-agnostic ?? 
        if platform.system() == "Windows":
            file_store_path = inputdir + "\\" + tile['Name']+".zip"
        elif platform.system() == "Linux":
            file_store_path = inputdir + "/" + tile['Name']+".zip"
    # with open(f"" + tile['Name']+".zip" , "wb") as file:
    with open(f"" + file_store_path , "wb") as file:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                file.write(chunk)
    print("Downloading of data is completed !!!!")




def data_sentinel_request(access_response, catalogue_response, dir_path = None):

    """"
    Download data from copernicus sentinel hub within available catalogue using access token 

    Author: Stanley N, Qing, Z, LTER-LIFE
    Date: 26-3-2024 

    -----------------
    Example: 
    start_date = "2022-06-01"
    end_date = "2022-06-10"
    data_collection = "SENTINEL-2"
    aoi = "POLYGON((4.6 53.1, 4.9 53.1, 4.9 52.8, 4.6 52.8, 4.6 53.1))'" 
    catalogue_response = dtSat.get_catalogue(start_date, end_date, data_collection, aoi)

    dtSat.data_request(access_response, catalogue_response)
    """
    access_token = access_response["access_token"]
    refresh_token = access_response["refresh_token"]

    headers = {"Authorization": f"Bearer {access_token}"}
    session = requests.Session()
    session.headers.update(headers)
    
    lookup_tiles = catalogue_response["value"]
    unique_date = [get_date(lookup_tiles[i]["OriginDate"]) for i in range(len(lookup_tiles))]
    unique_idx = [unique_date.index(i) for i in set(unique_date)]
    unique_tiles = []
    for i in unique_idx:
        unique_tiles.append(lookup_tiles[i])

    count = 0 ## for testing = download one data in the catalogue
    
    if not os.path.exists(dir_path): 
        print(f"directory path does not exist, creating directory: {dir_path}....")
        os.makedirs(dir_path)
    
    files_in_dir = [file.split(".")[0] for file in os.listdir(dir_path) if file.endswith(".zip")]
    
    
    for tile in lookup_tiles:
        # if(count > 0): break ## remove if you want to download all data in the catalogue
        
        if tile["Name"] in files_in_dir:
            print(f"Data with with name: {tile['Name']} already downloaded previously")
            continue
        
        endpoint_url = f"https://zipper.dataspace.copernicus.eu/odata/v1/Products(" + tile['Id'] + ")/$value"
        data_response = session.get(endpoint_url, allow_redirects=False)
        if data_response.status_code in (301, 302, 303, 307, 401):
            print(f"Authentication fail: {data_response.status_code}")
            print(f"Retrying automatically with a new token")
            data = {
                    "client_id": "cdse-public",
                    "grant_type": "refresh_token",
                    "refresh_token" : refresh_token
                }
            r = requests.post(
                    "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
                    data=data,
                )
            print(f"New token using refresh token completed: {r.status_code}")
            access_response = r.json()
            access_token = access_response['access_token']
            session.headers.update({"Authorization": f"Bearer {access_token}"})
            data_response = session.get(endpoint_url, verify=False, allow_redirects=True)
            print(f"Authentication with new access code: {data_response.status_code}")

        if data_response.status_code == 400:
            print("Refresh token & Access token has expired...")

        if dir_path is None or dir_path == "": 
            print("data is stored in folder name 'inputdir' " + os.getcwd())
            os.makedirs("./inputdir/")
            file_store_path = "./inputdir/" + tile['Name']+".zip" 
        else: 
            inputdir = os.path.abspath(dir_path)
            ## ?? make path OS-agnostic ?? 
            if platform.system() == "Windows":
                file_store_path = inputdir + "\\" + tile['Name']+".zip"
            elif platform.system() == "Linux":
                file_store_path = inputdir + "/" + tile['Name']+".zip"
        # with open(f"" + tile['Name']+".zip" , "wb") as file:
        with open(f"" + file_store_path , "wb") as file:
            for chunk in data_response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)
        # count = count + 1
        print(f"file with tile ID {tile['Id']} downloaded")
    print(f"Data download is completed in {dir_path}")


def glimpse_catalogue(catalogue_response, n = 5):
    """
    View and get a glimpse of data available in a catalogue.

    -----------------
    Example: 
    dtSat.glimpse_catalogue(catalogue_response, 3)
    """
    return pd.DataFrame.from_dict(catalogue_response['value']).head(n)

def get_date(date_str):
    res = re.search("[0-9]{4}-[0-9]{2}-[0-9]{2}", date_str)
    return res.group(0)

def get_centroid(catalogue_response):
    coordinates = catalogue_response["value"][1]["GeoFootprint"]["coordinates"][0]
    lon = []
    lat = []
    for i in range(len(coordinates)):
        lon.append(coordinates[i][0])
        lat.append(coordinates[i][1])
    return [sum(lon)/len(lon), sum(lat)/len(lat)]

def visualise_catalogue(catalogue_response):
    centroid = get_centroid(catalogue_response)  
    m = folium.Map(location=[centroid[1], centroid[0]], zoom_start=8, tiles="openstreetmap",\
              width=1000,height=500)
    
    for i in range(len(catalogue_response["value"])):
        polygon = catalogue_response["value"][i]["GeoFootprint"]["coordinates"]
        # print(f"{catalogue_response['value'][i]['OriginDate']} : {len(polygon[0])}: id {i}")
        polygon = Polygon(polygon)
        features = []
        features.append(Feature(geometry=polygon))
        feature_collection = FeatureCollection(features)
        aoi_geodf = gpd.GeoDataFrame.from_features(feature_collection["features"])
        
        for _, r in aoi_geodf.iterrows():
            sim_geo = gpd.GeoSeries(r["geometry"]).simplify(tolerance=0.001)
            geo_j = sim_geo.to_json()
            fillColor = np.random.randint(16, 256, size=3)
            fillColor = [str(hex(j))[2:] for j in fillColor]
            fillColor = '#'+''.join(fillColor).upper()
            geo_j = folium.GeoJson(data=geo_j, style_function=lambda x, fillColor=fillColor:  {"fillColor": fillColor})
            marker = folium.Popup((f"id : '{catalogue_response['value'][i]['Id']}'\n"
                                  f"count id : {i}\n"
                                  f"Orbit: {extract_orbit(catalogue_response['value'][i]['Name'])}"))
            marker.add_to(geo_j)
            geo_j.add_to(m)
    return m
    # m.save("wadden_sea.html")
    

def extract_sensing_date(x):
    """
    Utility function for extracting a single date from a catalogue
    
    -----------------
    Example: 
    extract_date(catalogue_response['value'][0]['Name'])
    """
    return re.search("([0-9]+T[0-9]+)", x).group(1)

def extract_orbit(x):
    """
    Utility function for extracting a single orbit from a catalogue
    
    -----------------
    Example:
    extract_orbit(catalogue_response['value'][0]['Name'])
    """
    return re.search("R[0-9]*", x).group(0)

def extract_tile(x):
    """
    Utility function for extracting a single tile from a catalogue
    
    -----------------
    Example:
    extract_orbit(catalogue_response['value'][0]['Name'])
    """
    return re.search("T[0-9]{2}[A-Z]{3}", x).group(0)

def filter_by_orbit(x, orbit, name_only = True):
    """
    Utility function for filtering/subsetting a single orbit from a catalogue
    
    -----------------
    Example:
    filter_by_orbit(catalogue_response, orbit = "R051", name_only = False)
    """
    
    idx = [i for i in range(len(x["value"])) if extract_orbit(x['value'][i]['Name']) in orbit]
    metadata_context = x["@odata.context"]
    if name_only: 
        res = [x['value'][i]['Name']  for i in idx]
    else: 
        res = [x['value'][i] for i in idx]
        if "@odata.nextLink" in x.keys(): 
            metadata_nextlink = x["@odata.nextLink"] 
        else: 
            metadata_nextlink = ""
        res = {'@odata.context' : metadata_context, 
              'value' : res, 
              "@odata.nextLink" : metadata_nextlink}
    return res 

def filter_by_orbit_and_tile(x, orbit, tile, name_only = True):
    """
    Utility function for extracting a single orbit AND tile from a catalogue
    
    -----------------
    Example:
    filter_by_orbit_and_tile(catalogue_response, orbit = "R051", tile = "T31UFV", name_only = False)
    """
    
    idx = [i for i in range(len(x["value"])) if extract_orbit(x['value'][i]['Name']) in orbit and extract_tile(x['value'][i]['Name']) in tile]
    metadata_context = x["@odata.context"]
    if name_only: 
        res = [x['value'][i]['Name']  for i in idx]
    else: 
        res = [x['value'][i] for i in idx]
        if "@odata.nextLink" in x.keys(): 
            metadata_nextlink = x["@odata.nextLink"] 
        else: 
            metadata_nextlink = ""
        res = {'@odata.context' : metadata_context, 
              'value' : res, 
              "@odata.nextLink" : metadata_nextlink}
    return res 

    
def upload_local_directory_to_minio(client = None, bucket_name = None, local_path = None, minio_path = None, collection = "sentinel", year = 2015):
    """
    Utility function for uploading files from local LTER directory to a miniO S3 Bucket 
    
    -----------------
    Example:
    local_path = "../app_acolite/outputdir"
    upload_local_directory_to_minio(client = minio_client,
                               bucket_name = param_s3_public_bucket,  
                               local_path = local_path, 
                               minio_path = f"/acolite_output/", collection = "landsat", year = 2023)
    """
    
    assert os.path.isdir(local_path)
    
    if collection == "sentinel":
        L2W_files = glob.glob(f"{local_path}/{collection}/{year}/**/S2**L2W**") 
    elif collection == "landsat":
        L2W_files = glob.glob(f"{local_path}/{collection}/{year}/**/L8**L2W**")
    else: 
        print("Can only support Sentinel and Landsat files at the moment...")
        return 

    for local_file in L2W_files:
        # print(local_file)
        local_file = local_file.replace(os.sep, "/") # Replace \ with / on Windows
        if not os.path.isfile(local_file):
            upload_local_directory_to_minio(
                local_file, bucket_name, minio_path + "/" + os.path.basename(local_file))
        else:
            remote_path = os.path.join(
                minio_path, local_file[1 + len(local_path):])
            remote_path = remote_path.replace(
                os.sep, "/")  # Replace \ with / on Windows
            client.fput_object(bucket_name, remote_path, local_file)
            
        print(f"{local_file} uploaded to MiniIO....")

def upload_satellite_to_minio(client = None, bucket_name = None, local_path = None, 
                              minio_path = None, collection = "sentinel", year = 2015):
    """
    Utility function for uploading files from sentinel-2 data as zip or unzip file to a miniO S3 Bucket 
    To be integrated with upload_local_directory_to_minio. 
    -----------------
    Example:
    local_path = "../app_acolite/outputdir"
    upload_satellite_to_minio(client = minio_client,
                              bucket_name = param_s3_public_bucket,  
                              local_path = local_path, 
                              minio_path = f"/acolite_output/", collection = "landsat", year = 2023)
    """
    
    L2W_files = glob.glob(f"{local_path}/**")
    
    if any([os.path.splitext(file)[1]  == ".zip" for file in L2W_files]):
        for local_file in L2W_files:
            local_file = local_file.replace(os.sep, "/") # Replace \ with / on Windows
            if not os.path.isfile(local_file):
                upload_satellite_to_minio(client, bucket_name, local_file, minio_path + "/" + os.path.basename(local_file))
            else:
                remote_path = os.path.join(minio_path, local_file[1 + len(local_path):])
                remote_path = remote_path.replace(os.sep, "/")  # Replace \ with / on Windows
                client.fput_object(bucket_name, remote_path, local_file)
            print(f"{local_file} uploaded to MiniIO....")
    else:    
        for local_file in L2W_files:
            local_file = local_file.replace(os.sep, "/") # Replace \ with / on Windows
            if not os.path.isfile(local_file):
                upload_satellite_to_minio(client, bucket_name, local_file, minio_path + "/" + os.path.basename(local_file))
            else:
                remote_path = os.path.join(minio_path, local_file[1 + len(local_path):])
                remote_path = remote_path.replace(os.sep, "/")  # Replace \ with / on Windows
                client.fput_object(bucket_name, remote_path, local_file)
            print(f"{local_file} uploaded to MiniIO....")

def upload_csv_to_minio(client = None, bucket_name = None, local_path = None, 
                              minio_path = None, collection = "sentinel", year = 2015):
    """
    Utility function for uploading files from sentinel-2 data as zip or unzip file to a miniO S3 Bucket 
    To be integrated with upload_local_directory_to_minio. 
    -----------------
    Example:
    local_path = "../app_acolite/outputdir"
    upload_csv_to_minio(client = minio_client,
                        bucket_name = param_s3_public_bucket,  
                        local_path = local_path, 
                        minio_path = f"/acolite_output/", collection = "landsat", year = 2023)
    """
    L2W_files = glob.glob(f"{local_path}/**")
    
    for local_file in L2W_files:
        local_file = local_file.replace(os.sep, "/") # Replace \ with / on Windows
        if not os.path.isfile(local_file):
            upload_satellite_to_minio(client, bucket_name, 
                local_file, minio_path + "/" + os.path.basename(local_file))
        else:
            remote_path = os.path.join(minio_path, local_file[1 + len(local_path):])
            remote_path = remote_path.replace(
                os.sep, "/")  # Replace \ with / on Windows
            client.fput_object(bucket_name, remote_path, local_file)
            
        print(f"{local_file} uploaded to MiniIO....")
        

def download_acolite_from_minio(client = None, bucket_name = "naa-vre-waddenzee-shared", tile = "T31UFU", collection = "sentinel", dir_path = "./", year = 2015):
        """
    Utility function for downloading acolite output files from a miniO S3 Bucket to a local LTER directory 
    
    -----------------
    Example:
    local_path = "../tmp/data"
    download_acolite_from_minio(bucket_name = param_s3_public_bucket,  
                               dir_path = local_path, 
                               tile = "T31UFU", collection = "sentinel", year = 2023)
    """
        objects = client.list_objects(bucket_name, prefix=f"acolite_output/{collection}/{year}",recursive = True)
        for obj in objects:
            filename = dir_path + os.path.basename(obj.object_name)
            if re.search(tile, filename):
                client.fget_object("naa-vre-waddenzee-shared", obj.object_name, filename)
                print(f"File {os.path.basename(obj.object_name)} is downloaded and stored in {dir_path}")