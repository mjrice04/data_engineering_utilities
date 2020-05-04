import requests
import pandas as pd
import numpy as np
import json
import os


def address_reader(path='data/address_data_example.csv'):
    """
    This function reads the address data and returns a dataframe of the addresses
    :param path: path to the data
    :return:
    """
    address_csv = pd.read_csv(path, dtype={'Address': 'str', 'City': 'str', 'State': 'str',
                                           'Zip': 'str', 'Comment': 'str'})
    return address_csv


def get_lat_long(address_csv):
    """
    makes a call to the google maps API service and returns a list of lat, long with an ID
    :param address_csv: input address csv
    :return:
    """
    lat_long_list = []
    for index, row in address_csv.iterrows():
        print(row)
        street = row['Address']
        city = row['City']
        state = row['State']
        zipcode = row['Zip']
        comment = row['Comment']
        clean = lambda x: '+'.join(x.split(' '))
        clean_street = clean(street)
        clean_city = clean(city)
        URL = f"https://maps.googleapis.com/maps/api/geocode/json?address={clean_street},+{clean_city},+{state}+{zipcode}&key={os.environ['MAPS_API_KEY']}"
        response = requests.get(URL)
        json_response = json.loads(response.text)
        try:
            lat = json_response['results'][0]['geometry']['location']['lat']
        except:
            lat = 0
        try:
            lng = json_response['results'][0]['geometry']['location']['lng']
        except:
            lng = 0
        lat_long_list.append(f"{index},{lat},{lng},{comment}")
    return lat_long_list

def lat_long_to_frame(lat_long_list):
    """
    creates pandas dataframe out of list of latitudes and longitudes
    :param lat_long_list: list of lat and long
    :return:
    """
    lat_list = []
    lng_list = []
    index_list = []
    comment_list = []
    for i in lat_long_list:
        split = i.split(',')
        index_list.append(split[0])
        lat_list.append(split[1])
        lng_list.append(split[2])
        comment_list.append(split[3])
    dict = {'ID': index_list, 'LAT': lat_list, 'LNG': lng_list, 'Comment': comment_list}
    df_lng_lat = pd.DataFrame(dict)
    return df_lng_lat

def lat_long_data_handler():
    """
    runs logic for parsing and returning long and lat frame based on input data csv
    :return:
    """
    address_csv = address_reader()
    lat_long = get_lat_long(address_csv=address_csv)
    lat_long_frame = lat_long_to_frame(lat_long_list=lat_long)
    print(lat_long_frame)
    return lat_long_frame


if __name__ == "__main__":
    lat_long_data_handler()
