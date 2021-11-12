#!/usr/bin/env python
# coding: utf-8


import healpy as hp
import pandas as pd
import numpy as np
import os
import csv
import time
import errno
import httplib2
from bs4 import BeautifulSoup, SoupStrainer
from csv import writer


http = httplib2.Http()

def parse_source_data(url='https://cdn.gea.esac.esa.int/Gaia/gdr2/gaia_source/csv/'):
    """
    This function parses the source url 'https://cdn.gea.esac.esa.int/Gaia/gdr2/gaia_source/csv/'
    for .csv.gz files and appends them to the list

    :param url: the source url from where the Gaia data needs to be downloaded
    :return: list object with file names
    """
    csv_files = []

    try:
        print("Attempting to establish connection with the Data Source")
        status, response = http.request('https://cdn.gea.esac.esa.int/Gaia/gdr2/gaia_source/csv/')
        print("Succesfully created the connection")

    except requests.exceptions.HTTPError as err:
        raise SystemExit(err)
    
    print("**********Parsing STARTED *******")
    for link in BeautifulSoup(response, parse_only=SoupStrainer('a')):
        
        if link.has_attr('href'):
            if (link['href'].endswith('.csv.gz')):
                abs_path = url + link['href']
                csv_files.append(abs_path)
            else:
                pass
    print("**********Parsing ENDED *********")
    #return csv_files
    return pd.DataFrame(csv_files, columns=['Gaia_file_paths'])

file_paths = parse_source_data()

def to_hips(long,lat):
    """
    :param long: ra in degrees
    :param lat: dec in degrees
    :return: pix path
    """
    path_name = 'Norder{k}/Npix{pix}/catalog.csv'.format(k=5,pix = hp.ang2pix(theta=long, phi=lat, nside=2**5, lonlat=True))
    return path_name

def csv_to_hips_2(df_csv, k=5):
    """

    :param df_csv: pandas dataframe containing gaia data paths
    :param k: Nside
    :return: None
    """
    for x in range(0, len(df_csv)):

        try:
            df_csv_file = pd.read_csv(df_csv['Gaia_file_paths'][x])
            df_csv_file['pix'] = df_csv_file.apply(lambda row: to_hips(row['ra'], row['dec']), axis=1)
            unique_paths = df_csv_file['pix'].unique().tolist()
            
        except FileNotFoundError:
            print("Gaia File not found.")

        for paths in unique_paths:

            df_temp = df_csv_file[df_csv_file['pix'] == paths]
            hips_path = paths.split('cata')[0]
            print(hips_path)

            dir_path = '/home/obaiddawarki/Gaia_Data_Analysis/' + hips_path

            if not os.path.exists(dir_path):
                try:
                    print("Attempting to create directory {}".format(dir_path))
                    os.makedirs(dir_path)
                    print("Succesfully created directory {}".format(dir_path))
                    abs_path = dir_path + 'catalog.csv'
                    df_temp.to_csv(abs_path, index=False)
                except OSError as e:
                    if e.errno != errno.EEXIST:
                        raise  # raises the error again
            else:
                abs_path = dir_path + 'catalog.csv'
                df_temp.to_csv(abs_path, mode='a', header=False)
                

test_data  = file_paths.head(100)
csv_to_hips_2(df_csv=test_data, k=5)



