from django.shortcuts import render
import zipfile
import pandas as pd
import redis
import requests
from bs4 import BeautifulSoup
import os
import urllib.request
from django.http import HttpResponse
# Create your views here.
DOWNLOAD_URL = "https://www.bseindia.com/markets/MarketInfo/BhavCopy.aspx"

def download_copy(DOWNLOAD_URL):
    page = requests.get(DOWNLOAD_URL)
    soup = BeautifulSoup(page.text, "html.parser")
    print(soup)
    tag = soup.find(id='ContentPlaceHolder1_btnhylZip')
    link = tag.get('herf',None)
    filepath = os.path.join(os.getcwd(),
                            'bhavacopy',
                            'download_data')
    urllib.request.urlretrieve(link, filepath)
    return HttpResponse('page')

def extract_copy_zip():
    bhavcopy_dir = os.path.join(os.getcwd(),
                                "bhavacopy",
                                "download_data")
    bhavcopy_zip = os.path.join(bhavcopy_dir, "bhavcopy.zip")
    zip_ref = zipfile.ZipFile(bhavcopy_zip, 'r')
    filename = zip_ref.namelist()[0]
    zip_ref.extractall(bhavcopy_dir)
    zip_ref.close()
    return filename

def redis_conn():
    return redis.StrictRedis(host="localhost",
                             db=1,
                             charset='utf-8',
                             decode_responses=True)

def store_bhavcopy_data(equity_filename):
    conn = redis_conn()
    csv_data = pd.read_csv(os.path.join(os.getcwd(),
                                        "bhavacopy",
                                        "download_data",
                                        equity_filename))
    csv_data = csv_data[['SC_CODE', 'SC_NAME', 'OPEN', 'HIGH', 'LOW', 'CLOSE']].copy()

    conn.flushall()
    for index, row in csv_data.iterrows():
        conn.hmset(row['SC_CODE'], row.to_dict())

def get_latest_equity():
    results = []
    conn = redis_conn()
    keys = conn.keys('*')

    for equity in keys:
        results.append(conn.hgetall(equity))

    return results[:10]