# -*- coding: utf-8 -*-
"""
Created on Fri Dec  4 00:59:26 2020

@author: 6430u
"""

import requests
import json

def check(url):
    response = requests.request("GET", url, data="")
    json_data = json.loads(response.text)
    return json_data


def build():
    url = "http://localhost:9200/final"
    json_data = check(url)

    if(not 'error' in json_data):
        print("Deleted an index: abstract")
        response = requests.request("DELETE", url)

    response = requests.request("PUT", url)
    if (response.status_code == 200):
        print("Created an index: abstract")