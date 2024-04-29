# Importing Necessary dependencies
import requests
import pandas as pd
import json

def run_extraction():
    # Extraction Layer
    url = "https://realty-mole-property-api.p.rapidapi.com/randomProperties"

    querystring = {"limit":"500"}

    headers = {
        "X-RapidAPI-Key": "5a3613b421mshab2a25ab8756075p148395jsn6b0083cd9efc",
        "X-RapidAPI-Host": "realty-mole-property-api.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)

    data = response.json()

    filename = 'real_estate.json'

    with open(filename, 'w') as file:
        json.dump(data, file, indent=4)