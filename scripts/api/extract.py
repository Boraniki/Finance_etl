from alpha_vantage.timeseries import TimeSeries
from bs4 import BeautifulSoup
from time import sleep
from pathlib import Path
import pandas as pd
import re
import requests
import json


def get_all_stock_symbols(**kwargs):
    base_url = "https://api.stockanalysis.com/api/screener/s/f"
    headers = {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "origin": "https://stockanalysis.com",
        "referer": "https://stockanalysis.com/",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    }
    
    all_companies = []
    page = 1  # Start from the first page
    companies_per_page = 1000  # Adjust based on API documentation (1000 max per request)

    for i in range(6):
        print(f"Fetching page {page}...")
        params = {
            "m": "s",  # Sorting metric (e.g., stock name)
            "s": "asc",  # Sort order (ascending)
            "c": "s,n,industry,marketCap",  # Columns: symbol, name, industry, market cap
            "cn": companies_per_page,  # Number of companies per page
            "p": page,  # Current page
            "i": "stocks",  # Data type
        }

        response = requests.get(base_url, headers=headers, params=params)
        if response.status_code != 200:
            print(f"Failed to fetch data: {response.status_code}")
            break

        data = response.json()
        
        results = data.get("data").get("data")
        if not results:
            print("No more data available.")
            break
        # Append the current page's data
        all_companies.extend(results)
        page += 1  # Go to the next page

    print(f"Total companies fetched: {len(all_companies)}")

    ti = kwargs["ti"]
    ti.xcom_push(key='stock_symbols', value=all_companies)
    return all_companies

def get_forbes_global_500(**kwargs):
    base_url = f"https://fortune.com/api/getRankingSearchYear/global500/"
    
    headers = {
        "accept": "*/*",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "referer": "https://fortune.com/ranking/global500/",
    }
    all_years = []
    years = range(1999,2025)
    for year in years:
        url = base_url + str(year)
        print(url)
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            all_years.append(data)
        else:
            print(f"Failed to fetch data: HTTP {response.status_code}")
            print(response.text)
    kwargs['ti'].xcom_push(key='global500', value=all_years)
    return all_years

def get_stock_data(**kwargs):
    DATA_DIR = Path(__file__).resolve().parent.parent.parent / "config"
    with open(DATA_DIR / "api-keys.txt", "r", encoding="utf-8") as file:
        content = file.read()

    pattern = r"alpha_vantage_key:([A-Z0-9]+)"
    match = re.search(pattern, content)
    if match:
        API_KEY = match.group(1)

    ts = TimeSeries(key=API_KEY, output_format="json")

    stock_data = list()
    get_stock_tickers = get_top_twentyfive_companies()
    stock_tickers = get_stock_tickers["s"].to_list()[:25]
    for stock_ticker in stock_tickers:
        data, meta_data = ts.get_daily(symbol=stock_ticker, outputsize="full")
        stock_data.append({meta_data.get("2. Symbol"):data})
        sleep(5)

    kwargs['ti'].xcom_push(key='stock', value=stock_data)
    return stock_data

def get_top_twentyfive_companies():
    DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data" 
    all_data_df = pd.read_json(DATA_DIR / "all_companies.json")
    top_50 = all_data_df.nlargest(50, 'marketCap').reset_index(drop=True)
    return top_50
