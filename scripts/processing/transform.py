import pandas as pd
import re
import json

from pathlib import Path
import sys

# Add the project root to sys.path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

# Import functions from the extract module
from scripts.api.extract import get_all_stock_symbols, get_forbes_global_500, get_stock_data


# Transform stock symbol data
def transform_stock_symbols(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_symbols', key='stock_symbols')

    df_stock_symbols = pd.json_normalize(data)

    columns = ["symbol", "company_name", "industry_sector", "market_cap"]
    df_stock_symbols.columns = columns

    # Apply the normalization function
    df_stock_symbols["company_name"] = df_stock_symbols["company_name"].apply(normalize_company_name)

    df_stock_symbols.dropna(inplace=True)
    df_stock_symbols.reset_index(drop=True, inplace=True)

    ti.xcom_push(key='transformed_stock_symbols', value=df_stock_symbols)
    return df_stock_symbols


# Transform Forbes Global 500 data
def transform_forbes_global_500(**kwargs):
    ti = kwargs['ti']
    nested_data = ti.xcom_pull(task_ids='extract_global500', key='global500')

    df_global500 = pd.concat(
        [pd.json_normalize(item["items"]).assign(year=item["year"]) for item in nested_data],
        axis=0
    )

    # Select relevant columns and rename them
    df_global500 = df_global500[[
        "name", "rank", "data.Revenues ($M)", "data.Industry", "data.Country / Territory", "year"
    ]]
    df_global500.columns = [
        "company_name", "rank_position", "annual_revenue", "industry_sector", "country_of_origin", "date"
    ]

    # Apply normalization on company names
    df_global500["company_name"] = df_global500["company_name"].apply(normalize_company_name)

    # Data cleaning
    df_global500["date"] = pd.to_datetime(df_global500["date"], format='%Y')
    df_global500['country_of_origin'] = df_global500['country_of_origin'].fillna("Unknown")
    df_global500["annual_revenue"] = df_global500["annual_revenue"].replace(
        {r'\$': '', r',': ''}, regex=True).astype(float)
    df_global500.reset_index(drop=True, inplace=True)

    ti.xcom_push(key='transformed_global500', value=df_global500)
    return df_global500


# Transform stock market data
def transform_stock_market_data(**kwargs):
    ti = kwargs['ti']
    nested_data = ti.xcom_pull(task_ids='extract_stock_data', key='stock')

    data = []
    # Flatten nested data
    for stock_data in nested_data:
        for stock, daily_data in stock_data.items():
            for date, metrics in daily_data.items():
                record = {"symbol": stock, "date": date, **metrics}
                data.append(record)

    # Create DataFrame
    df_stock_data = pd.DataFrame(data)
    numeric_columns = ["1. open", "2. high", "3. low", "4. close", "5. volume"]
    df_stock_data[numeric_columns] = df_stock_data[numeric_columns].apply(pd.to_numeric)

    # Rename columns for consistency
    df_stock_data.rename(columns={
        "date": "date",
        "symbol": "symbol",
        "1. open": "open",
        "2. high": "high",
        "3. low": "low",
        "4. close": "close",
        "5. volume": "volume"
    }, inplace=True)

    # Clean currency symbols
    columns_to_clean = ['close', 'open', 'high', 'low']
    df_stock_data[columns_to_clean] = df_stock_data[columns_to_clean].replace({r'\$': ''}, regex=True).astype(float)

    df_stock_data.reset_index(drop=True, inplace=True)
    ti.xcom_push(key='transformed_stock', value=df_stock_data)
    return df_stock_data


# Normalize company names
def normalize_company_name(name):
    # Apply normalization using regex substitution
    normalize_pattern = r'( Corporation|, Inc\.|, ltd\.|ltd\.| Inc\.|.com| systems| Incorporated)'
    return re.sub(normalize_pattern, '', name, flags=re.IGNORECASE).strip().lower()
