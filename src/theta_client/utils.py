import requests
from io import StringIO
import csv
from typing import List

import pandas as pd
import httpx


def get_index_tickers(index_name):
    urls = {
        "sp500": "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
        "dow": "https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average",
        "nasdaq100": "https://en.wikipedia.org/wiki/NASDAQ-100",
        "russell1000": "https://en.wikipedia.org/wiki/Russell_1000_Index",
        "sp400": "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies",
        "sp600": "https://en.wikipedia.org/wiki/List_of_S%26P_600_companies",
    }

    if index_name not in urls:
        raise ValueError(f"Index {index_name} not supported")

    url = urls[index_name]

    # Add headers to avoid 403 error
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    # Use requests to get the page content
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raise an exception for bad status codes

    # Read HTML tables from the response content
    tables = pd.read_html(StringIO(response.text))

    # Different pages have different structures
    if index_name == "sp500":
        return tables[0]["Symbol"].tolist()
    elif index_name == "dow":
        return tables[1]["Symbol"].tolist()
    elif index_name == "nasdaq100":
        return tables[4]["Ticker"].tolist()
    else:
        # Try common column names
        for col in ["Symbol", "Ticker", "Stock Symbol"]:
            if col in tables[0].columns:
                return tables[0][col].tolist()

    return []


def _get_list(url: str, params: dict) -> List[str]:
    response = httpx.get(url, params=params, timeout=60)
    response.raise_for_status()

    # Parse the entire CSV response at once
    reader = csv.reader(StringIO(response.text))
    next(reader)  # Skip the header row

    # Flatten all rows into a single list
    results = [field for row in reader for field in row]

    return results


def get_roots(sec: str = "option") -> List[str]:
    url = f"http://127.0.0.1:25510/v2/list/roots/{sec}"
    params = {"use_csv": "true"}
    return _get_list(url, params)


def get_dates(root: str, sec: str = "stock") -> List[str]:
    url = f"http://127.0.0.1:25510/v2/list/dates/{sec}/quote"
    params = {"use_csv": "true", "root": root}
    return _get_list(url, params)

def get_theta_symbols() -> List[str]:
    url = "http://localhost:25503/v3/option/list/symbols"
    response = httpx.get(url)
    response.raise_for_status()

    reader = csv.DictReader(StringIO(response.text))
    return [row["symbol"] for row in reader]


def get_symbol_universe() -> List[str]:
    theta = get_theta_symbols()
    sp500 = get_index_tickers("sp500")
    rut1k = get_index_tickers("russell1000")
    sp600 = get_index_tickers("sp600")
    sp400 = get_index_tickers("sp400")

    return list(set(sp500 + rut1k + sp600 + sp400).intersection(set(theta)))