import requests
import pandas as pd
# from helper.string_manipulation import join_stock_string
# from helper.config import load_config
from datetime import datetime,timedelta
import subprocess
import json
import random
import yaml
import urllib.parse

from typing import Dict

def flatten_dict(d, parent_key='') -> str:
    """
    Flatten a nested dictionary into a tilde (~) separated string with colons (:) for keys.
    
    Parameters:
        d (dict): The dictionary to flatten.
        parent_key (str, optional): The parent key for the current level of the dictionary. Defaults to ''.
    
    Returns:
        str: A flattened string with tilde (~) and colons (:) separators.

    Example:
        >>> flatten_dict({
        "floor": "HOSE,HNX,UPCOM,OTC",
        "date": {
            "gte": "2025-03-27",
            "lte": "2025-03-27"
            }
        })
        'floor:HOSE,HNX,UPCOM,OTC~date:gte:2025-03-27~date:lte:2025-03-27'
    """
    parts = []
    for k, v in d.items():
        new_key = f"{parent_key}:{k}" if parent_key else k
        if isinstance(v, dict):
            parts.append(flatten_dict(v, new_key))
        else:
            parts.append(f"{new_key}:{v}")
    return "~".join(parts)

def api_to_pandas(baseURL: str, endpoint: str, params_dict: Dict, headers: Dict, timeout: int = 10, use_proxy: bool = False, rerun_proxy: bool = True, proxy_list_filter: str = 'proxy/proxy_list_filter.txt') -> pd.DataFrame:
    """
    Fetches data from an API endpoint and converts it into a pandas DataFrame.

    Parameters:
        baseURL (str): The base URL of the API.
        endpoint (str): The specific endpoint to fetch data from.
        params_dict (dict): A nested dictionary containing the parameters to be included in the API request.
        headers (dict): A dictionary containing the headers to be included in the API request.
        timeout (int, optional): The timeout value for the API request in seconds. Defaults to 10.
        use_proxy (bool, optional): Whether to use a proxy or not. Defaults to False.
        rerun_proxy (bool, optional): Whether to rerun the proxy script to update the proxy list. Defaults to True.
        proxy_list_filter (str, optional): The file containing the list of proxies. Defaults to 'proxy/proxy_list_filter.txt'.

    Returns:
        pandas.DataFrame: A DataFrame containing the fetched data from the API endpoint.
    """

    if use_proxy:
        # Rerun the proxy script to update the proxy list
        if rerun_proxy:
            subprocess.run(['python', 'proxy/proxy.py'])

        # Read the proxy list from the file
        with open(proxy_list_filter, 'r') as f:
            proxies = [line.strip() for line in f.readlines()]
            if not proxies:
                raise ValueError("proxy_list_filter is empty. Please check the proxy list file at proxy/proxy_list_filter.txt and proxy/proxy_list_raw.txt.")

    page_num = 1
    df = pd.DataFrame()

    # Convert nested dictionary to required format, keep one layer only
    for key, value in params_dict.items():
        if isinstance(value, dict):
            params_dict[key] = flatten_dict(value)

    total_pages = None
    while True:
        params_dict['page'] = page_num

        try:
            response = requests.get(
                f"{baseURL}{endpoint}",
                params=params_dict,
                headers=headers,
                timeout=timeout,
                proxies=proxies[0] if use_proxy else None
            )
            status_code = response.status_code
            url = urllib.parse.unquote(response.url)
            response_data = response.json().get('data', [])
            total_pages_info = response.json().get('totalPages')

            if total_pages_info:
                print(f"Total pages: {total_pages_info}")

            if not response_data:
                print(f"API returned no data on page {page_num} {url}")
                break

            df_partition = pd.json_normalize(response_data)
            df = pd.concat([df, df_partition])

            if use_proxy:
                print(f"Getting page {page_num}, status: {status_code}, url: {url}, proxy: {proxies[0]}")
            else:
                print(f"Getting page {page_num}, status: {status_code}, url: {url}")

            if total_pages is None:
                total_pages = total_pages_info if total_pages_info else (page_num + 1 if len(response_data) > 0 else page_num)

            if page_num >= total_pages:
                break

            page_num += 1

        except requests.RequestException as e:
            if use_proxy:
                proxy_index = 1
                while proxy_index < len(proxies):
                    proxy = proxies[proxy_index]
                    try:
                        response = requests.get(
                            f"{baseURL}{endpoint}",
                            params=params_dict,
                            headers=headers,
                            timeout=timeout,
                            proxies={ "http": proxy, "https": proxy }
                        )
                        break
                    except requests.RequestException as e:
                        print(f"Proxy {proxy} failed: {e}, trying next proxy")
                        proxy_index += 1

                if proxy_index == len(proxies):
                    print(f"All proxies failed for {url}. Please check the proxy list file at proxy/proxy_list_filter.txt and proxy/proxy_list_raw.txt.")
                    break
            else:
                print(f"Error: {e}, breaking")
                break

    if df.empty:
        raise ValueError("API returned no data")

    return df

def gen_api_config(file_path: str = 'helper/tables.yaml', list_user_agent: str = 'helper/list_user_agent.txt', content_type: str = 'application/json') -> Dict[str, dict]:
    """
    Reads a YAML file at the given file path and returns the processed configuration.

    The configuration is processed by replacing 'function' with calculated dates and adding a 'headers' field that contains a randomly chosen user agent.

    Parameters:
    - file_path (str): Path to the YAML file to read.
    - list_user_agent (str): Path to the file containing the list of user agents (default is 'helper/list_user_agent.txt').

    Returns:
    - A dictionary containing the processed configuration.
    """
    with open(file_path, 'r') as f:
        config = yaml.safe_load(f)

    # Process the config and replace 'function' with calculated dates
    for key, value in config.items():
        # Get the 'params_dict' from the configuration
        params_dict = value.get('params_dict', {})

        # Look for 'q' key (query parameters)
        query_params = params_dict.get('q', {})

        # Iterate through all keys to find 'date' or other date-related fields
        for sub_key, sub_value in query_params.items():
            if isinstance(sub_value, dict):  # Check for nested dictionaries like 'date' or 'effectiveDate'
                for date_key, date_value in sub_value.items():
                    if isinstance(date_value, int):
                        if date_key == 'gte':
                            # Replace 'gte' with the calculated date
                            sub_value[date_key] = add_days_from_today(days=date_value)
                        elif date_key == 'lte':
                            # Replace 'lte' with the calculated date
                            sub_value[date_key] = add_days_from_today(days=date_value)

        # List user agents
        with open(list_user_agent, 'r') as f:
            user_agents = f.read().split('\n')

        # Create a dictionary with a randomly chosen user agent
        headers = {
            'User-Agent': random.choice(user_agents),
            'Content-Type': content_type,
        }

        # Add the 'headers' dictionary to the configuration
        value['headers'] = headers

    # Return the processed configuration
    return config

def add_days_from_today(date_format: str = '%Y-%m-%d', days: int = 0) -> str:
    """
    Return a formatted date string based on the current date and an optional number of days to add.

    Parameters:
    - date_format (str): Format of the output date string (default is '%Y-%m-%d').
    - days (int): Number of days to add to the current date (default is 0).

    Returns:
    - str: Formatted date string based on the current date and the added days.
    """
    date_added = datetime.now() + timedelta(days=days)

    return date_added.strftime(date_format)

if __name__ == '__main__':
    fk_date = datetime.now().strftime('%Y%m%d')
    schema = 'vnd'

    # List table to run flow
    table = 'example'

    arguments_dict = gen_api_config()[table]
    print(arguments_dict)
    print('-'*40)
    print(arguments_dict['params_dict'])
    print('-'*40)
    print(arguments_dict['params_dict']['q'])
    print('-'*40)
    
    api_to_pandas(
            # baseURL=arguments_dict['baseURL']
            # , endpoint=arguments_dict['endpoint']
            # , params_dict=arguments_dict['params_dict']
            # , headers=arguments_dict['headers'],
            **arguments_dict, ##can be used instead of extract key-value from arguments_dict
            timeout=10,
            use_proxy=False,
            rerun_proxy=False
            )