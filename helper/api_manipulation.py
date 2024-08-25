import requests
import pandas as pd
from helper.string_manipulation import join_stock_string
from helper.config import load_config
from datetime import datetime,timedelta
import subprocess
import json
import random

from typing import Dict,List

def params_dict_to_string(params_dict: Dict) -> str:
    """
    Converts a nested dictionary of parameters into a string format suitable for API requests.

    Parameters:
        params_dict (dict): A nested dictionary containing the parameters to be converted.

    Returns:
        str: A string representation of the parameters in the format key1=value1&key2=value2...

    Example:
        params_dict = {'sort': {'field1': 'asc', 'field2': 'desc'}, 'filter': {'date': '2022-01-01'}}
        params_string = params_dict_to_string(params_dict)
        # Output: 'sort=field1:asc~field2:desc&filter=date:2022-01-01'
    """
    if not isinstance(params_dict, dict):
        raise TypeError("Input `params_dict` must be a dictionary.")
    
    params_dict_change = dict(params_dict)
    # Iterate over the nested dictionary to handle complex structures
    for k1,v1 in list(params_dict_change.items()):
        # Iterate over the inner dictionary to process its elements
        if isinstance(v1,dict):
            for k2,v2 in list(v1.items()):
                if isinstance(v2,dict):
                    v1[k2] = '~'.join([str(k2) +':'+ str(k3) +':'+str(v3) for k3,v3 in v2.items()])
                    v1[''] = v1[k2]
                    del v1[k2]

    for k1,v1 in params_dict_change.items(): # sort,date
        if isinstance(v1,dict):    
            params_dict_change[k1] = '~'.join([str(v2) if k2 == '' else str(k2) +':'+str(v2) for k2,v2 in v1.items()])
            
    params_dict_change = '&'.join([str(k1) + '=' + str(v1) for k1,v1 in params_dict_change.items()])

    return params_dict_change

def api_to_pandas(baseURL: str, endpoint: str, params_dict: Dict, headers: Dict, timeout: int = 10, use_proxy: bool = False, rerun_proxy: bool = True, proxy_list_filter: str = 'proxy\proxy_list_filter.txt') -> pd.DataFrame:
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
        proxy_list_filter (str, optional): The file containing the list of proxies. Defaults to 'proxy\proxy_list_filter.txt'.

    Returns:
        pandas.DataFrame: A DataFrame containing the fetched data from the API endpoint.
    """

    if use_proxy:
        # Rerun the proxy script to update the proxy list
        if rerun_proxy:
            subprocess.run(['python', 'proxy\proxy.py'])

        # Read the proxy list from the file
        with open(proxy_list_filter, 'r') as f:
            proxies = [line.strip() for line in f.readlines()]
            if not proxies:
                raise ValueError("proxy_list_filter is empty. Please check the proxy list file at proxy\proxy_list_filter.txt and proxy\proxy_list_raw.txt.")
        proxy_index = 0

    page_num = 1
    df = pd.DataFrame()

    while True:
        if use_proxy:
            # Use a proxy for the request
            if proxy_index >= len(proxies):
                print(f"All proxies failed for {response.url}. Plase check the proxy list file at proxy\proxy_list_filter.txt and proxy\proxy_list_raw.txt.")
                break

            proxy = proxies[proxy_index]
            proxies_dict = {
                "http": proxy,
                "https": proxy
            }
            try:
                response = requests.get(
                    f"{baseURL}{endpoint}?{params_dict_to_string(params_dict)}&page={page_num}",
                    headers=headers,
                    proxies=proxies_dict,
                    timeout=timeout
                )
                try:
                    response.json()
                except json.JSONDecodeError:
                    print(f"Error decode json from proxy {proxy}:{response.url}, trying next proxy") 
                    proxy_index += 1               
            except requests.RequestException as e:
                print(f"Proxy {proxy} failed: {e}, trying next proxy")
                proxy_index += 1

            try:
                response.json()
            except json.JSONDecodeError:
                print(f"Error decode json from proxy {proxy}:{response.url}, trying next proxy")
                proxy_index += 1

        else:
            # Make the request without a proxy
            response = requests.get(
                f"{baseURL}{endpoint}?{params_dict_to_string(params_dict)}&page={page_num}",
                headers=headers,
                timeout=timeout
            )
            try:
                response.json()
            except json.JSONDecodeError:
                print(f"Error: {response.status_code} {response.reason} on page {page_num} {response.url}")
                break # break if error

        status_code = response.status_code
        url = response.url
        response_data = response.json()['data']
        if page_num == 1:
            total_pages = response.json()['totalPages']

        df_partition = pd.json_normalize(response_data)
        df = pd.concat([df, df_partition])

        if use_proxy:
            print(f"Getting page {page_num}/{total_pages}, status: {status_code}, url: {url}, proxy: {proxy}")
        else:
            print(f"Getting page {page_num}/{total_pages}, status: {status_code}, url: {url}")

        if page_num >= total_pages:
            break

        page_num += 1

    return df

def gen_arguments(endpoint: str, symbol: List[str] = None, floor: List[str] = None, sort: str = None, size: int = 9999,
                  from_date: str = None, to_date: str = None, 
                  from_effective_date: str = None, to_effective_date: str = None,
                  baseURL: str = 'https://finfo-api.vndirect.com.vn',
                  list_user_agent: str = 'helper/list_user_agent.txt',
                  ) -> Dict:
    """
    Generates a dictionary of arguments for making API requests to retrieve stock prices based on the provided parameters.

    Args:
    - endpoint (str): The endpoint for retrieving stock prices. Defaults to '/v4/stock_prices'.
    - symbol (List[str], optional): A list of stock symbols to filter the results. Defaults to None.
    - floor (List[str], optional): A list of stock floor codes to filter the results. Defaults to None.
    - sort (str, optional): The sorting parameter for the results. Defaults to 'date'.
    - size (int, optional): The maximum number of results to retrieve. Defaults to 9999.
    - from_date (str, optional): The start date for filtering stock prices. Defaults to None.
    - to_date (str, optional): The end date for filtering stock prices. Defaults to None.
    - from_effective_date (str, optional): The start effective date for filtering stock prices. Defaults to None.
    - to_date (str, optional): The end effective date for filtering stock prices. Defaults to None.
    - baseURL (str, optional): The base URL of the API. Defaults to 'https://finfo-api.vndirect.com.vn'.
    - list_user_agent (str, optional): The file containing the list of user agents. Defaults to 'helper/list_user_agent.txt'.
    - filename (str, optional): The name of the configuration file containing headers. Defaults to 'helper/headers.ini'.
    - section (str, optional): The section in the configuration file containing headers. Defaults to 'vnd_headers'.

    Returns:
    - Dict: A dictionary containing the arguments required for making the API request, including baseURL, endpoint, params_dict, and headers.

    Raises:
    - ValueError: If either 'from_date' or 'to_date' is missing when the other is provided.

    Note:
    - This function relies on the 'join_stock_string' function from 'helper.string_manipulation' and the 'load_config' function from 'helper.config' for processing symbols and loading headers, respectively.
    """
    params_dict = {}

    if sort is not None:
        params_dict['sort'] = sort

    if size is not None:
        params_dict['size'] = size

    if symbol is not None:
        params_dict.setdefault('q', {})['code'] = join_stock_string(symbol)

    if floor is not None:
        params_dict.setdefault('q', {})['floor'] = join_stock_string(floor)

    if from_date is not None and to_date is not None:
        params_dict.setdefault('q', {})['date'] = {'gte': from_date, 'lte': to_date}
    elif from_date is None and to_date is not None:
        raise ValueError('need parameter: from_date')
    elif from_date is not None and to_date is None:
        raise ValueError('need parameter: to_date')
    
    if from_effective_date is not None and to_effective_date is not None:
        params_dict.setdefault('q', {})['effectiveDate'] = {'gte': from_effective_date, 'lte': to_effective_date}
    elif from_effective_date is None and to_effective_date is not None:
        raise ValueError('need parameter: from_effective_date')
    elif from_effective_date is not None and to_effective_date is None:
        raise ValueError('need parameter: to_effective_date')
    
    # List user agents
    with open(list_user_agent, 'r') as f:
        user_agents = f.read().split('\n')

    headers = {
        'User-Agent': random.choice(user_agents),
        'Content-Type': 'application/json',
    }

    return {
        'baseURL': baseURL,
        'endpoint': endpoint,
        'params_dict': params_dict,
        'headers': headers,
    }

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

def load_arguments_dict(table):
    """
    Load arguments dictionary for a specific table from a configuration file.

    Parameters:
    table (str): The name of the table to load arguments for.

    Returns:
    dict: A dictionary containing the arguments for the specified table.

    Raises:
    Exception: If the specified table is not found in the configuration file.
    """
    config = load_config(filename='helper/tables.ini',section=table)

    # Assign variables to globals
    for key,val in config.items():
        print(f'exec(): {key} = {val}')
        exec(key + '=' + val, globals())
    
    arguments_dict = globals()['arguments_dict']

    return arguments_dict

if __name__ == '__main__':
    table = 'factless_stock_events'
    arguments_dict = load_arguments_dict(table=table)
    baseURL,endpoint,params_dict,headers=arguments_dict.values()
    use_proxy=True
    proxy_list_filter='proxy\proxy_list_filter.txt'
    timeout=10
    api_to_pandas(**arguments_dict,use_proxy=True)