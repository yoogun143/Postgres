import requests
import pandas as pd
from helper.string_manipulation import join_stock_string
from helper.config import load_config

from typing import Dict

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


def api_to_pandas(baseURL: str, endpoint: str, params_dict: Dict, headers: Dict, timeout: int = 10) -> pd.DataFrame:
    """
    Fetches data from an API endpoint and converts it into a pandas DataFrame.

    Parameters:
        baseURL (str): The base URL of the API.
        endpoint (str): The specific endpoint to fetch data from.
        params_dict (dict): A nested dictionary containing the parameters to be included in the API request.
        headers (dict): A dictionary containing the headers to be included in the API request.
        timeout (int, optional): The timeout value for the API request in seconds. Defaults to 10.

    Returns:
        pandas.DataFrame: A DataFrame containing the fetched data from the API endpoint.

    Example:
        df = api_to_pandas('https://api.example.com/', 'data', {'sort': {'field1': 'asc', 'field2': 'desc'}, 'filter': {'date': '2022-01-01'}}, {'Authorization': 'Bearer token'})
    """
    page_num = 1
    df = pd.DataFrame()

    while True:
        response = requests.get(
            f"{baseURL}{endpoint}?{params_dict_to_string(params_dict)}&page={page_num}",
            headers=headers,
            timeout=timeout
        )

        status_code = response.status_code
        url = response.url
        response_data = response.json()['data']
        if page_num == 1:
            total_pages = response.json()['totalPages']

        df_partition = pd.json_normalize(response_data)
        df = pd.concat([df, df_partition])

        print(f"Getting page {page_num}/{total_pages}, status: {status_code}, url: {url}")
        if page_num >= total_pages:
            break

        page_num += 1

    return df

def api_arguments_date_filter(symbol: str, from_date: str, to_date: str, baseURL: str = 'https://finfo-api.vndirect.com.vn',
                              endpoint: str = '/v4/stock_prices', filename: str = 'helper/headers.ini',
                              section: str = 'vnd_headers') -> Dict:
    """
    Converts the input parameters into a dictionary suitable for making API requests to retrieve stock prices within a specified date range.

    Parameters:
        symbol (str): The stock symbol for which the prices are to be retrieved.
        from_date (str): The start date of the date range for which prices are to be retrieved (format: 'YYYY-MM-DD').
        to_date (str): The end date of the date range for which prices are to be retrieved (format: 'YYYY-MM-DD').
        baseURL (str, optional): The base URL of the API. Default is 'https://finfo-api.vndirect.com.vn'.
        endpoint (str, optional): The endpoint for retrieving stock prices. Default is '/v4/stock_prices'.
        filename (str, optional): The path to the configuration file containing headers. Default is 'helper/headers.ini'.
        section (str, optional): The section in the configuration file containing headers. Default is 'vnd_headers'.

    Returns:
        dict: A dictionary containing the necessary arguments for making the API request.

    Example:
        arguments_dict = api_arguments_date_filter('AAPL', '2022-01-01', '2022-01-31')
        # Output: {'baseURL': 'https://finfo-api.vndirect.com.vn', 'endpoint': '/v4/stock_prices', 'params_dict': {'sort': 'date', 'q': {'code': 'AAPL', 'date': {'gte': '2022-01-01', 'lte': '2022-01-31'}}, 'size': 10}, 'headers': {...}}
    """
    stock_string = join_stock_string(symbol)
    params_dict = {
        # gte – greater than or equal Value
        # gt – greater than Value
        # lt – less than Value
        # lte – less than or equal Value
        # gsd – ground sample distance  
        'sort': 'date',
        'q':{
            'code':stock_string,
            'date':{
                'gte':from_date,
                'lte':to_date
            }
        },
        'size':10
    }
    headers = load_config(filename,section)

    arguments_dict = {
        'baseURL':baseURL,
        'endpoint':endpoint,
        'params_dict':params_dict,
        'headers':headers,
    }

    return arguments_dict