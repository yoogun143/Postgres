from typing import List
from typing import Any
from typing import Dict
import psycopg2
import psycopg2.extras as extras
from psycopg2.extensions import AsIs

import pandas as pd
import os

from helper.config import load_config
from helper.string_manipulation import sql_to_list,get_column_name_from_create_table
from helper.api_manipulation import api_to_pandas,gen_arguments

# # test strng
# from datetime import datetime
# schema='vnd'
# table='dim_symbol'
# fk_date = datetime.now().strftime('%Y%m%d')
# floor=['HOSE','HNX','UPCOM','OTC']
# endpoint= '/v4/stocks'
# arguments_dict=gen_arguments(endpoint=endpoint,floor=floor)

def api_to_csv(arguments_dict: Dict, schema: str, table: str, fk_date: str) -> None:
    """
    Fetches data from an API endpoint and exports it to a CSV file.

    Parameters:
        arguments_dict (dict): A dictionary containing the arguments required for the API request, including 'baseURL', 'endpoint', 'params_dict', and 'headers'.
        schema (str): The schema name for the CSV file.
        table (str): The table name for the CSV file.
        fk_date (str): The foreign key date used in the CSV file name.

    Returns:
        None

    Example:
        api_to_csv({'baseURL': 'https://api.example.com/', 'endpoint': 'data', 'params_dict': {'sort': {'field1': 'asc', 'field2': 'desc'}, 'filter': {'date': '2022-01-01'}}, 'headers': {'Authorization': 'Bearer token'}}, 'schema_name', 'table_name', '2022-01-01')
    """
    data_path = f"raw/{schema}_{table}_{fk_date}.csv"
    os.makedirs(os.path.dirname(data_path), exist_ok=True)
    
    df = api_to_pandas(
            # baseURL=arguments_dict['baseURL']
            # , endpoint=arguments_dict['endpoint']
            # , params_dict=arguments_dict['params_dict']
            # , headers=arguments_dict['headers']
            **arguments_dict ##can be used instead of extract key-value from arguments_dict
            , timeout=10
            )
    # df.columns = [x.lower() for x in df.columns]
    df.to_csv(data_path,index=False)
    print(f'Exported {len(df)} rows to {data_path}')

def csv_to_staging(schema: str, table: str, fk_date: str) -> None:
    """
    Reads a CSV file and inserts its contents into a staging table in a PostgreSQL database.
    
    Args:
        schema (str): The schema name where the staging table resides.
        table (str): The name of the staging table to be populated.
        fk_date (str): A date string used to construct the CSV file name.
        
    Returns:
        None
    """
    data_path = f"raw/{schema}_{table}_{fk_date}.csv"
    df = pd.read_csv(data_path)
    df.columns = [x.lower() for x in df.columns] #postgres not like uppercase column name

    commands = sql_to_list(f'etl/sql/{schema}/{table}/create_table.sql')
    staging_columns = get_column_name_from_create_table(commands[1])

    if len(df) == 0:
        raise ValueError(f"Check CSV file in {data_path} is empty")
    
    # Sort Pandas columns based on CREATE TABLE sql statement
    df = df[staging_columns]
    df = df.fillna(AsIs('Null'))

    df_columns = list(df)
    # create (col1,col2,...)
    columns = ','.join(list(df))

    # create VALUES('%s', '%s",...) one '%s' per column
    values = "VALUES({})".format(",".join(["%s" for _ in df_columns])) 

    #create INSERT INTO table (columns) VALUES('%s',...)
    insert_stmt = f"INSERT INTO staging.{table} ({columns}) {values}"

    try:
        config = load_config()
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                cur.execute(f"TRUNCATE staging.{table}")

                ### Method 1:copy from pandas
                extras.execute_batch(cur, insert_stmt, df.values)

                ### Method 2: Copy from csv: not work because cannot control orders of columns
                # with open(data_path, mode="r", encoding="utf8") as file:
                    # cur.copy_expert( ## 
                    #     f"COPY staging.{table} FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                    #     file,
                    # )
                conn.commit()

                print(f"Inserted {len(df)} rows of csv to staging: {table}")

    except (psycopg2.DatabaseError, Exception) as error:
        print(error)

def staging_to_warehouse(schema: str, table: str, fk_date: str) -> None:
    """
    Reads SQL commands from a file, connects to a PostgreSQL database, and executes the commands to load data from a staging area to a warehouse table.

    Args:
        schema (str): The schema name in the database.
        table (str): The table name in the database.
        fk_date (str): The foreign key date used in the SQL commands.
    """
    commands = sql_to_list(f'etl/sql/{schema}/{table}/load_warehouse.sql')

    try:
        config = load_config()
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                for command in commands[:-1]:
                    cur.execute(command, {
                        'schema': AsIs(schema),
                        'table': AsIs(table),
                        'fk_date': fk_date
                    })
                    print(f'{cur.statusmessage} rows: {schema}.{table}')

    except (psycopg2.DatabaseError, Exception) as error:
        print(error)

if __name__ == '__main__':
    arguments_dict=gen_arguments(symbol=['MWG','FPT','VNM','VND'],from_date='2024-06-01',to_date='2024-06-21')
    api_to_csv(arguments_dict,'vnd','daily_acctno_cashflow',20240621)
    csv_to_staging('vnd','daily_acctno_cashflow',20240621)
    staging_to_warehouse('vnd','daily_acctno_cashflow',20240621)

