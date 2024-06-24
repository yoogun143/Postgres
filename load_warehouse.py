from typing import List
from typing import Any
from typing import Dict
import psycopg2
import psycopg2.extras as extras
from psycopg2.extensions import AsIs

import pandas as pd
import os

from helper.config import load_config
from helper.string_manipulation import sql_to_list
from helper.api_manipulation import api_to_pandas,api_arguments_date_filter

# # test strng
# schema='vnd'
# table='daily_acctno_cashflow'
# fk_date = 20240621
# symbol=['MWG','FPT','VNM','VND']
# from_date='2024-06-01'
# to_date='2024-06-21'

def api_to_csv(arguments_dict: Dict, schema: str, table: str, fk_date: str) -> None:
    """
    Fetches data from an API using specified arguments, converts it into a pandas DataFrame,
    and exports the DataFrame to a CSV file.

    Args:
        arguments_dict (dict): Dictionary containing API request parameters.
        schema (str): String representing the database schema.
        table (str): String representing the table name.
        fk_date (str): String representing the date filter.
    """
    data_path = f"csv/{schema}_{table}_{fk_date}.csv"
    os.makedirs(os.path.dirname(data_path), exist_ok=True)
    
    df = api_to_pandas(**arguments_dict, timeout=10)
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
    data_path = f"csv/{schema}_{table}_{fk_date}.csv"
    
    try:
        config = load_config()
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                with open(data_path, "r") as file:
                    cur.execute(f"TRUNCATE staging.{table}")
                    cur.copy_expert(
                        f"COPY staging.{table} FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                        file,
                    )
                    print(f"Inserted {cur.rowcount} rows to staging: {table}")

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
    commands = sql_to_list(f'sql/{schema}/{table}/load_warehouse.sql')

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


def pandas_to_staging(df: pd.DataFrame, table: str) -> None:

    config: dict = load_config()
    
    if len(df) == 0:
        return # Exit function if DataFrame is empty
        
    df_columns = list(df)
    # create (col1,col2,...)
    columns = ','.join(list(df))

    # create VALUES('%s', '%s",...) one '%s' per column
    values = "VALUES({})".format(",".join(["%s" for _ in df_columns])) 

    #create INSERT INTO table (columns) VALUES('%s',...)
    insert_stmt = "INSERT INTO {} ({}) {}".format(table,columns,values)

    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                cur.execute("truncate " + table + ";")  # avoiding uploading duplicate data!
                extras.execute_batch(cur, insert_stmt, df.values)
                conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

if __name__ == '__main__':
    arguments_dict=api_arguments_date_filter(['MWG','FPT','VNM','VND'],'2024-06-01','2024-06-21')
    api_to_csv(arguments_dict,'vnd','daily_acctno_cashflow',20240621)
    csv_to_staging('vnd','daily_acctno_cashflow',20240621)
    staging_to_warehouse('vnd','daily_acctno_cashflow',20240621)

