from typing import List
from typing import Any
from typing import Dict
import psycopg2
import psycopg2.extras as extras
from psycopg2.extensions import AsIs
from psycopg2 import sql
# from datetime import datetime

import pandas as pd
import os

from helper.config import load_config
from helper.string_manipulation import sql_to_list,get_column_name_from_create_table,comma_separated_to_list
from helper.api_manipulation import api_to_pandas,gen_arguments

# # test strng
# from datetime import datetime
# schema='vnd'
# table='dim_symbol'
# fk_date = datetime.now().strftime('%Y%m%d')
# floor=['HOSE','HNX','UPCOM','OTC']
# endpoint= '/v4/stocks'
# arguments_dict=gen_arguments(endpoint=endpoint,floor=floor)

# schema = 'vnd' 
# table = 'dim_symbol'
# fk_date = 20230701
# config = load_config()
# conn = psycopg2.connect(**config) 

def update_scd_type_2(schema: str, table: str, fk_date: str) -> None:
    '''
    Update Slowly Changing Dimension Type 2 for a given schema, table, and foreign key date.

    Parameters:
    - schema (str): The schema name where the table is located.
    - table (str): The name of the table to update.
    - fk_date (str): The foreign key date to consider for the update.

    This function updates the Type 2 Slowly Changing Dimension by comparing natural and change keys between the main and staging tables. It invalidates existing records and inserts new records based on specific conditions.

    Raises:
    - psycopg2.DatabaseError: If there is an issue with the database connection.
    - Exception: For any other unexpected errors during the update process.
    '''
    scd2_config = load_config(filename='helper/scd2.ini',section=table)
    natural_key = comma_separated_to_list(scd2_config['natural_key'])
    natural_key_staging = comma_separated_to_list(scd2_config['natural_key_staging'])
    change_key = comma_separated_to_list(scd2_config['change_key'])
    change_key_staging = comma_separated_to_list(scd2_config['change_key_staging'])
    all_fields = comma_separated_to_list(scd2_config['all_fields'])
    all_fields = [field for field in all_fields if field not in ('eff_date', 'end_date', 'valid', 'fk_date')]
    all_fields_staging = comma_separated_to_list(scd2_config['all_fields_staging'])
    all_fields_staging = [field for field in all_fields_staging if field not in ('eff_date', 'end_date', 'valid', 'fk_date')]

    v_condition_1 = [sql.SQL(f"a.{key} = b.{staging_key}") for key,staging_key in zip(natural_key,natural_key_staging)]
    v_condition_1 = sql.SQL(" AND ").join(v_condition_1)

    v_condition_2 = [sql.SQL(f"a.{key} IS DISTINCT FROM b.{staging_key}") for key,staging_key in zip(change_key, change_key_staging)]
    v_condition_2 = sql.SQL(" OR \n         ").join(v_condition_2)

    v_condition_3 = [sql.SQL(f"b.{key}") for key in all_fields_staging]
    v_condition_3 = sql.SQL(", ").join(v_condition_3)

    v_condition_4 = [sql.SQL(f"a.{key} IS NULL") for key in natural_key]
    v_condition_4 = sql.SQL(" OR ").join(v_condition_4)

    v_condition_5 = [sql.SQL(f"{key}") for key in all_fields]
    v_condition_5 = sql.SQL(", ").join(v_condition_5)

    invalidate_query = sql.SQL("""
        UPDATE {schema}.{table} a
        SET end_date = to_date(b.fk_date, 'yyyyMMdd') - INTERVAL '1 day',
            valid = FALSE
        FROM staging.{table} b
        WHERE ({p_condition_1})
        AND a.valid = TRUE
        AND ({p_condition_2});
    """).format(
        schema = sql.Identifier(schema),
        table = sql.Identifier(table),
        p_condition_1 = v_condition_1,
        p_condition_2 = v_condition_2
    )
    # print(invalidate_query.as_string(conn))

    insert_query = sql.SQL("""
        INSERT INTO {schema}.{table} ({p_condition_5}, eff_date, end_date, valid)
        SELECT {p_condition_3}, to_date(b.fk_date, 'yyyyMMdd'), '9999-01-01', TRUE
        FROM staging.{table} b
        LEFT JOIN {schema}.{table} a
        ON ({p_condition_1})
        AND a.valid = TRUE
        WHERE b.fk_date = '{fk_date}' AND
             ({p_condition_4} OR
              {p_condition_2});
    """).format(
        schema = sql.Identifier(schema),
        table = sql.Identifier(table),
        fk_date = sql.SQL(str(fk_date)),
        p_condition_1 = v_condition_1,
        p_condition_2 = v_condition_2,
        p_condition_3 = v_condition_3,
        p_condition_4 = v_condition_4,
        p_condition_5 = v_condition_5,
    )
    # print(insert_query.as_string(conn))

    try:
        config = load_config()
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                for query in [invalidate_query, insert_query]:
                    cur.execute(query)
                    print(query.as_string(conn))
                    print(cur.statusmessage)

    except (psycopg2.DatabaseError, Exception) as error:
        print(error)
    
def api_to_csv(arguments_dict: Dict, schema: str, table: str, fk_date: str, use_proxy: bool = False,rerun_proxy: bool = True,timeout: int = 10) -> None:
    """
    Fetches data from an API endpoint and exports it to a CSV file.

    Parameters:
        arguments_dict (dict): A dictionary containing the arguments required for the API request, including 'baseURL', 'endpoint', 'params_dict', and 'headers'.
        schema (str): The schema name for the CSV file.
        table (str): The table name for the CSV file.
        fk_date (str): The foreign key date used in the CSV file name.
        use_proxy (bool): Whether to use a proxy for the API request. Defaults to False.
        rerun_proxy (bool): Whether to rerun the proxy if the API request fails. Defaults to True.

    Returns:
        None

    Example:
        api_to_csv({'baseURL': 'https://api.example.com/', 'endpoint': 'data', 'params_dict': {'sort': {'field1': 'asc', 'field2': 'desc'}, 'filter': {'date': '2022-01-01'}}, 'headers': {'Authorization': 'Bearer token'}}, 'schema_name', 'table_name', '2022-01-01')
    """
    data_path = f"raw/{schema}_{table}_{fk_date}.csv"
    os.makedirs(os.path.dirname(data_path), exist_ok=True)

    if rerun_proxy == False:
        print("Rerun proxy manually by running proxy\proxy.py")
    
    df = api_to_pandas(
            # baseURL=arguments_dict['baseURL']
            # , endpoint=arguments_dict['endpoint']
            # , params_dict=arguments_dict['params_dict']
            # , headers=arguments_dict['headers']
            **arguments_dict, ##can be used instead of extract key-value from arguments_dict
            timeout=timeout,
            use_proxy=use_proxy,
            rerun_proxy=rerun_proxy
            )
    # df.columns = [x.lower() for x in df.columns]
    df['fk_date'] = fk_date
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

    # Remove dulicates
    df = df.drop_duplicates()

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
    '''
    Load data from staging to the warehouse based on the table type.
    Parameters:
    - schema (str): The schema name where the table is located.
    - table (str): The name of the table to load data into the warehouse.
    - fk_date (str): The foreign key date to consider for the load.
    Returns:
    - None
    Raises:
    - ValueError: If the table name does not start with 'dim' or 'fact'.
    '''
    if table.startswith('fact_'):
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

    elif table.startswith('dim_') or table.startswith('factless_'):
        update_scd_type_2(schema, table, fk_date)

    else: 
        raise ValueError('Table name must starts with dim_ OR fact_ OR factless_')

if __name__ == '__main__':
    arguments_dict=gen_arguments(symbol=['MWG','FPT','VNM','VND'],from_date='2024-06-01',to_date='2024-06-21')
    api_to_csv(arguments_dict,'vnd','daily_acctno_cashflow',20240621)
    csv_to_staging('vnd','daily_acctno_cashflow',20240621)
    staging_to_warehouse('vnd','daily_acctno_cashflow',20240621)