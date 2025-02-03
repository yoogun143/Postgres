import os
import sqlite3
import pandas as pd

from create_table import create_table
from helper.config import load_config
from helper.string_manipulation import sql_to_list,get_column_name_from_create_table

import psycopg2
import psycopg2.extras as extras
from psycopg2.extensions import AsIs

knowledge_db = os.path.expanduser("~/Library/Application Support/Knowledge/knowledgeC.db")

schema = 'apple'
table = 'fact_screentime'

def get_last_created_at(schema: str = 'apple', table: str = 'fact_screentime') -> float:
    """
    Retrieves the last created_at value from the specified table in the specified schema, or 0 if no records exist.

    Args:
        schema (str): The name of the Postgres schema containing the table. Defaults to 'apple'.
        table (str): The name of the table containing the created_at column. Defaults to 'fact_screentime'.

    Returns:
        float: The last created_at value from the specified table, or 0 if no records exist.
    """
    try:
        config = load_config()
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:

                ### Method 1:copy from pandas
                cur.execute(f"SELECT MAX(created_at)::float FROM {schema}.{table};")
                last_created_at = cur.fetchone()[0] 

                if last_created_at is None:
                    last_created_at = 0
                else: 
                    last_created_at = last_created_at

    except (psycopg2.DatabaseError, Exception) as error:
        print(error)

    return last_created_at

def sqlite_to_dataframe(last_created_at: float) -> pd.DataFrame:
    """
    Reads data from the SQLite database located at `knowledge_db` and returns it as a Pandas DataFrame.

    Parameters
    ----------
    last_created_at : float
        The last created_at value to filter records from the database.

    Returns
    -------
    df : pandas.DataFrame
        A Pandas DataFrame containing the data fetched from the database.
    """
    ...

def dataframe_to_postgres(df:pd.DataFrame, schema:str, table:str) -> None:
    """
    Inserts a Pandas DataFrame into a PostgreSQL table.

    This function processes a DataFrame by adjusting its column names to lowercase,
    ordering the columns based on an SQL CREATE TABLE statement, filling null values,
    and removing duplicates. The data is then inserted into a specified PostgreSQL table
    using batch execution.

    Parameters:
    ----------
    df : pd.DataFrame
        The DataFrame containing the data to be inserted.
    schema : str
        The PostgreSQL schema name where the table resides.
    table : str
        The PostgreSQL table name to which the data will be inserted.

    Returns:
    -------
    None

    Raises:
    ------
    psycopg2.DatabaseError
        If there is a PostgreSQL error during the execution.
    Exception
        For any other error that occurs.
    """

    df.columns = [x.lower() for x in df.columns] #postgres not like uppercase column name

    commands = sql_to_list(f'etl/sql/{schema}/{table}/create_table.sql')
    warehouse_columns = get_column_name_from_create_table(commands[0])
    
    # Sort Pandas columns based on CREATE TABLE sql statement
    df = df[warehouse_columns]
    df = df.fillna(AsIs('Null'))

    # Remove dulicates
    df = df.drop_duplicates()

    df_columns = list(df)
    # create (col1,col2,...)
    columns = ','.join(list(df))

    # create VALUES('%s', '%s",...) one '%s' per column
    values = "VALUES({})".format(",".join(["%s" for _ in df_columns])) 

    #create INSERT INTO table (columns) VALUES('%s',...)
    insert_stmt = f"INSERT INTO {schema}.{table} ({columns}) {values}"

    try:
        config = load_config()
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:

                ### Method 1:copy from pandas
                extras.execute_batch(cur, insert_stmt, df.values)

                conn.commit()

                print(f"Inserted {len(df)} rows from sqlite to warehouse: {table}")

    except (psycopg2.DatabaseError, Exception) as error:
        print(error)

if __name__ == '__main__':
    create_table(schema=schema,table=table)
    last_created_at = get_last_created_at(schema=schema,table=table)
    df = sqlite_to_dataframe(last_created_at)
    dataframe_to_postgres(df,schema=schema,table=table)