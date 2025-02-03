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
    Retrieves the last created_at value from the specified table in the specified schema from the Postgres database.

    Args:
        schema (str): The schema name of the table to query. Defaults to 'apple'.
        table (str): The table name to query. Defaults to 'fact_screentime'.

    Returns:
        float: The last created_at value from the specified table in the specified schema, or 0 if no such value exists.
    """
    try:
        config = load_config()
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:

                ### Method 1:copy from pandas
                cur.execute(f"SELECT MAX(created_at)::float FROM {schema}.{table};")
                last_created_at: float = cur.fetchone()[0]  # type hint the return value

                if last_created_at is None:
                    last_created_at = 0
                else:
                    last_created_at = last_created_at

    except (psycopg2.DatabaseError, Exception) as error:
        print(error)

    return last_created_at

def sqlite_to_dataframe(last_created_at: float) -> pd.DataFrame:
    """
    Retrieves the data from the specified SQLite database and returns it as a pandas DataFrame.
    
    Parameters
    ----------
    last_created_at : float
        The last created_at value from the specified table in the specified schema, or 0 if no such value exists.

    Returns
    -------
    pandas.DataFrame
        A pandas DataFrame containing the data from the SQLite database.
    """
    if not os.path.exists(knowledge_db):
        print("Could not find knowledgeC.db at %s." % (knowledge_db))
        exit(1)

    # Check if knowledgeC.db is readable
    if not os.access(knowledge_db, os.R_OK):
        print("The knowledgeC.db at %s is not readable.\nPlease grant full disk access to the application running the script (e.g. Terminal, iTerm, VSCode etc.)." % (knowledge_db))
        exit(1)

    # Connect to the SQLite database
    with sqlite3.connect(knowledge_db) as con:
        cur = con.cursor()

        # Execute the SQL query to fetch data
        # Modified from https://rud.is/b/2019/10/28/spelunking-macos-screentime-app-usage-with-r/
        query = """
        SELECT
            ZOBJECT.ZVALUESTRING AS "app",
            (ZOBJECT.ZENDDATE - ZOBJECT.ZSTARTDATE) AS "usage_time",
            (ZOBJECT.ZSTARTDATE + 978307200) as "start_time",
            (ZOBJECT.ZENDDATE + 978307200) as "end_time",
            (ZOBJECT.ZCREATIONDATE + 978307200) as "created_at",
            ZOBJECT.ZSECONDSFROMGMT AS "tz",
            ZSOURCE.ZDEVICEID AS "device_id",
            ZMODEL AS "device_model"
        FROM
            ZOBJECT
            LEFT JOIN
            ZSTRUCTUREDMETADATA
            ON ZOBJECT.ZSTRUCTUREDMETADATA = ZSTRUCTUREDMETADATA.Z_PK
            LEFT JOIN
            ZSOURCE
            ON ZOBJECT.ZSOURCE = ZSOURCE.Z_PK
            LEFT JOIN
            ZSYNCPEER
            ON ZSOURCE.ZDEVICEID = ZSYNCPEER.ZDEVICEID
        WHERE
            ZSTREAMNAME = "/app/usage" AND
            (ZOBJECT.ZCREATIONDATE + 978307200) > ?
        ORDER BY
            ZCREATIONDATE DESC
        """
        cur.execute(query, (last_created_at,))

        # Fetch all rows from the result set
        df: pd.DataFrame = pd.DataFrame(cur.fetchall())

        if len(df) == 0:
            raise ValueError(f"No new data found after {last_created_at}")
        
        df.columns = [desc[0] for desc in cur.description]

    return df

def dataframe_to_postgres(df:pd.DataFrame, schema:str, table:str) -> None:
    """
    Inserts a Pandas DataFrame into a PostgreSQL table.

    This function processes a DataFrame by converting column names to lowercase,
    sorting columns based on a SQL schema, filling NaN values with 'Null', 
    and removing duplicates before inserting the data into a specified PostgreSQL table.

    Parameters:
    - df (pd.DataFrame): The DataFrame to be inserted into the database.
    - schema (str): The name of the schema in the PostgreSQL database.
    - table (str): The name of the table within the schema where data will be inserted.

    Returns:
    - None

    Raises:
    - psycopg2.DatabaseError: If there is an error with the PostgreSQL database operations.
    - Exception: For any other general errors that occur during execution.
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