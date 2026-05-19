import os
import sqlite3
import pandas as pd
import subprocess
from datetime import datetime

from create_table import create_table
from helper.config import load_config
from helper.string_manipulation import sql_to_list,get_column_name_from_create_table

import psycopg2
import psycopg2.extras as extras
from psycopg2.extensions import AsIs

knowledge_db = os.path.expanduser("~/Library/Application Support/Knowledge/knowledgeC.db")

schema = 'apple'
table = 'fact_screentime'

def get_mac_device_id() -> str:
    """
    Gets the current Mac's device identifier using system_profiler.

    Returns
    -------
    str
        The device identifier (UUID) of the current Mac, or None if not found.
    """
    try:
        result = subprocess.run(
            ['system_profiler', 'SPHardwareDataType', '-json'],
            capture_output=True,
            text=True,
            timeout=5
        )
        import json
        data = json.loads(result.stdout)
        hardware = data.get('SPHardwareDataType', [{}])[0]
        device_id = hardware.get('platform_UUID')
        if device_id:
            return device_id.lower()
    except Exception as e:
        print(f"Could not get Mac device ID: {e}")
    return None

def get_device_mapping() -> dict:
    """
    Creates a mapping of device IDs to device models from ZSYNCPEER table.
    Normalizes device IDs to lowercase for case-insensitive matching.

    Returns
    -------
    dict
        Dictionary mapping device_id (lowercase) -> device_model
    """
    device_map = {}
    try:
        with sqlite3.connect(knowledge_db) as con:
            cur = con.cursor()
            cur.execute("""
                SELECT DISTINCT ZDEVICEID, ZMODEL
                FROM ZSYNCPEER
                WHERE ZDEVICEID IS NOT NULL
                ORDER BY ZDEVICEID
            """)
            for device_id, model in cur.fetchall():
                if device_id:
                    # Normalize to lowercase and remove hyphens for consistent matching
                    normalized_id = device_id.lower().replace('-', '')
                    device_map[normalized_id] = model if model else 'Unknown Device'
                    # Also store with original format
                    device_map[device_id.lower()] = model if model else 'Unknown Device'
    except Exception as e:
        print(f"Could not load device mapping: {e}")
    return device_map

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

def get_available_devices() -> pd.DataFrame:
    """
    Retrieves all available devices and their data counts from the SQLite database.

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing device IDs, models, and record counts.
    """
    if not os.path.exists(knowledge_db):
        print("Could not find knowledgeC.db at %s." % (knowledge_db))
        return pd.DataFrame()

    with sqlite3.connect(knowledge_db) as con:
        cur = con.cursor()
        query = """
        SELECT
            ZSOURCE.ZDEVICEID AS "device_id",
            ZMODEL AS "device_model",
            COUNT(*) as "record_count"
        FROM
            ZOBJECT
            LEFT JOIN ZSOURCE ON ZOBJECT.ZSOURCE = ZSOURCE.Z_PK
            LEFT JOIN ZSYNCPEER ON ZSOURCE.ZDEVICEID = ZSYNCPEER.ZDEVICEID
        WHERE
            ZSTREAMNAME = "/app/usage"
        GROUP BY
            ZSOURCE.ZDEVICEID, ZMODEL
        ORDER BY
            COUNT(*) DESC
        """
        cur.execute(query)
        df = pd.DataFrame(cur.fetchall())
        if len(df) > 0:
            df.columns = [desc[0] for desc in cur.description]

    return df

def sqlite_to_dataframe(last_created_at: float) -> pd.DataFrame:
    """
    Retrieves the data from the specified SQLite database and returns it as a pandas DataFrame.
    Intelligently maps device IDs and handles unlinked records.

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

    # Get current Mac's device ID and device mapping
    mac_device_id = get_mac_device_id()
    device_map = get_device_mapping()

    # Print available devices
    print("\n📱 Current Mac Device ID:", mac_device_id or "Not detected")
    print("📱 Available devices in database:")
    devices_df = get_available_devices()
    if len(devices_df) > 0:
        for _, row in devices_df.iterrows():
            print(f"   - {row['device_model'] or 'Unknown'} (ID: {row['device_id']}) - {row['record_count']} records")
    else:
        print("   No devices found with linking info")

    # Connect to the SQLite database
    with sqlite3.connect(knowledge_db) as con:
        cur = con.cursor()

        # Execute the SQL query to fetch data from ALL devices
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
            (ZOBJECT.ZCREATIONDATE + 978307200) > ? AND
            ZOBJECT.ZVALUESTRING IS NOT NULL AND
            ZOBJECT.ZVALUESTRING != ''
        ORDER BY
            ZCREATIONDATE DESC
        """
        cur.execute(query, (last_created_at,))

        # Fetch all rows from the result set
        df: pd.DataFrame = pd.DataFrame(cur.fetchall())

        if len(df) == 0:
            raise ValueError(f"No new data found after {last_created_at}")

        df.columns = [desc[0] for desc in cur.description]

    # Handle unlinked records (device_id is NULL)
    print(f"\n🔗 Device Linking Status:")
    linked_count = len(df[df['device_id'].notna()])
    unlinked_count = len(df[df['device_id'].isna()])
    print(f"   With device info: {linked_count} records")
    print(f"   Without device info: {unlinked_count} records")

    if unlinked_count > 0:
        # For unlinked records, assign them to the current Mac
        if mac_device_id:
            mac_id_normalized = mac_device_id.lower().replace('-', '')
            df.loc[df['device_id'].isna(), 'device_id'] = mac_device_id.lower()
            mac_model = device_map.get(mac_id_normalized) or device_map.get(mac_device_id.lower()) or 'Mac'
            df.loc[df['device_id'].isna(), 'device_model'] = mac_model
            print(f"   ✓ Assigned {unlinked_count} unlinked records to: {mac_model}")
        else:
            # Fallback: mark as Unknown but use device mapping if available
            df.loc[df['device_id'].isna(), 'device_id'] = 'unknown'
            df.loc[df['device_id'].isna(), 'device_model'] = 'Unknown Device'
            print(f"   ⚠ Could not identify Mac device, marking {unlinked_count} as 'Unknown Device'")

    # Apply device mapping to all device IDs (normalized lookup)
    def get_device_model(device_id):
        if pd.isna(device_id):
            return 'Unknown Device'
        device_id_norm = str(device_id).lower().replace('-', '')
        return device_map.get(device_id_norm) or device_map.get(str(device_id).lower()) or 'Unknown Device'

    df['device_model'] = df['device_id'].apply(get_device_model)

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
    print(f"\n🔄 Fetching screentime data since: {pd.Timestamp(last_created_at, unit='s')}")
    df = sqlite_to_dataframe(last_created_at)

    # Show summary by device
    print(f"\n📊 Data summary by device:")
    device_summary = df.groupby('device_model').agg({
        'app': 'count',
        'usage_time': 'sum'
    }).rename(columns={'app': 'records', 'usage_time': 'total_usage_seconds'})
    device_summary['total_usage_hours'] = device_summary['total_usage_seconds'] / 3600
    print(device_summary)

    dataframe_to_postgres(df,schema=schema,table=table)