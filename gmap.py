import psycopg2
import json
from helper.config import load_config

import pandas as pd

def csv_to_staging(schema: str, table: str, fk_date: str) -> None:

    df_path = 'data\location-history.json'
    with open(df_path, 'r') as f:
        data_json = json.load(f)
    df = pd.json_normalize(data_json)
    df.columns = [x.lower() for x in df.columns] #postgres not like uppercase column name

    print(df.columns)

    # commands = sql_to_list(f'etl/sql/{schema}/{table}/create_table.sql')
    # staging_columns = get_column_name_from_create_table(commands[1])

    # if len(df) == 0:
    #     raise ValueError(f"Check CSV file in {data_path} is empty")
    
    # # Sort Pandas columns based on CREATE TABLE sql statement
    # df = df[staging_columns]
    # df = df.fillna(AsIs('Null'))

    # # Remove dulicates
    # df = df.drop_duplicates()

    # df_columns = list(df)
    # # create (col1,col2,...)
    # columns = ','.join(list(df))

    # # create VALUES('%s', '%s",...) one '%s' per column
    # values = "VALUES({})".format(",".join(["%s" for _ in df_columns])) 

    # #create INSERT INTO table (columns) VALUES('%s',...)
    # insert_stmt = f"INSERT INTO staging.{table} ({columns}) {values}"

    # try:
    #     config = load_config()
    #     with psycopg2.connect(**config) as conn:
    #         with conn.cursor() as cur:
    #             cur.execute(f"TRUNCATE staging.{table}")

    #             ### Method 1:copy from pandas
    #             extras.execute_batch(cur, insert_stmt, df.values)

    #             ### Method 2: Copy from csv: not work because cannot control orders of columns
    #             # with open(data_path, mode="r", encoding="utf8") as file:
    #                 # cur.copy_expert( ## 
    #                 #     f"COPY staging.{table} FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
    #                 #     file,
    #                 # )
    #             conn.commit()

    #             print(f"Inserted {len(df)} rows of csv to staging: {table}")

    # except (psycopg2.DatabaseError, Exception) as error:
    #     print(error)