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

def api_to_csv(arguments_dict, schema, table, fk_date):

    data_path = "csv/{}_{}_{}.csv".format(schema,table,fk_date)
    os.makedirs(os.path.dirname(data_path), exist_ok=True)
    
    df = api_to_pandas(**arguments_dict, timeout=10)
    print('Exported {} rows to {}'.format(len(df), data_path))

def csv_to_staging(schema,table,fk_date):
    data_path = "csv/{}_{}_{}.csv".format(schema,table,fk_date)
    # df = pd.read_csv(data_path)

    try:
        config = load_config()
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                with open(data_path, "r") as file:
                    cur.execute("truncate staging.{}".format(table)) 
                    cur.copy_expert(
                        "COPY staging.{} FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'".format(table),
                        file,
                    )
                    # print(cur.statusmessage)
                    print('Inserted {} rows to staging: {}'.format(cur.rowcount,table))

    except (psycopg2.DatabaseError, Exception) as error:
        print(error)

def staging_to_warehouse(schema,table,fk_date):

    commands = sql_to_list('sql/{}/{}/load_warehouse.sql'.format(schema,table))

    try:
        config = load_config()
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                # Execute each commands individually
                for command in commands[:-1]:
                    cur.execute(command,{
                                        'schema':AsIs(schema)
                                        ,'table':AsIs(table)
                                        ,'fk_date':fk_date
                                        })
                    print('{} rows: {}.{}'.format(cur.statusmessage,schema,table))
                    # print(cur.rowcount)                    

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

