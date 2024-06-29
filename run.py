from create_table import create_table
from load_warehouse import api_to_csv,csv_to_staging,staging_to_warehouse
from datetime import datetime
from helper.api_manipulation import gen_arguments

if __name__ == '__main__':
    fk_date_today = datetime.now().strftime('%Y%m%d')
    schema = 'vnd'
    # table = 'dim_symbol'
    table = input("Insert table name:")
    
    create_table(schema=schema,table=table)

    arguments_dict=gen_arguments(endpoint= '/v4/stocks', floor=['HOSE','HNX','UPCOM','OTC'])
    api_to_csv(arguments_dict=arguments_dict
               ,schema=schema
               ,table=table
               ,fk_date=fk_date_today
               )
    csv_to_staging(schema=schema,table=table,fk_date=fk_date_today)
    staging_to_warehouse(schema=schema,table=table,fk_date=fk_date_today)