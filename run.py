from create_table import create_table
from load_warehouse import api_to_csv,csv_to_staging,staging_to_warehouse
from datetime import datetime
from helper.api_manipulation import gen_arguments

if __name__ == '__main__':
    fk_date = datetime.now().strftime('%Y%m%d')
    schema = 'vnd'

    # ### dim_symbol
    # table = 'dim_symbol'
    # endpoint = '/v4/stocks'
    # floor = ['HOSE','HNX','UPCOM','OTC']
    # arguments_dict=gen_arguments(endpoint= endpoint, floor=floor)

    ### fact_stock_price_daily
    table = 'fact_stock_price_daily'
    endpoint = '/v4/stock_prices'
    floor = ['HOSE','HNX','UPCOM','OTC']
    from_date = '2024-06-01'
    to_date = '2024-06-28'
    sort='date'
    arguments_dict=gen_arguments(endpoint= endpoint, floor=floor, from_date=from_date, to_date=to_date,sort=sort)
    
    create_table(schema=schema,table=table)

    api_to_csv(arguments_dict=arguments_dict
               ,schema=schema
               ,table=table
               ,fk_date=fk_date
               )
    csv_to_staging(schema=schema,table=table,fk_date=fk_date)
    staging_to_warehouse(schema=schema,table=table,fk_date=fk_date)