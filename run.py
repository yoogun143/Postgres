from create_table import create_table
from load_warehouse import api_to_csv,csv_to_staging,staging_to_warehouse
from datetime import datetime
from helper.api_manipulation import load_arguments_dict

if __name__ == '__main__':
    fk_date = datetime.now().strftime('%Y%m%d')
    schema = 'vnd'

    # List table to run flow
    table_list = [
        'dim_symbol',
        'fact_stock_price_daily',
        'fact_stock_events'
        ]

    for table in table_list:

        # Seprators
        print('-'*40)
        print('-'*40)

        arguments_dict = load_arguments_dict(table=table)

        create_table(schema=schema,table=table)

        api_to_csv(arguments_dict=arguments_dict
                ,schema=schema
                ,table=table
                ,fk_date=fk_date
                )
        csv_to_staging(schema=schema,table=table,fk_date=fk_date)
        staging_to_warehouse(schema=schema,table=table,fk_date=fk_date)

        