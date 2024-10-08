from create_table import create_table
from load_warehouse import api_to_csv,csv_to_staging,staging_to_warehouse
from helper.api_manipulation import load_arguments_dict
from datetime import datetime

if __name__ == '__main__':
    fk_date = datetime.now().strftime('%Y%m%d')
    schema = 'vnd'

    # List table to run flow
    table_list = [
        # 'dim_symbol',
        # 'fact_stock_price',
        # 'factless_stock_events',
        # 'dim_financial_models',
        'factless_financial_statements'
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
                # ,use_proxy=True
                # ,rerun_proxy=True
                ,timeout=20
                )
        csv_to_staging(schema=schema,table=table,fk_date=fk_date)
        staging_to_warehouse(schema=schema,table=table,fk_date=fk_date)