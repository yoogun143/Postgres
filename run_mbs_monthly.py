from helper.api_manipulation import get_bearer_token, gen_api_config
from helper.config import load_config
from helper.mbs import get_order_merged, get_cash_statement, get_stock_statement
from load_warehouse import pandas_to_warehouse, api_to_csv
from datetime import datetime, timedelta

if __name__ == '__main__':
    # Constants
    schema = 'mbs'
    fk_date = datetime.now().strftime('%Y%m%d')
    from_date = datetime.strptime(fk_date, '%Y%m%d').replace(day=1) - timedelta(days=1)
    to_date = datetime.strptime(fk_date, '%Y%m%d').replace(day=1) - timedelta(days=1)
    from_date = from_date.replace(day=1).strftime('%Y%m%d')
    to_date = to_date.strftime('%Y%m%d')

    # User credentials
    mbs_config = load_config(filename='helper/database.ini', section='mbs')
    username = mbs_config['username']
    password = mbs_config['password']
    account = mbs_config['account']

    # Run functions
    token = get_bearer_token(username=username, password=password)

    table_list = [
        'fact_stock_order',
        'fact_cash_statement',
        'fact_stock_statement',
    ]

    for table in table_list:
        if table == 'fact_stock_order':
            # fact_stock_order
            arguments_dict = gen_api_config()["fact_stock_order"]
            arguments_dict['endpoint_order'] = arguments_dict['endpoint_order'].replace("{account}", account)
            arguments_dict['endpoint_deal'] = arguments_dict['endpoint_deal'].replace("{account}", account)
            arguments_dict['params_dict']['fromDate'] = from_date
            arguments_dict['params_dict']['toDate'] = to_date
            arguments_dict['headers']['authorization'] = f"Bearer {token}"

            api_to_csv(
                arguments_dict={
                    'baseURL': arguments_dict['baseURL'],
                    'endpoint': arguments_dict['endpoint_order'],
                    'params_dict': arguments_dict['params_dict'],
                    'headers': arguments_dict['headers']
                },
                schema=schema,
                table='fact_stock_order_order',
                fk_date=fk_date,
                data_json_field='items'
            )

            api_to_csv(
                arguments_dict={
                'baseURL': arguments_dict['baseURL'],
                'endpoint': arguments_dict['endpoint_deal'],
                'params_dict': arguments_dict['params_dict'],
                'headers': arguments_dict['headers']
                },
                schema=schema,
                table='fact_stock_order_deal',
                fk_date=fk_date,
                data_json_field='items'
            )

            order_merged = get_order_merged(fk_date=fk_date)
            pandas_to_warehouse(order_merged, schema=schema, table='fact_stock_order', truncate=False)

        elif table == 'fact_cash_statement':
            arguments_dict = gen_api_config()["fact_cash_statement"]
            arguments_dict['params_dict']['fromDate'] = from_date
            arguments_dict['params_dict']['toDate'] = to_date
            arguments_dict['params_dict']['account'] = account
            arguments_dict['params_dict']['masterAccount'] = account
            arguments_dict['headers']['authorization'] = f"Bearer {token}"

            api_to_csv(
                arguments_dict=arguments_dict,
                schema=schema,
                table='fact_cash_statement',
                fk_date=fk_date,
                data_json_field='items'
            )

            fact_cash_statement = get_cash_statement(fk_date=fk_date)
            pandas_to_warehouse(fact_cash_statement, schema=schema, table='fact_cash_statement', truncate=False)

        elif table == 'fact_stock_statement':
            arguments_dict = gen_api_config()["fact_stock_statement"]
            arguments_dict['params_dict']['fromDate'] = from_date
            arguments_dict['params_dict']['toDate'] = to_date
            arguments_dict['params_dict']['account'] = account
            arguments_dict['params_dict']['masterAccount'] = account
            arguments_dict['headers']['authorization'] = f"Bearer {token}"

            api_to_csv(
                arguments_dict=arguments_dict,
                schema=schema,
                table='fact_stock_statement',
                fk_date=fk_date,
                data_json_field='items'
            )

            fact_stock_statement = get_stock_statement(fk_date=fk_date)
            print(fact_stock_statement)
            pandas_to_warehouse(fact_stock_statement, schema=schema, table='fact_stock_statement', truncate=False)