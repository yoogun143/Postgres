from mbs import get_bearer_token, get_order_merged, get_cash_statement, get_stock_statement
from helper.config import load_config
from load_warehouse import pandas_to_warehouse

if __name__ == '__main__':
    # Constants
    schema = 'mbs'
    from_date = 20250201
    to_date = 20250228

    # User credentials
    mbs_config = load_config(filename='helper/database.ini', section='mbs')
    username = mbs_config['username']
    password = mbs_config['password']
    account = mbs_config['account']

    # Run functions
    token = get_bearer_token(username=username, password=password)

    #fact_stock_order
    order_merged = get_order_merged(token=token, from_date=from_date, to_date=to_date, account=account)
    pandas_to_warehouse(order_merged, schema=schema, table='fact_stock_order', truncate=False)

    #fact_cash_statement
    fact_cash_statement = get_cash_statement(token=token, from_date=from_date, to_date=to_date,account=account)
    pandas_to_warehouse(fact_cash_statement, schema=schema, table='fact_cash_statement', truncate=False)

    #fact_stock_statement
    fact_stock_statement = get_stock_statement(token=token, from_date=from_date, to_date=to_date,account=account)
    pandas_to_warehouse(fact_stock_statement, schema=schema, table='fact_stock_statement', truncate=False)