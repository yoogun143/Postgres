import ast
import pandas as pd

def get_order_merged(fk_date: str) -> pd.DataFrame:
    """
    Get merged order data from MBS API.

    Parameters
    ----------
    fk_date : str
        The foreign key date to filter the orders or deals.

    Returns
    -------
    pd.DataFrame
        The merged order data.
    """

    order_path = f"raw/mbs_fact_stock_order_order_{fk_date}.csv"
    order = pd.read_csv(order_path)

    deal_path = f"raw/mbs_fact_stock_order_deal_{fk_date}.csv"
    deal = pd.read_csv(deal_path)

    # Drop 'rowNo' column
    order = order.drop(columns=['rowNo'])
    deal = deal.drop(columns=['rowNo'])

    # Convert 'orderNo' column to string type
    order['orderNo'] = order['orderNo'].astype(str)
    deal['orderNo'] = deal['orderNo'].astype(str)

    # Merge order and deal data
    order_merged = pd.merge(order, deal, how='left', on='orderNo')

    # Clean up merged data
    order_merged['orderDate'] = pd.to_datetime(order_merged['orderDate'], dayfirst=True).dt.date
    order_merged['dueDate'] = pd.to_datetime(order_merged['dueDate'], dayfirst=True).dt.date

    columns_order = [
        'side',
        'account',
        'symbol',
        'price',
        'quantity',
        'orderStatus',
        'createdDate',
        'orderNo',
        'exchangeID',
        'matchedValue',
        'channel',
        'fillQuantity',
        'avgPrice',
        'orderPrice',
        'fillValue',
        'accountCode',
        'shareCode',
        'orderTime',
        'orderDate',
        'dueDate',
        'matchedPrice',
        'matchedVolume'
    ]
    order_merged = order_merged[columns_order]
    order_merged.columns = [
        'side',
        'account',
        'symbol',
        'price',
        'quantity',
        'order_status',
        'created_date',
        'order_no',
        'exchange',
        'matched_value',
        'channel',
        'fill_quantity',
        'avg_price',
        'order_price',
        'fill_value',
        'account_code',
        'share_code',
        'order_time',
        'order_date',
        'due_date',
        'matched_price',
        'matched_volume'
    ]

    return order_merged

def get_cash_statement(fk_date:str) -> pd.DataFrame:
    """
    Get the cash statement data from the MBS API.

    Parameters
    ----------
    fk_date : str
        The foreign key date to filter the cash statement.

    Returns
    -------
    pd.DataFrame
        The cash statement data.
    """
    cash_statement_path = f"raw/mbs_fact_cash_statement_{fk_date}.csv"
    fact_cash_statement = pd.read_csv(cash_statement_path)

    # Drop the rowNum column
    fact_cash_statement = fact_cash_statement.drop(columns=['rowNum'])

    # Convert the tradeDate column to date
    fact_cash_statement['tradeDate'] = pd.to_datetime(
        fact_cash_statement['tradeDate'], format='%Y%m%d'
    ).dt.date

    # Create a new column Txtype
    fact_cash_statement['Txtype'] = fact_cash_statement.apply(
        lambda row: 'Debit' if row['cashUp'] == 0 else 'Credit', axis=1
    )

    # Create a new column Amount
    fact_cash_statement['Amount'] = fact_cash_statement[['cashUp', 'cashDown']].max(axis=1)

    # Select the columns
    fact_cash_statement = fact_cash_statement[
        ['accountNo', 'tradeDate', 'content', 'Txtype', 'Amount', 'entryType']
    ]

    # Rename the columns
    fact_cash_statement.columns = [
        'account', 'date', 'description', 'txtype', 'amount', 'entry_type'
    ]

    return fact_cash_statement


def get_stock_statement(fk_date:str) -> pd.DataFrame:
    """
    Get the stock statement data from the MBS API.

    Parameters
    ----------
    fk_date : str
        The foreign key date to filter the stock statement.

    Returns
    -------
    pd.DataFrame
        The stock statement data with columns as account, date, trans_no, symbol, status, credit, debit, description
    """
    stock_statement_path = f"raw/mbs_fact_stock_statement_{fk_date}.csv"
    fact_stock_statement = pd.read_csv(stock_statement_path).drop_duplicates()
    
    # Convert the details column to a list of dictionaries
    fact_stock_statement["details"] = fact_stock_statement["details"].apply(ast.literal_eval)
    
    # Explode the list of dictionaries into separate rows
    fact_stock_statement = fact_stock_statement.explode('details')
    
    # Normalize the dictionaries
    fact_stock_statement = pd.json_normalize(fact_stock_statement['details'])

    # Drop the rowNum column
    fact_stock_statement = fact_stock_statement.drop(columns=['rowNum'])

    # Convert the dueDate column to date
    fact_stock_statement['dueDate'] = pd.to_datetime(
        fact_stock_statement['dueDate'], dayfirst=True
    ).dt.date

    # Select the columns
    fact_stock_statement = fact_stock_statement[
        ['accountCode', 'dueDate', 'transNo', 'shareCode', 'shareStatus', 'shareIn', 'shareOut', 'content']
    ]

    # Rename the columns
    fact_stock_statement.columns = [
        'account', 'date', 'trans_no', 'symbol', 'status', 'credit', 'debit', 'description'
    ]

    return fact_stock_statement
