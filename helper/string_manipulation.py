from typing import Union, List, Tuple
import re

def sql_to_list(sql_path: str) -> list:
    """
    Read an SQL file, remove comments and unnecessary whitespace, and split the SQL commands into a list.

    Args:
    - sql_path: The file path to the SQL file as a string.

    Returns:
    - A list of SQL commands as strings.
    """
    with open(sql_path, 'r') as fd:
        commands = fd.read()

    # Remove SQL comments and unnecessary newlines
    commands = re.sub('--(.*)(?=\n)', '', commands)
    commands = commands.replace('\n', '')
    commands = ' '.join(commands.split())
    commands = commands.split(';')

    return commands

def join_stock_string(symbol: Union[str, List[str], Tuple[str]]) -> str:
    """
    Takes a stock symbol or a collection of stock symbols and returns a single string of symbols separated by commas.

    Args:
    - symbol: A single stock symbol as a string or a collection of stock symbols as a list or tuple.

    Returns:
    - A single string of stock symbols separated by commas if the input is a list or tuple, or the original string if the input is a single symbol.
    """
    if isinstance(symbol, (list, tuple)):
        return ','.join(symbol)
    return symbol
