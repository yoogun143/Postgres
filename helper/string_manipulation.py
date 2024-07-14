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

def get_column_name_from_create_table(sql_statement: str) -> list:
    """
    Extracts column names from a CREATE TABLE SQL statement.

    Args:
        sql_statement (str): The CREATE TABLE SQL statement from which to extract column names.

    Returns:
        list: A list of column names extracted from the SQL statement.
    """
    # Define a regular expression pattern to match column names followed by their data types
    pattern = r'\b(\w+)\s+\b(?:VARCHAR|INT|TEXT|DATE|NUMERIC|DECIMAL)\b'

    # Use re.findall to find all matches of the pattern in the sql_statement
    matches = re.findall(pattern, sql_statement)

    # # Filter out any matches that are equal to 'fk_date'
    # filtered_matches = [match for match in matches if match != 'fk_date']

    return matches

def comma_separated_to_list(string: str) -> list:
    """
    Transforms a comma-separated string into a list and removes blank elements.

    Parameters:
    s (str): The input string separated by commas.

    Returns:
    list: A list of strings with no blank elements.
    """
    return [item.strip() for item in string.split(',') if item.strip()]