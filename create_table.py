import psycopg2
from psycopg2.extensions import AsIs
from helper.config import load_config
from helper.string_manipulation import sql_to_list

# # Test
# schema = 'vnd'
# table = 'daily_acctno_cashflow'

def create_table(schema: str, table: str) -> None:
    """
    Reads SQL commands from a file, connects to a PostgreSQL database using configuration parameters,
    and executes the SQL commands to create a table within a specified schema.

    :param schema: The schema name where the table will be created.
    :param table: The table name to be created.
    :return: None
    """
    commands = sql_to_list(f'sql/{schema}/{table}/create_table.sql')

    try:
        config = load_config()
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                for command in commands[:-1]:
                    cur.execute(command, {'schema': AsIs(schema), 'table': AsIs(table)})
                    print(cur.statusmessage)
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)

if __name__ == '__main__':
    create_table('vnd','daily_acctno_cashflow')