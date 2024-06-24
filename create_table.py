import psycopg2
from psycopg2.extensions import AsIs
from helper.config import load_config
from helper.string_manipulation import sql_to_list

# # Test
# schema = 'vnd'
# table = 'daily_acctno_cashflow'

def create_table(schema,table):

    commands = sql_to_list('sql/{}/{}/create_table.sql'.format(schema,table))

    try:
        config = load_config()
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                # Execute statement
                for command in commands[:-1]:
                    cur.execute(command,{'schema':AsIs(schema),'table':AsIs(table)})
                    print(cur.statusmessage)
                    # print(cur.rowcount)
                    
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)

if __name__ == '__main__':
    create_table('vnd','daily_acctno_cashflow')