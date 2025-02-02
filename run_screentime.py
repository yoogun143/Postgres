from create_table import create_table
from load_warehouse import csv_to_staging,staging_to_warehouse,sqlite_to_csv
from datetime import datetime

if __name__ == '__main__':
    fk_date = datetime.now().strftime('%Y%m%d')
    schema = 'apple'

    # List table to run flow
    table_list = [
        'fact_screentime'
        ]

    for table in table_list:

        # Seprators
        print('-'*40)
        print('-'*40)

        create_table(schema=schema,table=table)

        sqlite_to_csv(
                schema=schema
                ,table=table
                ,fk_date=fk_date
                )
        csv_to_staging(schema=schema,table=table,fk_date=fk_date)
        staging_to_warehouse(schema=schema,table=table,fk_date=fk_date)