import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import parameters
import os.path
import sys

table_path = parameters.path
table_name = parameters.name
table_extension = parameters.extension

try:
    connection = psycopg2.connect(database=parameters.database,
                                user=parameters.user,
                                password=parameters.password,
                                host=parameters.host,
                                port=parameters.port)
    cur = connection.cursor()

    engine = create_engine(f'postgresql://{parameters.user}:{parameters.password}@{parameters.host}:{parameters.port}/{parameters.database}')

    # Excel to DB
    if parameters.excel_to_db:
        print(f'Converting {table_name + table_extension} to the database table...')
        table = pd.read_excel(table_path + table_name + table_extension)
        suffix = '_exported'
        ok = False
        index = 1
        while not ok:
            try:
                name = table_name + suffix
                table.to_sql(name, engine)
                ok = True
                print(f'Table {name} on the database {parameters.database} successfully created')
            except(ValueError) as err:
                print(err)
                if index == 1:
                    suffix += '_' + str(index)
                else:
                    suffix = suffix[:-1] + str(index)
                index += 1

    # DB to Excel
    else:
        print(f'Converting {table_name} to the Excel file...')
        try:
            table = pd.read_sql(table_name, engine)
        except(Exception) as err:
            print(err)
            print('\nProbably, there is no DB with this name.')
            sys.exit()

        suffix = '_exported'
        name = table_path + table_name + suffix + table_extension

        index = 1
        while os.path.isfile(name):
            print(f'File {name} already exists')
            if index == 1:
                suffix += '_' + str(index)
            else:
                suffix = suffix[:-1] + str(index)
            index += 1
            name = table_path + table_name + suffix + table_extension
        else:
            table.to_excel(name)
            print(f'File {name} successfully created')

except(Exception) as err:
    print(err)
finally:
    if connection:
        cur.close()
        connection.close()
        print('Connection closed')
