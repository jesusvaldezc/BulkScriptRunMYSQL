
from asyncio import new_event_loop
import sys
from ast import Return
from csv import QUOTE_ALL, reader, writer
from dataclasses import replace
import os
from sqlite3 import Cursor
import pandas as pd
import mysql.connector
import pandas
import base64, io
import sqlalchemy

cwd = os.getcwd()

db = sqlalchemy.create_engine('mysql://root:admin@127.0.0.1:3306/testpython')

with open(cwd + '\\Query.sql') as file:
        queries = file.read().split(';')


while len(queries) > 1:

    actualQuery = queries.pop(0)
    #actualQuery = actualQuery.replace('\n', ' ')
    #actualQuery = actualQuery.replace('\t', ' ')
    actualQuery = actualQuery.strip()
    tableName = actualQuery.rsplit('.')[1]
    
    try:
        db.execute(actualQuery)
        print('Running query... ', actualQuery)
        df = pd.read_sql(actualQuery, con=db)
        #print(df)
        #df = pd.DataFrame(cursor.fetchall())
        #df = df.iloc[: , 1:]
        compression_opts = dict(method='zip', archive_name= tableName + '.csv')  
        df.to_csv(tableName + '.zip', index=False, compression=compression_opts, sep='|', header=True, encoding='utf-16', quoting=QUOTE_ALL)
    except (db.Error, db.Warning) as e:
        print(e)
        sys.exit(1)

        
