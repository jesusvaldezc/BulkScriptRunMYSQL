
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

cwd = os.getcwd()

db = mysql.connector.connect(
  host="127.0.0.1",
  user="root",
  password="admin",
  database="testpython"
)

consts = ['wtpart', 
         'wtpartmaster', 
         'wtpartusagelink', 
         'att_wtpart', 
         'epmdoocument', 
         'epmdocumentmaster']

with open(cwd + '\\Query.sql') as file:
        queries = file.read().split(';')

cursor = db.cursor(buffered=True)

while len(queries) > 1:

    actualQuery = queries.pop(0)
    actualQuery = actualQuery.replace('\n', ' ')
    actualQuery = actualQuery.replace('\t', ' ')
    actualQuery = actualQuery.strip()

    if actualQuery.startswith('SELECT'):
        for i in consts:
            if actualQuery.endswith(i):
                try:
                    cursor.execute(actualQuery)
                    print('Running query... ', cursor.statement)
                    df = pd.read_sql(actualQuery, con=db)
                    #print(df)
                    #df = pd.DataFrame(cursor.fetchall())
                    #df = df.iloc[: , 1:]
                    compression_opts = dict(method='zip', archive_name= i + '.csv')  
                    df.to_csv(i + '.zip', index=False, compression=compression_opts, sep='|', header=True, encoding='utf-16', quoting=QUOTE_ALL)
                except (db.Error, db.Warning) as e:
                    print(e)
                    sys.exit(1)
    else:
        try:
            cursor.execute(actualQuery)
            print('Running query... ', cursor.statement)
            db.commit()
        except (mysql.connector.Error, mysql.connector.Warning) as e:
            print(e)
            sys.exit(1)
        
