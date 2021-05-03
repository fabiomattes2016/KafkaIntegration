import pandas as pd
import pyodbc
import os


conn = pyodbc.connect('DRIVER={SQL Server};SERVER=MGA-SQL001\SQL2014;DATABASE=HOSPPITANGUEIRASV13V14;UID=des;PWD=benner')

sql_query = pd.read_sql_query('''
    SELECT * FROM [dbo].[VW_PACINTERN]
'''
,conn)

os.remove('export_data.csv')

df = pd.DataFrame(sql_query)
df.to_csv(r'export_data.csv', index=False)
