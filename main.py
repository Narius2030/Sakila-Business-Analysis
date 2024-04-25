# import src.hiveconnect.hiveapp as hiveapp
# hiveapp.create_table('orders_demo')
# hiveapp.load_data('orders_demo')

import pandas as pd
import pyodbc

sql_conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=.;DATABASE=BanHang;Trusted_Connection=yes') 
query = "SELECT * FROM SanPham"
df = pd.read_sql(query, sql_conn)
print(df.head(5))