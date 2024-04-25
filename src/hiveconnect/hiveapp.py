import pyodbc 
from pyhive import hive
import pandas as pd 
import logging

# logging basic file config:-
logging.basicConfig(filename="log.txt",level=logging.DEBUG,
                    filemode='a',
                    format= '%(asctime)s - %(message)s',
                    datefmt= '%d-%b-%y %H:%M:%S') 

# to create database
def create_database(dbname):
      
    try:
        pyodbc.autocommit = True
        connection = pyodbc.connect("DSN=Hive_connection", autocommit=True)
        cur = connection.cursor()
        cur.execute(f"CREATE DATABASE {dbname};")
        print(f"\n Database:- {dbname} is created successfully \n")
        logging.info(f"Database:- {dbname}is created successfully")
        
        
    except Exception as e:
        logging.error(e)   



# create temporary csv table
def create_table(tablename):
    
    try:
        connection = hive.Connection(host="127.0.0.1", port="10000", username='buidu', database='default')
        cur = connection.cursor()
        
        cur.execute(f"create table {tablename} (order_id INT, order_date DATE, order_customer_id INT, order_status string) \
            ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' \
            STORED AS TEXTFILE")
        
        print(f"{tablename} is created successfully \n")
        logging.info('Table:- {tablename} is created successfully')
        
    except Exception as e:
        logging.error(e) 

# to load data into csv table
def load_data(table_name):
    
    try:
        pyodbc.autocommit = True
        connection = hive.Connection(host="127.0.0.1", port="10000", username='buidu', database='default')
        cur = connection.cursor()
        cur.execute(f"load data local inpath 'C:/Education/Uni/BigData/Final_Project/data/retail_db/orders/orders.txt' overwrite into table {table_name}")
        print(f'data is loaded into {table_name} \n')
        logging.info(f'data is loaded into {table_name}')
        
    except Exception as e:
        logging.error(e)

# to create permanent orc table and load data from temp csv table.
def create_table_orc_permanent(tablename1,tablename2):
    
    try:
        pyodbc.autocommit = True
        connection = pyodbc.connect("DSN=Hive_connection", autocommit=True)
        cur = connection.cursor()
        cur.execute('use hive_challange;')
        cur.execute(f"create table {tablename2}(d_id int, d_name string, d_destin string, d_code int) row format delimited fields terminated by ',' stored as parquet;")
        cur.execute(f"from {tablename1} insert overwrite table {tablename2} select *;")
        print(f'Table:- {tablename2} is created successfully \n')
        logging.info(f'Table:- Permanent table {tablename2} is created successfully')
        
    except Exception as e:
        logging.error(e)


import warnings 
warnings.filterwarnings('ignore')

# to print the sorted table into dataframe

def show_sorted_dataframe(table_name): 
    try:
        pyodbc.autocommit = True
        connection = pyodbc.connect("DSN=Hive_connection", autocommit=True)
        cur = connection.cursor()
        cur.execute(f"use hive_challange;")
        df = pd.read_sql(f"select * from {table_name} sort by d_id Desc;",connection)
        print(f'sorting of table {table_name} is under process..... \n')
        print(df)
        logging.info('sorting of the table in descending order wrt d_id is done and table is displayed as dataframe')

    except Exception as e:
        logging.error(e)  

# drop table temp table;
def drop_temp_table(tablename):
    
    try:
        pyodbc.autocommit = True
        connection = pyodbc.connect("DSN=Hive_connection", autocommit=True)
        cur = connection.cursor()
        cur.execute('use hive_challange')
        cur.execute(f"drop table {tablename};")
        print(f'\n Table:- Temprory csv table {tablename} is deleted successfully \n')        
        logging.info(f'Table:- Temprory csv table {tablename} is deleted successfully')
        
    except Exception as e:
        logging.error(e) 


# To remove the sql Alchemy warning
warnings.filterwarnings('ignore')

#store the row details into python list of tuples
def df_rows_details(table_name): 
    try:
        pyodbc.autocommit = True
        connection = hive.Connection(host="127.0.0.1", port="10000", username='buidu', database='default')
        
        df = pd.read_sql(f"select * from {table_name}",connection)

        records = df.to_records(index=False)
        result = list(records)
        print('\n converting the rows_data into python list of tuples \n')
        print('converted suscessfully the rows_data into python list of tuples')
    
    except Exception as e:
        logging.error(e)
        return None    
    
    return result