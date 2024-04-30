from pyhive import hive
import pandas as pd 
import logging
import os

# logging basic file config:-
logging.basicConfig(filename="log.txt",level=logging.DEBUG,
                    filemode='a',
                    format= '%(asctime)s - %(message)s',
                    datefmt= '%d-%b-%y %H:%M:%S') 

# to create database
def create_database(dbname, username):
      
    try:
        connection = hive.Connection(host="127.0.0.1", port="10000", username=username, database='default')
        cur = connection.cursor()
        cur.execute(f"CREATE DATABASE {dbname}")
        
        print(f"\n Database:- {dbname} is created successfully \n")
        logging.info(f"Database:- {dbname}is created successfully")
        
    except Exception as e:
        logging.error(e)   



# create Rental Dimension
def CreateTableDimRental(username):
    
    try:
        connection = hive.Connection(host="127.0.0.1", port="10000", username=username, database = 'temp')
        cur = connection.cursor()
        
        cur.execute(f'''CREATE TABLE Dim_Rental (
                            rental_key INT,
                            rental_id INT,
                            rental_date DATE,
                            inventory_id INT,
                            customer_id INT,
                            return_date DATE,
                            staff_id INT,
                            amount FLOAT,
                            payment_date DATE
                        )
                        CLUSTERED BY (rental_id) INTO 8 BUCKETS
                        ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
                        STORED AS TEXTFILE''')
        
        print(f"DimRental is created successfully \n")
        logging.info('Table:- DimRental is created successfully')
        
    except Exception as e:
        logging.error(e)
        
def CreateTableDimCustomer(username):
    
    try:
        connection = hive.Connection(host="127.0.0.1", port="10000", username=username, database='sakila_dwh')
        cur = connection.cursor()
        
        cur.execute(f'''CREATE TABLE Dim_Customer (
                            customer_key INT,
                            customer_id INT,
                            store_id INT,
                            address_id INT,
                            active INT,
                            full_name string,
                            city string,
                            country string
                        )
                        CLUSTERED BY (customer_id) INTO 6 BUCKETS
                        ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
                        STORED AS TEXTFILE''')
        
        print(f"DimCustomer is created successfully \n")
        logging.info('Table:- DimCustomer is created successfully')
        
    except Exception as e:
        logging.error(e) 

def CreateTableFactSegment(username):
    
    try:
        connection = hive.Connection(host="127.0.0.1", port="10000", username=username, database='sakila_dwh')
        cur = connection.cursor()
        
        cur.execute(f'''CREATE TABLE Fact_Segment (
                            customer_id INT,
                            city string,
                            country string,
                            active TINYINT,
                            full_name string,
                            rental_id INT,
                            amount FLOAT,
                            rental_date DATE,
                            first_date DATE,
                            recency INT,
                            monetary FLOAT,
                            frequency INT
                        )
                        CLUSTERED BY (customer_id) INTO 8 BUCKETS
                        ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
                        STORED AS TEXTFILE''')
        
        print(f"FactSegment is created successfully \n")
        logging.info('Table:- FactSegment is created successfully')
        
    except Exception as e:
        logging.error(e)

def IntegrateFactSegment(username):
    
    try:
        connection = hive.Connection(host="127.0.0.1", port="10000", username=username, database='sakila_dwh')
        cur = connection.cursor()
        
        cur.execute(f'''INSERT INTO TABLE Fact_Segment
                        SELECT
                            c.customer_id, c.city, c.country, c.active, c.full_name,
                            r.rental_id, r.amount, r.rental_date,
                            (SELECT MIN(rental_date) FROM Dim_Rental sub_r WHERE sub_r.customer_id = c.customer_id) AS first_date,
                            DATEDIFF(
                                (SELECT MAX(rental_date) FROM Dim_Rental sub_r WHERE sub_r.customer_id = c.customer_id),
                                (SELECT MIN(rental_date) FROM Dim_Rental sub_r WHERE sub_r.customer_id = c.customer_id)
                            ) AS recency,
                            (SELECT SUM(amount) FROM dim_rental sub_r WHERE sub_r.customer_id = c.customer_id) AS monetary,
                            (SELECT COUNT(rental_id) FROM dim_rental sub_r WHERE sub_r.customer_id = c.customer_id) AS frequency
                        FROM dim_customer c
                        JOIN dim_rental r ON r.customer_id = c.customer_id''')
        
        print(f"FactSegment is integrated successfully \n")
        logging.info('Table:- FactSegment is integrated successfully')
        
    except Exception as e:
        logging.error(e)

# to load data into csv table
def LoadData(table_name, csv_file_path, username):
    
    try:
        connection = hive.Connection(host="127.0.0.1", port="10000", username=username, database='sakila_dwh')
        absolute_csv_file_path = os.path.abspath(csv_file_path).replace("\\", "/")
        cur = connection.cursor()
        
        cur.execute(f"LOAD DATA LOCAL INPATH '/{absolute_csv_file_path}' OVERWRITE INTO TABLE {table_name}")
        
        print(f'data is loaded successfully into {table_name} \n')
        logging.info(f'data is loaded successfully into {table_name}')
        
    except Exception as e:
        logging.error(e)

# drop table temp table;
def drop_table(table_name, username):
    
    try:
        connection = hive.Connection(host="127.0.0.1", port="10000", username=username, database='sakila_dwh')
        cur = connection.cursor()
        
        cur.execute(f"DROP TABLE {table_name}")
        
        print(f'\n Table: {table_name} is deleted successfully \n')        
        logging.info(f'Table: {table_name} is deleted successfully')
        
    except Exception as e:
        logging.error(e) 

#store the row details into python list of tuples
def ExtractRows(sqlstr, username): 
    try:
        connection = hive.Connection(host="127.0.0.1", port="10000", username=username, database='sakila_dwh')
        
        df = pd.read_sql(sqlstr, connection)

        records = df.to_records(index=False)
        result = list(records)
        print('\n converting the rows_data into python list of tuples \n')
        print('converted suscessfully the rows_data into python list of tuples')
    
    except Exception as e:
        logging.error(e)
        return None    
    
    return result