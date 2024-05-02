from pyhive import hive
import pyodbc 
import pandas as pd 
import logging
import os
logging.basicConfig(filename="log.txt",level=logging.DEBUG,
                    filemode='a',
                    format= '%(asctime)s - %(message)s',
                    datefmt= '%d-%b-%y %H:%M:%S') 
def LoadData(csv_file_path, tablename):
    try:
        connection = hive.Connection(host="127.0.0.1", port="10000", username='hdang', database='sakila')
        absolute_csv_file_path = os.path.abspath(csv_file_path).replace("\\", "/")
        load_data_sql = f""" 
        LOAD DATA LOCAL INPATH '/{absolute_csv_file_path}'
        OVERWRITE INTO TABLE {tablename}
        """
        # Execute SQL statement
        cursor = connection.cursor()
        cursor.execute(load_data_sql)
        cursor.close()
        print(f"Data loaded into {tablename} successfully.")
    except Exception as e:
        logging.error(e)
        return None
def CreateTableFact_Inventory_Analysic():
    try:
        connection=hive.Connection(host='127.0.0.1',port='10000',username='hdang',database='sakila')
        create_table_sql="""CREATE TABLE Fact_Inventory_Analysis (
                                        inventory_key INT,
                                        rental_key INT,
                                        orderdate_key int,
                                        remaining INT,
                                        Total_Rental_Amount FLOAT
    3                                   )
                                    ROW FORMAT DELIMITED
                                    FIELDS TERMINATED BY ','
                                    TBLPROPERTIES ('skip.header.line.count'='1')
                                """
        cursor=connection.cursor()
        cursor.execute(create_table_sql)
        cursor.close()
        print(f"Table Fact_Inventory_Analysic created successfully")
    except Exception as e:
            logging.error(e)
            return None 
def CreateTableDimDate():
    try:
        # lệnh kết nối
        connection=hive.Connection(host='127.0.0.1',port="10000",username='hdang',
                                    database='sakila')
        create_table_sql="""CREATE TABLE IF NOT EXISTS DimDate (
                                    date_key int,
                                    full_date STRING,
                                    day_of_week INT,
                                    day_num_in_month INT,
                                    day_num_overall INT,
                                    day_name STRING,
                                    day_abbrev STRING,
                                    weekday_flag STRING,
                                    week_num_in_year INT,
                                    week_num_overall INT,
                                    week_begin_date STRING,
                                    week_begin_date_key STRING,
                                    month STRING,
                                    month_num_overall INT,
                                    month_name STRING,
                                    month_abbrev STRING,
                                    quarter INT,
                                    year INT,
                                    year_month STRING,
                                    fiscal_month INT,
                                    fiscal_quarter INT,
                                    fiscal_year INT,
                                    month_end_flag STRING
                                )
                                ROW FORMAT DELIMITED
                                FIELDS TERMINATED BY ','
                                STORED AS TEXTFILE
                                TBLPROPERTIES ('skip.header.line.count'='1')
                                """
        cursor=connection.cursor()
        cursor.execute(create_table_sql)
        cursor.close()
        print(f"Table DimDate created successfully")
    except Exception as e:
            logging.error(e)
            return None 
def CreateDimRental():
    try:
        #lệnh kết nối
        connection = hive.Connection(host="127.0.0.1", port="10000", username='hdang', database='sakila')
        create_table_sql= """
                CREATE TABLE DimRental (
                rental_key INT,
                rental_id INT,
                rental_date STRING,
                inventory_id int,
                customer_id INT,
                return_date STRING,
                staff_id INT,
                amount DECIMAL(10, 2),
                payment_date STRING
            )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
            TBLPROPERTIES ('skip.header.line.count'='1')
            """
        cursor=connection.cursor()
        cursor.execute(create_table_sql)
        cursor.close()
        print(f"Table DimRental created successfully.")   
    except Exception as e:
        print(3)
        logging.error(e)
        return None 
#store the row details into python list of tuples
def CreateDimInventory(): 
    try:
        # Kết nối đến Hive server
        connection = hive.Connection(host="127.0.0.1", port="10000", username='hdang', database='sakila')
        # Lệnh SQL để tạo bảng
        create_table_sql = """
        CREATE TABLE DimInventory (
            inventory_key INT,
            inventory_id INT,
            title STRING,
            description STRING,
            release_year INT,
            language STRING,
            rental_duration INT,
            rental_rate DECIMAL(5,2),
            length INT,
            replacement_cost DECIMAL(5,2),
            rating STRING,
            special_features STRING,
            catalogy_name STRING
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        TBLPROPERTIES ('skip.header.line.count'='1')
        """
        # Thực thi lệnh SQL
        cursor = connection.cursor()
        cursor.execute(create_table_sql)
        cursor.close()
        print(f"Table DimInventory created successfully.")
    except Exception as e:
        logging.error(e)
        return None     
def df_rows_details(table_name): 
    try:
        connection = hive.Connection(host="127.0.0.1", port="10000", username='hdang', database='sakila')
        df = pd.read_sql(f"select * from {table_name}", connection)
        print('\n converting the rows_data into DataFrame \n')
        print('converted successfully the rows_data into DataFrame')
        print(df)  # In ra dữ liệu của DataFrame
    except Exception as e:
        logging.error(e)
        return None    
    
    return df

    