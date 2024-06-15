import json
import os
import sys

import pandas as pd
import psycopg2
from sqlalchemy import create_engine

sys.path.append(r"D:\Github Mikezxc\final\utils1")

from data_processing import (Date_Dimension_data,
                             Exception_Stock_Trading_Facts_data,
                             Stock_Dimension_data, Stock_Trading_Facts_data)
from db_utils import create_connection
from handle_json import load_db_config
from sql_utils import execute_sql_scripts

sys.stdout.reconfigure(encoding='utf-8')


def main():
    # Provide the absolute path to the config file
    config_file_path = r"D:\Github Mikezxc\final\config\env.json"
    sql_scripts_directory = r"D:\Github Mikezxc\final\sql_script"
    
    # Đọc cấu hình cơ sở dữ liệu
    config = load_db_config(config_file=config_file_path)
    
    # Tạo kết nối với cơ sở dữ liệu
    connection_string = create_connection(config)
    engine = create_engine(connection_string)
    
    # Kết nối tới cơ sở dữ liệu PostgreSQL và tạo schema nếu chưa tồn tại
    connection = None
    try:
        connection = psycopg2.connect(connection_string)
        cursor = connection.cursor()
        
        # Thực thi tất cả các script SQL trong thư mục
        execute_sql_scripts(cursor, sql_scripts_directory)
        
        connection.commit()
        print("Tất cả script SQL đã được thực thi thành công")
        
        # Chèn dữ liệu vào các bảng trong schema 'stock'
        Date_Dimension_data.to_sql('Dim_Date', engine, schema='stock', if_exists='append', index=False)
        Stock_Dimension_data.to_sql('Dim_Stock', engine, schema='stock', if_exists='append', index=False)
        Stock_Trading_Facts_data.to_sql('Fact_Stock', engine, schema='stock', if_exists='append', index=False)
        Exception_Stock_Trading_Facts_data.to_sql('Fact_Stock', engine, schema='stock', if_exists='append', index=False)
        
        print("Đã truyền dữ liệu thành công vào các bảng trong schema 'stock'")
        
    except Exception as e:
        print(f"Lỗi khi kết nối hoặc thực thi lệnh SQL: {e}")
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Đã đóng kết nối đến cơ sở dữ liệu PostgreSQL")

if __name__ == "__main__":
    main()
