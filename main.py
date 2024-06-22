import json
import os
import sys

import pandas as pd
import psycopg2
from sqlalchemy import create_engine

sys.path.append(r"D:\Github anyud\final\utils1")

from utils1.data_processing import (Date_Dimension_data,
                                    Exception_Stock_Trading_Facts_data,
                                    Stock_Dimension_data,
                                    Stock_Trading_Facts_data)
from utils1.db_utils import create_connection
from utils1.handle_json import load_db_config
from utils1.sql_utils import execute_sql_scripts

sys.stdout.reconfigure(encoding='utf-8')

def main():
    # Provide the absolute path to the config file
    config_file_path = r"D:\Github anyud\final\config\env.json"
    
    # List các file SQL cần thực thi
    pre_sql_scripts = [
        r"D:\Github anyud\final\sql_script\create_schema.sql",
    ]
    
    post_sql_scripts = [
        r"D:\Github anyud\final\sql_script\alter_table.sql",
    ]
    
    # Đọc cấu hình cơ sở dữ liệu
    config = load_db_config(config_file=config_file_path)
    
    # Tạo kết nối với cơ sở dữ liệu
    connection_string = create_connection(config)
    engine = create_engine(connection_string)
    
    # Kết nối tới cơ sở dữ liệu PostgreSQL và thực thi các script SQL
    connection = None
    try:
        connection = psycopg2.connect(connection_string)
        cursor = connection.cursor()
        
        # Thực thi các script SQL trước khi chèn dữ liệu
        execute_sql_scripts(cursor, pre_sql_scripts)
        
        connection.commit()
        print("Tất cả script SQL trước khi chèn dữ liệu đã được thực thi thành công")
        
        # Chèn dữ liệu vào các bảng trong schema 'stock'
        Date_Dimension_data.to_sql('Dim_Date', engine, schema='stock', if_exists='append', index=False)
        Stock_Dimension_data.to_sql('Dim_Stock', engine, schema='stock', if_exists='append', index=False)
        Stock_Trading_Facts_data.to_sql('Fact_Stock', engine, schema='stock', if_exists='append', index=False)
        Exception_Stock_Trading_Facts_data.to_sql('Fact_Stock', engine, schema='stock', if_exists='append', index=False)
        
        print("Đã truyền dữ liệu thành công vào các bảng trong schema 'stock'")
        
        # Thực thi các script SQL sau khi chèn dữ liệu
        execute_sql_scripts(cursor, post_sql_scripts)
        
        connection.commit()
        print("Tất cả script SQL sau khi chèn dữ liệu đã được thực thi thành công")
        
    except Exception as e:
        print(f"Lỗi khi kết nối hoặc thực thi lệnh SQL: {e}")
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Đã đóng kết nối đến cơ sở dữ liệu PostgreSQL")

if __name__ == "__main__":
    main()
