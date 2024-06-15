import json

import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# Load the uploaded CSV files
aaa_data = pd.read_csv("D:\\Github Mikezxc\\final\\Data_stock\\AAA Historical Data.csv")
aapl_data = pd.read_csv('D:\\Github Mikezxc\\final\\Data_stock\\AAPL Historical Data.csv')
acb_data = pd.read_csv('D:\\Github Mikezxc\\final\\Data_stock\\ACB Historical Data.csv')
bid_data = pd.read_csv('D:\\Github Mikezxc\\final\\Data_stock\\BID Historical Data.csv')
ctg_data = pd.read_csv('D:\\Github Mikezxc\\final\\Data_stock\\CTG Historical Data.csv')
fpt_data = pd.read_csv('D:\\Github Mikezxc\\final\\Data_stock\\FPT Historical Data.csv')
gas_data = pd.read_csv('D:\\Github Mikezxc\\final\\Data_stock\\AAPL Historical Data.csv')
nvda_data = pd.read_csv('D:\\Github Mikezxc\\final\\Data_stock\\NVDA Historical Data.csv')
vcb_data = pd.read_csv('D:\\Github Mikezxc\\final\\Data_stock\\VCB Historical Data.csv')
vnm_data = pd.read_csv('D:\\Github Mikezxc\\final\\Data_stock\\VNM Historical Data.csv')

# Function to preprocess the data with updated handling for 'Change %' column
def preprocess_data(df, stock_id):
    df['Date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y')
    df['Year'] = df['Date'].dt.year
    df['Month'] = df['Date'].dt.month
    df['Day'] = df['Date'].dt.day
    df['Weekday'] = df['Date'].dt.day_name()
    
    df['Stock_ID'] = stock_id
    df['Vol.'] = df['Vol.'].apply(lambda x: float(str(x).replace('M', '').replace('K', '')) * 1_000_000 if 'M' in str(x) else float(str(x).replace('K', '')) * 1_000 if 'K' in str(x) else float(x))
    df['Change %'] = df['Change %'].astype(str).str.replace('%', '').astype(float)
    
    return df

# Re-process the data with the updated function
aaa_data_processed = preprocess_data(aaa_data, 1)
aapl_data_processed = preprocess_data(aapl_data, 2)
acb_data_processed = preprocess_data(acb_data, 3)
bid_data_processed = preprocess_data(bid_data, 4)
ctg_data_processed = preprocess_data(ctg_data, 5)
fpt_data_processed = preprocess_data(fpt_data, 6)
gas_data_processed = preprocess_data(gas_data, 7)
nvda_data_processed = preprocess_data(nvda_data, 8)
vcb_data_processed = preprocess_data(vcb_data, 9)
vnm_data_processed = preprocess_data(vnm_data, 10)

# Select relevant columns for each table
date_dimension_columns = ['Date', 'Year', 'Month', 'Day', 'Weekday']
stock_trading_facts_columns = ['Date', 'Stock_ID', 'Price', 'Open', 'High', 'Low', 'Vol.', 'Change %']

# Prepare Date_Dimension table data
date_dimension_data = pd.concat([aaa_data_processed[date_dimension_columns], aapl_data_processed[date_dimension_columns], acb_data_processed[date_dimension_columns], bid_data_processed[date_dimension_columns], ctg_data_processed[date_dimension_columns], fpt_data_processed[date_dimension_columns], gas_data_processed[date_dimension_columns], nvda_data_processed[date_dimension_columns],vcb_data_processed[date_dimension_columns], vnm_data_processed[date_dimension_columns]]).drop_duplicates().reset_index(drop=True)

# Prepare Stock_Trading_Facts table data
stock_trading_facts_data = pd.concat([aaa_data_processed[stock_trading_facts_columns], acb_data_processed[stock_trading_facts_columns], bid_data_processed[stock_trading_facts_columns], ctg_data_processed[stock_trading_facts_columns], fpt_data_processed[stock_trading_facts_columns], nvda_data_processed[stock_trading_facts_columns], vcb_data_processed[stock_trading_facts_columns], vnm_data_processed[stock_trading_facts_columns]]).reset_index(drop=True)
exception_fact = pd.concat([aapl_data_processed[stock_trading_facts_columns],gas_data_processed[stock_trading_facts_columns]]) 
# Prepare Stock_Dimension table data
stock_dimension_data = pd.DataFrame({
    'Stock_ID': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    'Stock_Name': ['AAA', 'AAPL', 'ACB', 'BID', 'CTG', 'FPT', 'GAS', 'NVDA', 'VCB', 'VNM']
})


# Remove commas and convert to float64
stock_trading_facts_data['Price'] = stock_trading_facts_data['Price'].str.replace(',', '').astype(float)
stock_trading_facts_data['Open'] = stock_trading_facts_data['Open'].str.replace(',', '').astype(float)
stock_trading_facts_data['High'] = stock_trading_facts_data['High'].str.replace(',', '').astype(float)
stock_trading_facts_data['Low'] = stock_trading_facts_data['Low'].str.replace(',', '').astype(float)

# Đọc thông tin kết nối cơ sở dữ liệu từ file cấu hình
def load_db_config(config_file='config\\env.json'):
    with open(config_file, 'r') as file:
        config = json.load(file)
    return config

# Tạo kết nối với cơ sở dữ liệu
def create_connection(config):
    db_user = config['db_user']
    db_password = config['db_password']
    db_host = config['db_host']
    db_port = config['db_port']
    db_name = config['db_name']
    connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    return connection_string

# Lệnh SQL để tạo schema
create_schema_sql = "CREATE SCHEMA IF NOT EXISTS stock;"

# Kết nối tới cơ sở dữ liệu PostgreSQL và tạo schema nếu chưa tồn tại
try:
    connection = psycopg2.connect(connection_string)
    cursor = connection.cursor()
    cursor.execute(create_schema_sql)
    connection.commit()
    print("Schema đã được tạo thành công")
except Exception as e:
    print(f"Lỗi khi kết nối hoặc thực thi lệnh SQL: {e}")
finally:
    if connection:
        cursor.close()
        connection.close()
        print("Đã đóng kết nối đến cơ sở dữ liệu PostgreSQL")

# Đổi tên cột để chắc chắn khớp với tên mong muốn
stock_trading_facts_data.rename(columns={
    'Vol.': 'Volume',
    'Change %': 'Change_Percentage'
}, inplace=True)

exception_fact.rename(columns={
    'Vol.': 'Volume',
    'Change %': 'Change_Percentage'
}, inplace=True)

# Kiểm tra các giá trị không xác định trong cột 'Volume'
print(stock_trading_facts_data['Volume'].isna().sum())

# Xử lý các giá trị không xác định (NA hoặc inf) cho DataFrame stock_trading_facts_data
stock_trading_facts_data.fillna({'Volume': 0}, inplace=True)
stock_trading_facts_data.replace({'Volume': {float('inf'): 0, -float('inf'): 0}}, inplace=True)

# Xử lý các giá trị không xác định (NA hoặc inf) cho DataFrame exception_fact
exception_fact.fillna({'Volume': 0}, inplace=True)
exception_fact.replace({'Volume': {float('inf'): 0, -float('inf'): 0}}, inplace=True)

# Đảm bảo rằng các cột có kiểu dữ liệu chính xác
Date_Dimension_data = date_dimension_data.astype({
    'Date': 'datetime64[ns]',
    'Year': 'int',
    'Month': 'int',
    'Day': 'int',
    'Weekday': 'object'
})

Stock_Dimension_data = stock_dimension_data.astype({
    'Stock_ID': 'int',
    'Stock_Name': 'object'
})

Stock_Trading_Facts_data = stock_trading_facts_data.astype({
    'Date': 'datetime64[ns]',
    'Stock_ID': 'int',
    'Price': 'float',
    'Open': 'float',
    'High': 'float',
    'Low': 'float',
    'Volume': 'int',
    'Change_Percentage': 'float'
})

Exception_Stock_Trading_Facts_data = exception_fact.astype({
    'Date': 'datetime64[ns]',
    'Stock_ID': 'int',
    'Price': 'float',
    'Open': 'float',
    'High': 'float',
    'Low': 'float',
    'Volume': 'int',
    'Change_Percentage': 'float'
})

# Tạo kết nối với SQLAlchemy
engine = create_engine(connection_string)

# Chèn dữ liệu vào các bảng trong schema 'stock'
Date_Dimension_data.to_sql('Dim_Date', engine, schema='stock', if_exists='append', index=False)
Stock_Dimension_data.to_sql('Dim_Stock', engine, schema='stock', if_exists='append', index=False)
Stock_Trading_Facts_data.to_sql('Fact_Stock', engine, schema='stock', if_exists='append', index=False)
Exception_Stock_Trading_Facts_data.to_sql('Fact_Stock', engine, schema='stock', if_exists='append', index=False)

print("Dữ liệu đã được chèn vào cơ sở dữ liệu thành công")
