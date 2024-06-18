import pandas as pd

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
date_dimension_data = pd.concat([aaa_data_processed[date_dimension_columns], aapl_data_processed[date_dimension_columns], acb_data_processed[date_dimension_columns], bid_data_processed[date_dimension_columns], ctg_data_processed[date_dimension_columns], fpt_data_processed[date_dimension_columns], gas_data_processed[date_dimension_columns], nvda_data_processed[date_dimension_columns], vcb_data_processed[date_dimension_columns], vnm_data_processed[date_dimension_columns]]).drop_duplicates().reset_index(drop=True)

# Prepare Stock_Trading_Facts table data
stock_trading_facts_data = pd.concat([aaa_data_processed[stock_trading_facts_columns], acb_data_processed[stock_trading_facts_columns], bid_data_processed[stock_trading_facts_columns], ctg_data_processed[stock_trading_facts_columns], fpt_data_processed[stock_trading_facts_columns], nvda_data_processed[stock_trading_facts_columns], vcb_data_processed[stock_trading_facts_columns], vnm_data_processed[stock_trading_facts_columns]]).reset_index(drop=True)
exception_fact = pd.concat([aapl_data_processed[stock_trading_facts_columns], gas_data_processed[stock_trading_facts_columns]])

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

# Đổi tên cột để chắc chắn khớp với tên mong muốn
stock_trading_facts_data.rename(columns={
    'Vol.': 'Volume',
    'Change %': 'Change_Percentage'
}, inplace=True)

exception_fact.rename(columns={
    'Vol.': 'Volume',
    'Change %': 'Change_Percentage'
}, inplace=True)

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
