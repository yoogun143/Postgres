import os
import pandas as pd
from load_warehouse import pandas_to_warehouse

# Load the Excel file
file_path = r"D:\OneDrive - Northeastern University\MBS\Data\order history.xlsx"
df = pd.read_excel(file_path, sheet_name="Sheet1")

# Rename columns
df.rename(columns={
    "NGÀY ĐẶT LỆNH": "Date",
    "TÀI KHOẢN": "Account",
    "MÃ CK": "Symbol",
    "LOẠI GIAO DỊCH": "Side",
    "KHỐI LƯỢNG": "Order_quantity",
    "GIÁ": "Order_price",
    "TRẠNG THÁI": "Status",
    "SỐ HIỆU LỆNH": "Order_number",
    "KÊNH": "Channel",
    "KHỐI LƯỢNG KHỚP": "Match_quantity",
    "GIÁ KHỚP": "Match_price",
    "THỜI GIAN": "Match_time"
}, inplace=True)

# Convert data types
df["Date"] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce")
df["Order_quantity"] = pd.to_numeric(df["Order_quantity"], errors="coerce")
df["Order_price"] = pd.to_numeric(df["Order_price"], errors="coerce")
df["Match_quantity"] = pd.to_numeric(df["Match_quantity"], errors="coerce")
df["Match_price"] = pd.to_numeric(df["Match_price"], errors="coerce")
df["Match_time"] = pd.to_datetime(df["Match_time"], format="%H:%M:%S", errors="coerce").dt.time

# Convert text to uppercase
df["Side"] = df["Side"].str.upper()
df["Status"] = df["Status"].str.upper()

# Pad Account column with leading zeros
df["Account"] = df["Account"].astype(str).str.zfill(7)

# Reorder columns
columns_order = ["Date", "Account", "Symbol", "Side", "Order_quantity", "Order_price",
                 "Status", "Order_number", "Channel", "Match_quantity", "Match_price", "Match_time"]
df = df[columns_order]
df

# Store data in dim_location in PostgreSQL
schema = 'mbs'
table = 'fact_stock_order'
pandas_to_warehouse(df, schema=schema, table=table)

######################################################################################################################

def transform_excel_statemment(file_path):
    # Load Excel file and select 'My Sheet'
    xls = pd.ExcelFile(file_path)
    df = pd.read_excel(xls, sheet_name='My Sheet', skiprows=3)

    # Rename columns
    df.columns = df.iloc[0]
    df = df[1:].reset_index(drop=True)

    # # Handle NaN values and convert column types
    # df['TÀI KHOẢN'] = df['TÀI KHOẢN'].astype('string')
    # df['NGÀY THỰC HIỆN'] = df['NGÀY THỰC HIỆN'].astype('string')
    # df['PHÁT SINH TĂNG'] = pd.to_numeric(df['PHÁT SINH TĂNG'], errors='coerce').fillna(0).astype('int64')
    # df['PHÁT SINH GIẢM'] = pd.to_numeric(df['PHÁT SINH GIẢM'], errors='coerce').fillna(0).astype('int64')
    # df['SỐ DƯ'] = pd.to_numeric(df['SỐ DƯ'], errors='coerce').fillna(0).astype('int64')
    # df['NỘI DUNG'] = df['NỘI DUNG'].astype('string')

    # Remove bottom 3 rows
    df = df[:-3]

    # Remove 'STT' column if exists
    df.drop(columns=['STT'], errors='ignore', inplace=True)

    return df

def get_category(description):
    if 'Chuyển tiền mua' in description:
        return 'Stock Buy'
    elif 'Nhận tiền bán' in description:
        return 'Stock Sale'
    elif any(substring in description for substring in ['Phí bán', 'Phí dịch vụ bán']):
        return 'Sale Fee'
    elif any(substring in description for substring in ['Phí mua', 'Phí dịch vụ mua']):
        return 'Buy Fee'
    elif 'Phí GD (bao gồm Phí trả Sở)' in description:
        return 'Depository Trading Fee'
    elif 'MBS thu phí/nợ phí lưu ký chứng khoán' in description:
        return 'Depository Fee'
    elif 'Thu phí chuyển khoản bán CK theo quy định của TTLK' in description:
        return 'Depository Transfer Fee'
    elif 'Tạm thu thuế bán' in description:
        return 'Income Tax'
    elif any(substring in description for substring in ['Thuế TNCN 5% mã', 'Thu thuế cổ tức']):
        return 'Dividend Tax'
    elif 'MBS trả lãi tiền gửi' in description:
        return 'Demand Deposit Interest'
    elif 'Thu phi sao chep' in description:
        return 'Copy Fee'
    elif 'Giải ngân mua chứng khoán dịch vụ' in description:
        return 'Margin Release'
    elif 'Thu nợ gốc trong hạn' in description:
        return 'Margin Recover'
    elif any(substring in description for substring in ['Thu phí trong hạn MBLink', 'Thu phí trong hạn Mcredit']):
        return 'Margin Fee'
    elif 'Thu phí dịch vụ sức mua ứng trước' in description:
        return 'Cash Advanced Fee'
    elif any(substring in description for substring in ['Chuyen tien noi bo khi thuc hien copi24', 'Nop tien sao chep dich vu Copi24', 'Rut tien sao chep dich vu Copi24']):
        return 'Internal Transfer'
    elif description in ['back', 'c', 'chuyen', 'd', 'g']:
        return 'Internal Transfer'
    elif 'HOANG LE THANH CHUYEN TIEN TRACE' in description:
        return 'External Transfer'
    elif description in ['SOPHU REALTIME - NOP TIEN 005C038503 HOANG LE THANH', 'chuyen tien(29/12/2022 10:10:10)']:
        return 'External Transfer'
    elif any(substring in description for substring in ['Chi trả cổ tức', 'Tạm ứng cổ tức', 'Trả cổ tức']):
        return 'Cash Dividend'
    elif 'hoa hong gioi thieu cho KH theo CT' in description:
        return 'Referral Commission'
    else:
        return None

cash_statement_folder_path = r"D:\OneDrive - Northeastern University\MBS\Data\sao_ke_tien"
fact_cash_statement_list = []

for file_name in os.listdir(cash_statement_folder_path):
    if file_name.endswith(('.xlsx', '.xls')):
        file_path = os.path.join(cash_statement_folder_path, file_name)
        df = transform_excel_statemment(file_path)

        # Rename and clean columns
        df.rename(columns={
            'TÀI KHOẢN': 'Account',
            'NGÀY THỰC HIỆN': 'Date',
            'PHÁT SINH TĂNG': 'Credit',
            'PHÁT SINH GIẢM': 'Debit',
            'SỐ DƯ': 'Balance',
            'NỘI DUNG': 'Description'
        }, inplace=True)

        # Filter and transform
        df = df[df['Account'].notnull()]
        df['Account'] = df['Account'].apply(lambda x: str(x).zfill(7))
        df['Account'] = df['Account'].str.replace('\'','')
        df['Txtype'] = df.apply(lambda row: 'Debit' if row['Credit'] == 0 else 'Credit', axis=1)
        df['Amount'] = df[['Debit', 'Credit']].max(axis=1)
        df['Category'] = df['Description'].apply(get_category)

        # Drop unnecessary columns
        df.drop(columns=['Debit', 'Credit', 'Balance'], errors='ignore', inplace=True)

        # Filter out specific categories
        exclude_categories = ['Buy Fee', 'Depository Trading Fee', 'Income Tax', 'Margin Recover', 'Margin Release', 'Sale Fee', 'Stock Buy', 'Stock Sale']
        df = df[~df['Category'].isin(exclude_categories)]

        fact_cash_statement_list.append(df)

fact_cash_statement = pd.concat(fact_cash_statement_list, ignore_index=True)
fact_cash_statement

######################################################################################################################
stock_statement_folder_path = r"D:\OneDrive - Northeastern University\MBS\Data\sao_ke_chung_khoan"
fact_stock_statement_list = []

for file_name in os.listdir(stock_statement_folder_path):
    if file_name.endswith(('.xlsx', '.xls')):
        file_path = os.path.join(stock_statement_folder_path, file_name)
        df = transform_excel_statemment(file_path)

        # Rename and clean columns
        df.rename(columns={
            'TÀI KHOẢN': 'Account',
            'NGÀY THỰC HIỆN': 'Date',
            'MÃ CHỨNG KHOÁN': 'Symbol',
            'TRẠNG THÁI': 'Status',
            'PHÁT SINH TĂNG': 'Credit',
            'PHÁT SINH GIẢM': 'Debit',
            'NỘI DUNG': 'Description'
        }, inplace=True)

        # Filter and transform
        df = df[df['Account'].notnull()]
        df['Account'] = df['Account'].apply(lambda x: str(x).zfill(7))
        df['Account'] = df['Account'].str.replace('\'','')

        fact_stock_statement_list.append(df)

fact_stock_statement = pd.concat(fact_stock_statement_list, ignore_index=True)
fact_stock_statement