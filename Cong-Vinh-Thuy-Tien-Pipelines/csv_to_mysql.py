import pandas as pd
import mysql.connector
from configparser import ConfigParser
from pandas.errors import ParserError

# Function to extract data from CSV file based on keywords
def extract_data(file_path, keywords):
    try:
        df = pd.read_csv(file_path, header=None, encoding='utf-8')
        filtered_df = df[df[0].astype(str).str.contains('|'.join(keywords), case=False, regex=True)]
        return filtered_df
    except ParserError as e:
        print(f"Error reading CSV file: {e}")
        return pd.DataFrame()
    
# def load_data_to_mysql(data, host, user, password, database, table_name):
    connection = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        auth_plugin='mysql_native_password',
        charset='utf8mb4',
        collation='utf8mb4_unicode_ci'
    )
    cursor = connection.cursor()

    insert_query = f'''
        INSERT INTO {table_name} (content) VALUES 
    '''

    records = data[0].astype(str).tolist()
    records_str = ', '.join([f"('{record}')" for record in records])

    full_query = insert_query + records_str

    try:
        cursor.execute(full_query)
        connection.commit()
    except mysql.connector.Error as err:
        print(f"Error inserting records: {err}")

    cursor.close()
    connection.close()

def load_data_to_mysql(data, host, user, password, database, table_name):
    connection = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        auth_plugin='mysql_native_password',
        charset='utf8mb4',
        collation='utf8mb4_unicode_ci'
    )
    cursor = connection.cursor()

    insert_query = f'''
        INSERT INTO {table_name} (content) VALUES (%s);
    '''

    records = data[0].astype(str).tolist()
    records_tuple = [(record,) for record in records]

    try:
        cursor.executemany(insert_query, records_tuple)
        connection.commit()
    except mysql.connector.Error as err:
        print(f"Error inserting records: {err}")

    cursor.close()
    connection.close()


if __name__ == "__main__":
    # CSV file path
    csv_file_path = 'merged_output.csv'

    # Keywords for filtering
    keywords = ['Cong Vinh', 'Thuy Tien']

    # MySQL database connection details
    db_host = 'localhost'
    db_user = 'root'
    db_password = 'Saigon2024'
    db_name = 'charity'
    table_name = 'comments;'
    auth_plugin = 'caching_sha2_password'

    # Extract data
    extracted_data = extract_data(csv_file_path, keywords)

    # Load data into MySQL
    load_data_to_mysql(extracted_data, db_host, db_user, db_password, db_name, table_name)
