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

# Function to load data into MySQL database
def load_data_to_mysql(data, host, user, password, database, table_name):
    connection = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )
    cursor = connection.cursor()

    # Create table if not exists
    create_table_query = f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            comment TEXT
        )
    '''
    cursor.execute(create_table_query)
    connection.commit()

    # Insert data into the table
    insert_query = f'''
        INSERT INTO {table_name} (comment) VALUES (%s)
    '''

    records = data['comment'].tolist()
    for record in records:
        cursor.execute(insert_query, (record,))

    connection.commit()

    # Close the connection
    cursor.close()
    connection.close()

if __name__ == "__main__":
    # CSV file path
    csv_file_path = '200k_comments.csv'

    # Keywords for filtering
    keywords = ['Cong Vinh', 'Thuy Tien']

    # MySQL database connection details
    db_host = 'localhost'
    db_user = 'root'
    db_password = 'Saigon2024'
    db_name = 'charity'
    table_name = 'comments'

    # Extract data
    extracted_data = extract_data(csv_file_path, keywords)

    # Load data into MySQL
    load_data_to_mysql(extracted_data, db_host, db_user, db_password, db_name, table_name)
