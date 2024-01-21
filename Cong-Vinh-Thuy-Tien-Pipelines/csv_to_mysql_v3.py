import pandas as pd
import pymysql

# Function to load data from CSV file and filter lines containing keywords
def load_and_filter_data(file_path, keywords):
    df = pd.read_csv(file_path, header=None, names=['content'], encoding='utf-8').dropna()
    filtered_df = df[df['content'].str.contains('|'.join(keywords), case=False)]
    return filtered_df

# Function to insert data into MySQL database
def insert_into_database(data, host, user, password, database, table, column):
    connection = pymysql.connect(host=host,
                                user=user,
                                password=password,
                                database=database,
                                cursorclass=pymysql.cursors.DictCursor)

    try:
        with connection.cursor() as cursor:
            for index, row in data.iterrows():
                sql = f"INSERT INTO {table} ({column}) VALUES (%s)"
                cursor.execute(sql, (row['content'],))
        connection.commit()
    finally:
        connection.close()

# Main program
if __name__ == "__main__":
    # Configuration
    csv_filename = 'merged_output.csv'
    keywords = ['Cong Vinh', 'Thuy Tien']
    database_config = {
        'host': 'localhost',
        'user': 'root',
        'password': 'Saigon2024',
        'database': 'charity',
        'table': 'comments',
        'column': 'content',
    }

    # Load and filter data
    filtered_data = load_and_filter_data(csv_filename, keywords)

    # Insert data into MySQL database
    insert_into_database(filtered_data, **database_config)

    print("Data insertion completed.")
