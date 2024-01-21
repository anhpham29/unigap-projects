import pandas as pd
import mysql.connector

def csv_to_mysql(csv_file, db_config, keywords):
    try:
        # Read CSV file and extract second column
        df = pd.read_csv(csv_file, header=None)
        
        if df.empty:
            print("CSV file is empty.")
            return

        # Filter rows based on keywords in the single column, excluding NaN values
        filtered_df = df.dropna(subset=[0]).loc[df[0].astype(str).str.contains('|'.join(keywords), case=False)]

        # Connect to MySQL
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        try:
            # Select the existing database
            cursor.execute("USE charity")

            # Insert data into the table
            for content in filtered_df:
                cursor.execute("INSERT INTO comments (content) VALUES (%s)", (content,))

            # Commit changes
            connection.commit()
            print("Data loaded into MySQL successfully!")

        except mysql.connector.Error as err:
            print(f"Error: {err}")
        finally:
            # Close the cursor and connection
            cursor.close()
            connection.close()

    except pd.errors.EmptyDataError:
        print("CSV file is empty.")
    except pd.errors.ParserError as pe:
        print(f"Error parsing CSV file: {pe}")

if __name__ == "__main__":
    
    keywords = ['Cong Vinh', 'Thuy Tien']

    # Replace with your actual values
    db_config = {
        'host': 'localhost',
        'user': 'root',
        'password': 'Saigon2024'
    }

    # Replace 'merged_output.csv' with the actual path to your CSV file
    csv_file_path = 'merged_output.csv'

    # Call the function with the provided CSV file path and MySQL credentials
    csv_to_mysql(csv_file_path, db_config, keywords)
