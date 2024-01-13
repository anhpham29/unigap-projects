import pandas as pd

def merge_columns(input_file_path, output_file_path):
    try:
        # Read the existing CSV file with three columns, skipping lines with parsing errors
        df = pd.read_csv(input_file_path, header=None, encoding='utf-8', error_bad_lines=False)

        # Merge the three columns into a single column
        df['Merged_Content'] = df.apply(lambda row: ' '.join(row.dropna().astype(str)), axis=1)

        # Create a new DataFrame with the merged content column
        new_df = df[['Merged_Content']]

        # Save the merged data to a new CSV file
        new_df.to_csv(output_file_path, index=False, encoding='utf-8')

        print(f"Merged CSV saved to {output_file_path}")
    except pd.errors.ParserError as e:
        print(f"Error reading CSV file: {e}")

if __name__ == "__main__":
    input_csv_file = 'your_input_file.csv'  # Update with your actual input file path
    output_csv_file = 'merged_output.csv'  # Update with your desired output file path

    merge_columns(input_csv_file, output_csv_file)
