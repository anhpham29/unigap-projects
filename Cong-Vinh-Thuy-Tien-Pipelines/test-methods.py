import pandas as pd

# Create a DataFrame with some NaN values
data = {'Name': ['Alice', 'Bob', 'Charlie', None],
        'Age': [25, None, 22, 30],
        'City': ['New York', 'San Francisco', 'Los Angeles', 'Chicago']}

df = pd.DataFrame(data)

# Display the original DataFrame
print("Original DataFrame:")
print(df)

# Use dropna() to remove rows with NaN values
df_cleaned = df.dropna()

# Display the DataFrame after using dropna()
print("\nDataFrame after using dropna():")
print(df_cleaned)
