import pandas as pd
from datetime import datetime

class DataCleaning:
    def __init__(self):
        
        pass
    def clean_user_data(self, df):
        """
        Cleans user data in the DataFrame.

        Tasks:
        - Handle NULL values either by filling or dropping them.
        - Fix errors with dates and convert them to a consistent format.
        - Correct incorrectly typed values (e.g., converting strings to integers where applicable).
        - Identify and remove or correct rows with wrong information.

        :param df: pandas DataFrame containing user data.
        :return: Cleaned pandas DataFrame.
        """

        # Handle NULL values
      
        df = df.dropna()  
        # Fix date errors and standardize date format
        
        df.fillna({'column_name': 'default_value'}, inplace=True)  # Customize based on real column names/ values
        
       
        # Correct date errors - Ensure correct date format (example format: YYYY-MM-DD)
        df['date_column'] = pd.to_datetime(df['date_column'], errors='coerce', format='%Y-%m-%d')  # Customize column name and format
        
        # Fix incorrectly typed values
        df['numeric_column'] = pd.to_numeric(df['numeric_column'], errors='coerce')  # Customize column name

        # Remove rows with wrong information 
        df = df[df['specific_column'] != 'wrong_value']  # Customize condition

        return df
        pass

    

    def clean_card_data(self, df):
        """
        Cleans card data in the DataFrame.

        Tasks:
        - Fill in or drop NULL values.
        - Standardize formats for dates and other specific fields.
        - Remove or correct rows with erroneous values.

        :param df: pandas DataFrame containing card data.
        :return: Cleaned pandas DataFrame.
        """
        # Fill NULL values with a default value or use .dropna() to remove
       
        df['country_code'].fillna('Unknown', inplace=True)

        # Drop rows where critical information is missing
        df.dropna(subset=['card_number'], inplace=True)

        # Standardize date format
        # Note: Adjust the column name and format as necessary
        if 'expiration_date' in df.columns:
            df['expiration_date'] = pd.to_datetime(df['expiration_date'], errors='coerce').dt.strftime('%Y-%m-%d')

        df = df[df['card_number'].str.len() >= 16]

        if 'country_code' in df.columns:
            df['country_code'] = df['country_code'].str.upper()

        return df