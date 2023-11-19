#from data_extraction import user_data, pdf_df, products_df
import pandas as pd
#from database_utils import DatabaseConnector
import numpy as np 
import re


class DataCleaning:

    @staticmethod
    def clean_user_data(user_data):
        """Cleans and formats user data in a Pandas Dataframe."""
        #Check if the input is a pandas DataFrame
        if not isinstance(user_data, pd.DataFrame):
            raise ValueError("This is not a pandas Dataframe.")
        else:
            #Convert 'join_date' to datetime format, coerce errors to NaT (Not a Time)
            user_data['join_date'] = pd.to_datetime(user_data['join_date'], format = '%Y %B %d', errors='coerce')
            #Convert 'date_of_birth" to datetime format, coerce errors to NaT
            user_data['date_of_birth'] = pd.to_datetime(user_data['date_of_birth'], format = '%Y %B %d', errors = 'coerce')
            #Remove duplicate records based on 'user_uuid'
            user_data.drop_duplicates(subset='user_uuid', inplace=True)
            #Drop rows with any missing values
            user_data.dropna()
        return(user_data)
    
    @staticmethod
    def clean_card_data(pdf_df):
        #Check if the input is a pandas DataFrame
        if not isinstance(pdf_df, pd.DataFrame):
            raise ValueError("This is not a pandas Dataframe.")
        else:
            #Convert 'expiry_date' to datetime format, coerce errors to NaT, and drop rows with missing 'expiry_date'
            pdf_df['expiry_date'] = pd.to_datetime(pdf_df['expiry_date'], errors ='coerce', format='%m/%y')
            pdf_df = pdf_df.dropna(subset=['expiry_date'])
            #Drop rows with missing 'card_number'
            pdf_df = pdf_df.dropna(subset=['card_number'])
            # Convert 'date_payment_confirmed' to datetime, coerce errors to Nat, and drop rows with missing 'date_payment_confirmed'
            pdf_df['date_payment_confirmed'] = pd.to_datetime(pdf_df['date_payment_confirmed'], errors = 'coerce', format = '%Y-%m-%d')
            pdf_df = pdf_df.dropna(subset=['date_payment_confirmed'])
            #Define a list of valid card providers and filter the list to only incude these rows.
            valid_providers = ['Diners Club / Carte Blanche', 'American Express', 'JCB 16 digit', 'JCB 15 digit', 'Maestro', 'Mastercard', 'Discover', 'VISA 19 digit', 'VISA 16 digit', 'VISA 13 digit']
            pdf_df = pdf_df[pdf_df['card_provider'].isin(valid_providers)]
        return pdf_df


    @staticmethod
    def clean_store_data(store_data):
        """Cleans and formats store data in a Pandas Dataframe."""
        # Verify if input is a pandas Dataframe.
        if not isinstance(store_data, pd.DataFrame):
            raise ValueError("Input is not a pandas DataFrame.")
        else:
            # Replace all 'Null' strings with NaN
            store_data.replace('NULL', np.nan, inplace=True)
            #Remove new lines from the address field
            store_data['address'] = store_data['address'].str.replace('\n', ' ', regex=False)
            #Convert 'opening_date' to datetime format
            store_data['opening_date'] = pd.to_datetime(store_data['opening_date'], errors='coerce', format='%Y-%m-%d')

            # Include 'Web Portal' in valid store types and define valid store types 
            valid_store_types = ['Local', 'Super Store', 'Mall Kiosk', 'Outlet', 'Web Portal']
            store_data = store_data[store_data['store_type'].isin(valid_store_types)]

            #Remove duplicates within the 'address' line.
            store_data = store_data.drop_duplicates(subset=['address'])
            #Correct staff numbers and cnvert to the correct data type.
            staff_corrections = {'J78': '78', '30e': '30', '80R': '80', 'A97': '97', "3n9": '39'}
            store_data['staff_numbers'] = store_data['staff_numbers'].replace(staff_corrections)
            store_data['staff_numbers'] = pd.to_numeric(store_data['staff_numbers'], errors='coerce')
            store_data['staff_numbers'] = store_data['staff_numbers'].astype('Int64', errors='ignore')
            #Create valid list of countries and filter for this while setting the correct datatype
            valid_country_codes = ['GB', 'DE', 'US']
            store_data = store_data[store_data['country_code'].isin(valid_country_codes)]
            store_data['country_code'] = store_data['country_code'].astype('category')
            #Correct continent names and remove rows without the correct continents.
            continent_corrections = {'eeEurope': 'Europe', 'eeAmerica': 'America'}
            store_data['continent'] = store_data['continent'].replace(continent_corrections)
            rows_to_drop = store_data[store_data['continent'].isin(['NULL', 'QMAVR5H3LD', 'LU3E036ZD9', 
                                            '5586JCLARW', 'GFJQ2AAEQ8', 'SLQBD982C0', 
                                            'XQ953VS0FG', '1WZB1TE1HL'])].index
            store_data = store_data.drop(rows_to_drop)

            # Data type conversions
            store_data['address'] = store_data['address'].astype(str)
            store_data['longitude'] = pd.to_numeric(store_data['longitude'], errors='coerce')
            store_data['latitude'] = pd.to_numeric(store_data['latitude'], errors='coerce')
            store_data['locality'] = store_data['locality'].astype(str)
            store_data['store_code'] = store_data['store_code'].astype(str)
            store_data['store_type'] = store_data['store_type'].astype('category')
            store_data['continent'] = store_data['continent'].astype('category')

        return store_data

    @staticmethod
    def convert_product_weights(products_df):
        """Converts product weights to kg and cleans the weight column."""

        def convert_to_kg(value):
            """Helper function to convert weight to kg."""
            # If the value is already a float (indicating no units), return it as is
            if isinstance(value, float):
                return value
            try:
                # Detect calculation (e.g., "2x250ml") and evaluate it
                if 'x' in value:
                    parts = value.split('x')
                    if len(parts) == 2:
                        num, unit_value = parts
                        # Remove non-numeric characters from unit_value for multiplication
                        unit_value = re.sub(r'[^0-9.]', '', unit_value)
                        # Perform the multiplication and then continue with the conversion
                        value = str(float(num) * float(unit_value))


                # Check for 'kg' in weight and convert to float
                if 'kg' in value:
                    return float(value.replace('kg', ''))
                elif '77g .' == value:  # Specific case with an extra period
                    return 77.0 / 1000  # Convert 77g to kg
                # Check for 'g' in weight and convert to kg
                elif 'g' in value:
                    return float(value.replace('g', '')) / 1000
                # Check for 'ml' then convert to kg
                elif 'ml' in value:
                    return float(value.replace('ml', '')) / 1000
                elif value == '16oz':  # Specific case with ounces
                    return 16 * 0.0283495  # Convert 16oz to kg
                else:
                    # If no known unit, assume it's already in kg
                    return float(value)
            except ValueError as e:
                # If there's a ValueError, print it and return None to handle it later
                print(f"Error converting '{value}': {e}")
                return None

        # Apply the conversion to each entry in the weight column
        products_df['weight'] = products_df['weight'].apply(convert_to_kg)
        
        # Drop any. rows that were not properly converted
        products_df = products_df.dropna(subset=['weight'])
        
        return products_df
    @staticmethod
    def clean_products_data(products_df):
        """Cleans and formats product data in a Pandas DataFrame."""
        #Remove unnecessary unnamed column
        clean_df = products_df.drop(['Unnamed: 0'], axis=1)
        #Filter out rows with missing product names
        clean_df = clean_df[clean_df['product_name'].notna()]
        #Remove currency symbol and convert price to float
        clean_df['product_price'] = clean_df['product_price'].replace('[£]', '', regex=True).astype(float)
        #Replace invalid categories and replace them with NaN
        invalid_categories = ['S1YB74MLMJ', 'C3NCA2CL35', 'WVPMHZP59U']
        clean_df['category'] = clean_df['category'].replace(invalid_categories, np.nan)
        #Replace EAN number with NaN if incorrect length is found
        clean_df['EAN'] = clean_df['EAN'].apply(lambda x: x if len(str(x)) == 13 else np.nan)
        #Convert 'date_added' to datetime and change errors to NaT
        clean_df['date_added'] = pd.to_datetime(clean_df['date_added'], errors='coerce')
        #Remove duplicates and filter out rows with missing 'uuid'
        clean_df = clean_df.drop_duplicates(subset=['uuid'])
        clean_df = clean_df[clean_df['uuid'].notna()]
        #Identify and Replace invalid values with NaN
        invalid_removed = ['T3QRRH7SRP', 'BPSADIOQOK', 'H5N71TV8AY']
        clean_df['removed'] = clean_df['removed'].replace(invalid_removed, np.nan)
        #Drop rows without a product code
        clean_df = clean_df[clean_df['product_code'].notna()]
        return clean_df
    @staticmethod
    def clean_orders_data(orders_table):
        """Cleans and formats order data in a Pandas Dataframe."""
        #Drop unnecessary columns
        clean_orders_table = orders_table.drop(['level_0', 'first_name', 'last_name', 'index', "1"], axis = 1)
        #Convert card number to string
        clean_orders_table['card_number'] = clean_orders_table['card_number'].astype(str)
        return clean_orders_table

    @staticmethod
    def clean_events_data(df):
        # Handle NaN values in 'month' before filtering non-numeric values
        df = df.dropna(subset=['month'])
        # Drop rows with non-numeric 'month' values
        df = df[df['month'].str.isnumeric()]
        # Convert 'month', 'year', and 'day' to integers
        df['month'] = df['month'].astype(int)
        df['year'] = df['year'].astype(int)
        df['day'] = df['day'].astype(int)
        if pd.to_datetime(df['timestamp'], format='%H:%M:%S', errors='coerce').notnull().all():
            df['timestamp'] = pd.to_datetime(df['timestamp'], format='%H:%M:%S').dt.time
        # Check and handle missing values in other columns
        df.dropna(inplace=True)
        # Normalize text data in the 'time_period' column
        df['time_period'] = df['time_period'].str.lower()

        return df




