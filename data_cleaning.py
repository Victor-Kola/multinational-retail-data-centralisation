#from data_extraction import user_data, pdf_df, products_df
import pandas as pd
#from database_utils import DatabaseConnector
import numpy as np 
import re


class DataCleaning:

    @staticmethod
    def clean_user_data(user_data):
        if not isinstance(user_data, pd.DataFrame):
            raise ValueError("This is not a pandas Dataframe.")
        else:
            user_data['join_date'] = pd.to_datetime(user_data['join_date'], format = '%Y %B %d', errors='coerce')
            user_data['date_of_birth'] = pd.to_datetime(user_data['date_of_birth'], format = '%Y %B %d', errors = 'coerce')
            user_data.drop_duplicates(subset='user_uuid', inplace=True)
            user_data.dropna()
        return(user_data)
    
    @staticmethod
    def clean_card_data(pdf_df):
        if not isinstance(pdf_df, pd.DataFrame):
            raise ValueError("This is not a pandas Dataframe.")
        else:
            pdf_df['expiry_date'] = pd.to_datetime(pdf_df['expiry_date'], errors ='coerce', format='%m/%y')
            pdf_df = pdf_df.dropna(subset=['expiry_date'])
            pdf_df = pdf_df.dropna(subset=['card_number'])
            pdf_df['date_payment_confirmed'] = pd.to_datetime(pdf_df['date_payment_confirmed'], errors = 'coerce', format = '%Y-%m-%d')
            pdf_df = pdf_df.dropna(subset=['date_payment_confirmed'])

            valid_providers = ['Diners Club / Carte Blanche', 'American Express', 'JCB 16 digit', 'JCB 15 digit', 'Maestro', 'Mastercard', 'Discover', 'VISA 19 digit', 'VISA 16 digit', 'VISA 13 digit']
            pdf_df = pdf_df[pdf_df['card_provider'].isin(valid_providers)]
        return pdf_df

    @staticmethod
    def called_clean_store_data(store_data):
        if not isinstance(store_data, pd.DataFrame):
            raise ValueError("This is not a pandas Dataframe.")
        else:
            store_data.replace('NULL', np.nan, inplace=True)
            store_data['address'] = store_data['address'].str.replace('\n', ' ', regex = False)
            store_data = store_data.dropna(subset=['store_type'])
            valid_store_types = ['Local', 'Super Store', 'Mall Kiosk', 'Outlet']
            store_data = store_data[store_data['store_type'].isin(valid_store_types)]
            store_data['opening_date'] = pd.to_datetime(store_data['opening_date'], errors = 'coerce', format ='%Y-%m-%d')
            store_data = store_data.drop_duplicates(subset=['address'])
            staff_corrections = {'J78' : '78', '30e':'30', '80R':'80', 'A97' :'97', "3n9" : '39'}
            store_data['staff_numbers'] = store_data['staff_numbers'].replace(staff_corrections)
            store_data['staff_numbers'] = pd.to_numeric(store_data['staff_numbers'], errors = 'coerce')
            store_data['staff_numbers'] = store_data['staff_numbers'].astype(int)
            valid_country_codes = ['GB', 'DE', 'US']
            store_data = store_data[store_data['country_code'].isin(valid_country_codes)]
            store_data['country_code'] = store_data['country_code'].astype('category')

            continent_corrections = { 'eeEurope': 'Europe', 'eeAmerica': 'America'}
            store_data['continent'] = store_data['continent'].replace(continent_corrections)
            rows_to_drop = store_data[store_data['continent'].isin(['NULL', 'QMAVR5H3LD', 'LU3E036ZD9', 
                                           '5586JCLARW', 'GFJQ2AAEQ8', 'SLQBD982C0', 
                                           'XQ953VS0FG', '1WZB1TE1HL'])].index
            store_data = store_data.drop(rows_to_drop)
            store_data = store_data.drop('latitude', axis = 1 )

            store_data['address'] = store_data['address'].astype(str)
            store_data['longitude'] = store_data['longitude'].astype(float)
            store_data['latitude'] = store_data['latitude'].astype(float)
            store_data['locality'] = store_data['locality'].astype(str)
            store_data['store_code'] = store_data['store_code'].astype(str)
            store_data['store-type'] = store_data['store_type'].astype('category')
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
                # Check for 'ml' and assume 1:1 ratio with 'g', then convert to kg
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
        
        # If there was an error during conversion, the result will be None, so we can drop these rows or handle them
        products_df = products_df.dropna(subset=['weight'])
        
        return products_df
    @staticmethod
    def clean_products_data(products_df):
        clean_df = products_df.drop(['Unnamed: 0'], axis=1)
        clean_df = clean_df[clean_df['product_name'].notna()]
        clean_df['product_price'] = clean_df['product_price'].replace('[£]', '', regex=True).astype(float)
        anomalous_categories = ['S1YB74MLMJ', 'C3NCA2CL35', 'WVPMHZP59U']
        clean_df['category'] = clean_df['category'].replace(anomalous_categories, np.nan)
        clean_df['EAN'] = clean_df['EAN'].apply(lambda x: x if len(str(x)) == 13 else np.nan)
        clean_df['date_added'] = pd.to_datetime(clean_df['date_added'], errors='coerce')
        clean_df = clean_df.drop_duplicates(subset=['uuid'])
        clean_df = clean_df[clean_df['uuid'].notna()]
        anomalous_removed = ['T3QRRH7SRP', 'BPSADIOQOK', 'H5N71TV8AY']
        clean_df['removed'] = clean_df['removed'].replace(anomalous_removed, np.nan)
        clean_df = clean_df[clean_df['product_code'].notna()]
        return clean_df
    @staticmethod
    def clean_orders_data(orders_table):
        clean_orders_table = orders_table.drop(['level_0', 'first_name', 'last_name', 'index', "1"], axis = 1)
        clean_orders_table['card_number'] = clean_orders_table['card_number'].astype(str)
        return clean_orders_table

    @staticmethod
    def clean_events_data(events_data):
        events_data['combined_date'] = events_data['year'].astype(str) + '-' + \
                                     events_data['month'].astype(str) + '-' + \
                                     events_data['day'].astype(str)
        events_data['date'] = pd.to_datetime(events_data['combined_date'], errors='coerce') 
        events_data = events_data.dropna(subset=['date'])
        events_data = events_data.drop(['year', 'month', 'day', 'combined_date'], axis=1)
        events_data['timestamp'] = pd.to_datetime(events_data['timestamp'], format='%H:%M:%S').dt.time
        return events_data
    






#This method cleans user data, looking out for null values, errors with dates and incorrectly typed values and rows filled with wrong information.
#cleaned_user_data = DataCleaning.clean_user_data(user_data)

#DatabaseConnector.upload_to_db(cleaned_user_data, 'dim_users')

#cleaned_pdf = DataCleaning.clean_card_data(pdf_df)

#DatabaseConnector.upload_to_db(pdf_df, 'dim_card_details')

#need to upload to db the store_data don't forget.

#weights_cleaned = DataCleaning.convert_product_weights(products_df)
#cleaned_products = DataCleaning.clean_products_data(weights_cleaned)