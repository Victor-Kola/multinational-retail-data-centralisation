from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from database_utils import DatabaseConnector


#print(user_data['date_of_birth'])
#list_of_tables = DatabaseConnector.list_db_tables()
#print(list_of_tables)

#pdf_df = DataExtractor.retrieve_pdf_data()

#stores_df = DataExtractor.retrieve_stores_data()
#products_df = DataExtractor.extract_from_s3()


#products_in_kg = DataCleaning.convert_product_weights(products_df)
#cleaned_products = DataCleaning.clean_products_data(products_in_kg)

#DatabaseConnector.upload_to_db(cleaned_products, 'dim_products')

DatabaseConnector.list_db_tables()
orders_table = DataExtractor.read_rds_table('orders_table')
cleaned_orders_table = DataCleaning.clean_orders_data(orders_table)
#DatabaseConnector.upload_to_db(cleaned_orders_table, 'orders_table')

events_data = DataExtractor.extract_json_from_https()
events_data = DataCleaning.clean_events_data(events_data)
print(events_data)
DatabaseConnector.upload_to_db(events_data, 'dim_date_times')

