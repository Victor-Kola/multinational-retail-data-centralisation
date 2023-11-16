import pandas as pd
from database_utils import DatabaseConnector
import tabula
import yaml
import requests 
import boto3
from io import StringIO


with open('api_creds.yaml', "r") as file:
    config = yaml.safe_load(file)
      

class DataExtractor:
    @staticmethod
    def read_rds_table(table_name, db_connector = DatabaseConnector()):
        engine = db_connector.init_db_engine()
        if engine is not None:
            df= pd.read_sql_table(table_name, engine)
            return df
        else:
            print("The engine failed to initialise")
            return None
        

    @staticmethod
    def retrieve_pdf_data(pdf_path = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"):
        pdf_df = tabula.read_pdf(pdf_path, pages = 'all')
        pdf_df = pd.concat(pdf_df, ignore_index = True)
        return pdf_df
    
    @staticmethod
    def list_number_of_stores(endpoint = config['number_of_stores_endpoint'], api_header= {'x-api-key': "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}):
        response = requests.get(endpoint, headers = api_header)
        if response.status_code == 200:
            return response.json()['number_stores']
        else:
            return f'Error: {response.status_code}'
    
    @staticmethod
    def retrieve_stores_data(endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}", header = {'x-api-key': "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}):
        store_data_list = []
        for store_number in range(0, 451):
            store_endpoint = endpoint.format(store_number = store_number)
            response = requests.get(store_endpoint, headers = header)
            if response.ok:
                store_data = response.json()
                store_data_list.append(store_data)
            else:
                print(f'Error retrieving data for store number {store_number}: {response.status_code}')
            
        stores_df = pd.DataFrame(store_data_list)
        return stores_df
        
    @staticmethod
    def extract_from_s3(s3_address = 's3://data-handling-public/products.csv'):
        s3 = boto3.client('s3')

        bucket_name = s3_address.split('/')[2]
        object_key = '/'.join(s3_address.split('/')[3:])

        response = s3.get_object(Bucket = bucket_name ,Key = object_key)
        content = response['Body'].read().decode('utf=8')
        csv_data = StringIO(content)
        products_df = pd.read_csv(csv_data)
        return products_df
    @staticmethod
    def extract_json_from_https(address = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'):
        response = requests.get(address)
        if response.status_code != 200:
            raise Exception(f'Failed to fetch data: {response.status_code}')
        content = response.content.decode('utf-8')
        return pd.read_json(content)