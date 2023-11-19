# Import necessary libraries
import pandas as pd
from database_utils import DatabaseConnector
import tabula  # Used for extracting tables from PDFs
import yaml  # Used for reading YAML configuration files
import requests  # Used for making HTTP requests
import boto3  # Amazon Web Services (AWS) SDK for Python
from io import StringIO  # Used for reading string streams

# Load API credentials from a yaml file
with open('api_creds.yaml', "r") as file:
    config = yaml.safe_load(file)

# Define a class for various data extraction methods
class DataExtractor:
    @staticmethod
    def read_rds_table(table_name, db_connector=DatabaseConnector()):
        """Reads a table from RDS into a Pandas DataFrame."""
        # Initialize the database engine
        engine = db_connector.init_db_engine()
        # If engine initialization is successful, read the table
        if engine is not None:
            df = pd.read_sql_table(table_name, engine)
            return df
        else:
            # If engine fails to initialize, print an error and return None
            print("The engine failed to initialise")
            return None

    @staticmethod
    def retrieve_pdf_data(pdf_path="https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"):
        """Extracts data from a PDF and returns it as a Pandas DataFrame."""
        # Read all pages of the PDF
        pdf_df = tabula.read_pdf(pdf_path, pages='all')
        # Concatenate all pages into a single DataFrame
        pdf_df = pd.concat(pdf_df, ignore_index=True)
        return pdf_df
    
    @staticmethod
    def list_number_of_stores(endpoint=config['number_of_stores_endpoint'], api_header={'x-api-key': "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}):
        """Gets the number of stores from an API endpoint."""
        # Make a GET request to the API endpoint
        response = requests.get(endpoint, headers=api_header)
        # If the request is successful, return the number of stores from the JSON response
        if response.status_code == 200:
            return response.json()['number_stores']
        else:
            # If the request fails, return an error message with the status code
            return f'Error: {response.status_code}'
    
    @staticmethod
    def retrieve_stores_data(endpoint="https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}", header={'x-api-key': "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}):
        """Retrieves and aggregates store data from an API into a DataFrame."""
        store_data_list = []
        # Loop over a range of store numbers to get data for each store
        for store_number in range(0, 451):
            # Format the endpoint with the current store number
            store_endpoint = endpoint.format(store_number=store_number)
            # Make a GET request for the store data
            response = requests.get(store_endpoint, headers=header)
            if response.ok:
                # If the request is successful, add the store data to the list
                store_data = response.json()
                store_data_list.append(store_data)
            else:
                # If the request fails, print an error message with the status code
                print(f'Error retrieving data for store number {store_number}: {response.status_code}')
        
        # Convert the list of store data into a DataFrame
        stores_df = pd.DataFrame(store_data_list)
        return stores_df
        
    @staticmethod
    def extract_from_s3(s3_address='s3://data-handling-public/products.csv'):
        """Extracts data from a CSV file stored in an S3 bucket into a DataFrame."""
        # Initialize an S3 client
        s3 = boto3.client('s3')

        # Parse the S3 address to get the bucket name and object key
        bucket_name = s3_address.split('/')[2]
        object_key = '/'.join(s3_address.split('/')[3:])

        # Get the object from the S3 bucket
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        # Read the content of the file, decode it, and read it into a DataFrame
        content = response['Body'].read().decode('utf=8')
        csv_data = StringIO(content)
        products_df = pd.read_csv(csv_data)
        return products_df

    @staticmethod
    def extract_json_from_https(address='https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'):
        """Extracts JSON data from an HTTPS address and reads it into a DataFrame."""
        # Make a GET request to the given address
        response = requests.get(address)
        # If the request is successful, decode the content and load into a DataFrame
        if response.status_code != 200:
            raise Exception(f'Failed to fetch data: {response.status_code}')
        content = response.content.decode('utf-8')
        return pd.read_json(content)
