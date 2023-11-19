import yaml
from sqlalchemy import create_engine, inspect, text

class DatabaseConnector:
    @staticmethod
    def read_db_creds(filename='db_creds.yaml'):
        """Reads database credentials from a YAML file."""
        with open(filename, "r") as stream:
            try:
                # Load and return the database credentials
                db_creds = yaml.safe_load(stream)
                return db_creds
            except yaml.YAMLError as exc:
                # Handle YAML errors
                print("Error in configuration file:", exc) 
                return None
            
  

    @staticmethod 
    def init_db_engine(credentials = None, db_type="postgresql", dbapi="psycopg2"):
        """ Initializes and returns a database engine."""
        if credentials is None:
            # Read database credentials if not provided
            credentials = DatabaseConnector.read_db_creds()
        if credentials:
            # Construct the database connection string
            db_str = f"{db_type}+{dbapi}://{credentials['RDS_USER']}:{credentials['RDS_PASSWORD']}@{credentials['RDS_HOST']}:{credentials['RDS_PORT']}/{credentials['RDS_DATABASE']}"
            # Create and return the database engine
            engine = create_engine(db_str)
            return engine
        # Handle cases where credentials are not available
        else: 
            print("Could not read credentials or initialise the database engine.")
            return None
        
    @staticmethod
    def list_db_tables():
        """Lists the tables in the connected database."""
        engine = DatabaseConnector.init_db_engine()
        if engine is not None:
            # Use the inspector command to list table names. 
            inspector = inspect(engine)
            table_names = inspector.get_table_names()
            return table_names
        else:
            # Handle cases where the engine is not initialized
            print("Engine is not initialised")
            return None
        
    @staticmethod
    def upload_to_db(dataframe, table_name):
        """Uploads a pandas DataFrame to a specificed table in the database."""
        # Read a different set of credentials for the sales data
        credentials = DatabaseConnector.read_db_creds('sales_data_creds.yaml')
        engine = DatabaseConnector.init_db_engine(credentials)
        if engine is not None:
            try:
                # Upload the Dataframe to the specified table
                dataframe.to_sql(table_name, engine, index=False, if_exists = 'append')
                print(f'Data uploaded to table {table_name}')
            except Exception as e:
                # Handle exceptions during the upload process
                print(f'An error occured while uploading to database: {e}')
        else:
            # Handle cases where the database engine is not initialized
            print("Database engine is not initialised.")

