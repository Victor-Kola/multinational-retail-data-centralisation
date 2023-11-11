import yaml
from sqlalchemy import create_engine, inspect, text

class DatabaseConnector:
    @staticmethod
    def read_db_creds(filename='db_creds.yaml'):
        with open(filename, "r") as stream:
            try:
                db_creds = yaml.safe_load(stream)
                return db_creds
            except yaml.YAMLError as exc:
                print("Error in configuration file:", exc) 
                return None
            
  

    @staticmethod 
    def init_db_engine(credentials = None, db_type="postgresql", dbapi="psycopg2"):
        if credentials is None:
            credentials = DatabaseConnector.read_db_creds()
        if credentials:
            db_str = f"{db_type}+{dbapi}://{credentials['RDS_USER']}:{credentials['RDS_PASSWORD']}@{credentials['RDS_HOST']}:{credentials['RDS_PORT']}/{credentials['RDS_DATABASE']}"
            engine = create_engine(db_str)
            return engine
        
        else: 
            print("Could not read credentials or initialise the database engine.")
            return None
    @staticmethod
    def list_db_tables():
        engine = DatabaseConnector.init_db_engine()
        if engine is not None:
            inspector = inspect(engine)
            table_names = inspector.get_table_names()
            return table_names
        else:
            print("Engine is not initialised")
            return None
    @staticmethod
    def upload_to_db(dataframe, table_name):
        credentials = DatabaseConnector.read_db_creds('sales_data_creds.yaml')
        engine = DatabaseConnector.init_db_engine(credentials)
        if engine is not None:
            try:
                dataframe.to_sql(table_name, engine, index=False, if_exists = 'append')
                print(f'Data uploaded to table {table_name}')
            except Exception as e:
                print(f'An error occured while uploading to database: {e}')
        else:
            print("Database engine is not initialised.")

