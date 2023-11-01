import pandas as pd
from database_utils import DatabaseConnector

list_of_tables = DatabaseConnector.list_db_tables()
print(list_of_tables)

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

user_data = DataExtractor.read_rds_table('legacy_users')
print(user_data)