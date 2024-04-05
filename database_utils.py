#%%

import yaml
import sqlalchemy
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.reflection import Inspector

class DatabaseConnector:
    def __init__(self):
        # Initialize the database engine
        self.engine = self.init_db_engine()

    def read_db_creds(self, filepath= 'db_creds.yaml'):
        """
        Reads the database credentials from a YAML file.

        :param filepath: Path to the YAML file containing the credentials.
        :return: Dictionary of database credentials.
        """
        try:
            with open(filepath, 'r') as file:
                creds = yaml.safe_load(file)
                print("Credentials Loaded:", creds)  # Debugging print
                return creds
        except Exception as e:
            print(f"Failed to read database credentials: {e}")
            return None
    def init_db_engine(self):
        """
        Initializes and returns an SQLAlchemy database engine using credentials from the YAML file.

        :return: SQLAlchemy database engine.
        """
        creds = self.read_db_creds()
        db_url = f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        return create_engine(db_url)

    def list_db_tables(self):
        """
        Lists all tables in the database.

        :return: List of table names.
        """
        inspector = Inspector.from_engine(self.engine)
        return inspector.get_table_names()

    def upload_to_db(self, df, table_name):
        """
        Uploads a pandas DataFrame to a specified table in the database.

        :param df: Pandas DataFrame to upload.
        :param table_name: Name of the database table to upload the DataFrame to.
        """
        df.to_sql(name=table_name, con=self.engine, if_exists='append', index=False)


