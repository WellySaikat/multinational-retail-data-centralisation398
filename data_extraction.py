import pandas as pd
import tabula
class DataExtractor:
    def __init__(self):
       
        pass

    def extract_from_csv(self, file_path):
        
        pass

    def extract_from_api(self, url, params=None):
        
        pass

    def extract_from_s3(self, bucket_name, file_name, aws_access_key_id, aws_secret_access_key):
        
        pass

    def read_rds_table(self, database_connector, table_name):
        """
        Reads data from an RDS database table into a pandas DataFrame.

        :param database_connector: Instance of DatabaseConnector for database access.
        :param table_name: Name of the table to extract data from.
        :return: pandas DataFrame containing the data from the specified table.
        """
        with database_connector.engine.connect() as connection:
            return pd.read_sql_table(table_name, con=connection)
        
    def retrieve_pdf_data(self, link):  #should take https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf as arg
        """
        Extracts data from all pages of a PDF document available at the given link.

        :param link: URL or local path to the PDF document.
        :return: pandas DataFrame containing the extracted data.
        """
       
        dfs = tabula.read_pdf(link, pages='all', multiple_tables=True)

        
        if dfs:
            return pd.concat(dfs, ignore_index=True)
        else:
            return pd.DataFrame()  # Return an empty DtaFrame if no data was extracted
        