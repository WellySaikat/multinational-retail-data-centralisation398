# %%
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

class DataPipeline:
    def __init__(self):
        self.db_connector = DatabaseConnector()
        self.data_extractor = DataExtractor()
        self.data_cleaning = DataCleaning()

    def list_db_tables(self):
        print(self.db_connector.list_db_tables())

    def process_pdf_data(self, link):
        pdf_data_df = self.data_extractor.retrieve_pdf_data(link)
        cleaned_pdf_data_df = self.data_cleaning.clean_user_data(pdf_data_df)
    
        self.db_connector.upload_to_db(cleaned_pdf_data_df, 'dim_card_details')
        return cleaned_pdf_data_df

    def extract_clean_upload(self, table_name):
        df = self.data_extractor.read_rds_table(self.db_connector, table_name)
        cleaned_df = self.data_cleaning.clean_user_data(df)
        self.db_connector.upload_to_db(cleaned_df, 'dim_users')


link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'

data_pipeline = DataPipeline()
data_pipeline.list_db_tables()
pdf_data_df = data_pipeline.process_pdf_data(link)



print('PDF Data Columns:')
print(pdf_data_df.columns)
print('legacy_store_details:')


df = data_extractor.read_rds_table(db_connector, 'legacy_store_details') 

print(df.columns)

print('legacy_users:')

df = data_extractor.read_rds_table(db_connector, 'legacy_users')  

print(df.columns)

print('orders_table:')

df = data_extractor.read_rds_table(db_connector, 'orders_table')  

print(df.columns)





# %%
