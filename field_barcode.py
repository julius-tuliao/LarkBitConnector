import os
import re
import pandas as pd
import requests
from requests.exceptions import RequestException
import json
from dotenv import load_dotenv
from functools import partial
import time
import glob
from PIL import Image
from PIL.ExifTags import TAGS
import numpy as np
from collections.abc import Sequence

# Assuming you've separated each class into its module.
from lark.lark_authenticator import LarkAuthenticator
from lark.lark_contact_manager import LarkContactManager
from lark.bitable_manager import BitableManager
from db.mysql_database import Database
from utilities.download_manager import DownloadManager
from utilities.file_manager import FileManager
from utilities.backup_manager import BackupManager
from utilities.dataframe_manager import DataframeManager
from pathlib import Path
from dogpile.cache import make_region


load_dotenv()

region = make_region().configure(
    'dogpile.cache.memory',
    expiration_time=3600  # 1 hour
)


class FieldReleasedClass:

    def __init__(self):
        self.lark_auth = LarkAuthenticator()
        self.database = Database()
        self.contact_manager = LarkContactManager(self.lark_auth)
        self.dataframe_manager = DataframeManager()
        self.bitable_manager = BitableManager(self.lark_auth)
    
    def get_filtered_df(self):
        records = self.bitable_manager.get_records("tblOZvkoDY8jCBtC", filter="CurrentValue.[Done]!=%22Done%22")
        field_map = {
            'Correct Code': ['Correct Code', 'text'],
            'Field Name': 'Field Name',
            'Record': ['Record', 'text']
        }

        
        extracted_df = self.dataframe_manager.get_dataframe_from_records(records, field_map)
        print(extracted_df)

        if 'Correct Code' not in extracted_df.columns:
            raise ValueError("'Correct Code' column is missing in extracted_df.")
        extracted_df['Correct Code'] = extracted_df['Correct Code'].apply(lambda x: str(x).replace('\u200b', ''))

        filtered_df = extracted_df[extracted_df['Field Name'].notnull() & (extracted_df['Field Name'] != '') & extracted_df['Correct Code'].notnull() & (extracted_df['Correct Code'] != '')]
        filtered_df.reset_index(drop=True, inplace=True)
        return filtered_df

    def execute_sql_query(self, filtered_df):
        codes = filtered_df['Correct Code'].tolist()
        codes_string = ', '.join(f'"{code}"' for code in codes)
        query_path = Path('query/query_released.sql')
        with query_path.open('r') as file:
            sql_query = file.read().format(codes_string=codes_string)

        results, columns = self.database.execute_select_with_column_name(sql_query)
        query_df = pd.DataFrame(results, columns=columns)
        query_df = query_df[query_df['is_pullout'] != True]  
        query_df = query_df[query_df['agent_username'] != "POUT"]

        return pd.merge(filtered_df, query_df, left_on='Correct Code', right_on='leads_ref', how='inner')

    def get_oic_df(self):
        oic_list = self.bitable_manager.get_records("tblhPXVCDGH4ENqt29E")
        field_map = {
            'area': 'AREA',
            'Region': 'Region',
            'oic_email': ['OIC LARK NAME', 'email']
        }
        oic_df = self.dataframe_manager.get_dataframe_from_records(oic_list, field_map)
        oic_email = oic_df['oic_email'].tolist()
        oic_lark_user_ids = self.contact_manager.get_lark_user_ids(oic_email)
        oic_df['oic'] = oic_df['oic_email'].apply(lambda x: [oic_lark_user_ids.get(x)] if oic_lark_user_ids.get(x) else [])
        return oic_df

    def get_fs_df(self):
        fs_list = self.bitable_manager.get_records("tbli8g5Rg9o6tI7vcra")
        field_map = {
            'Field Name': 'Name',
            'field_email': ['Lark Name', 'email']
        }
        fs_df = self.dataframe_manager.get_dataframe_from_records(fs_list, field_map)
        fs_email = fs_df['field_email'].tolist()
        fm_lark_user_ids = self.contact_manager.get_lark_user_ids(fs_email)
        fs_df['field_lark'] = fs_df['field_email'].apply(lambda x: [fm_lark_user_ids.get(x)] if fm_lark_user_ids.get(x) else [])
        return fs_df
    
    @region.cache_on_arguments()
    def get_field_request(self):
        request_list = self.bitable_manager.get_records("tbl5LkNbwuqjmej41kX")
        field_map = {
            'leads_ref': 'Ref Code'
        }
        request_df = self.dataframe_manager.get_dataframe_from_records(request_list, field_map)
        
        return request_df
    
    @region.cache_on_arguments()
    def get_field_result(self):
        request_list = self.bitable_manager.get_records("tblctYupx7p1jfSgmyT")
        field_map = {
            'leads_ref': 'reference_code'
        }
        request_df = self.dataframe_manager.get_dataframe_from_records(request_list, field_map)
        
        return request_df
    
    def process_batch(self,batch):
        
        fields_list = []
        for _, row in batch.iterrows():
            dl_date =  self.bitable_manager.process_dl_date(row["leads_date"])

            fields = {
                "Ref Code": row['leads_ref'],
                "DL Request Date": dl_date
            }

            fields_list.append(fields)

        response_data = self.bitable_manager.add_rows_to_bitable_batch( "tbl5LkquqHnjmej41kX", fields_list)

        if response_data.get('msg') == 'success':
             # Prepare the update payload using the record IDs from the "Record" column
            update_records_batch = []
            for _, row in batch.iterrows():
                update_record = {
                    "fields": {
                        "Done": "Done"  # Set the "Done" column to have the value "Done"
                    },
                    "record_id": row["Record"]  # Use the record ID from the "Record" column
                }
                
                update_records_batch.append(update_record)

            # Call the update method
            update_response = self.bitable_manager.update_rows_to_bitable_batch( 'tblOZvkoDY8jCBtC', update_records_batch)
            print(f'Update response: {update_response}')


        else:
            print('The msg key is NOT equal to "success".')



        return response_data



    def main(self):
        if not self.database.connect():
            print("Failed to connect to the database!")
            exit(1)

        filtered_df = self.get_filtered_df()
        query_df = self.execute_sql_query(filtered_df)
        oic_df = self.get_oic_df()
        fs_df = self.get_fs_df()
        request_df = self.get_field_request()
        # result_df = self.get_field_result()

        merged_df = query_df.merge(oic_df, on="area", how="left")
        merged_df = merged_df.merge(fs_df, on="Field Name", how="left")

        # Merge request_df and merged_df on the 'leads_ref' column and add an indicator column to track the source of each row.
        merged_with_request = pd.merge(merged_df, request_df[['leads_ref']], on='leads_ref', how='outer', indicator=True)

        # Filter out rows that were present in both merged_df and request_df based on the 'leads_ref' column.
        filtered_df_request = merged_with_request[merged_with_request['_merge'] == 'left_only'].copy()

        # Drop the indicator column.
        filtered_df_request.drop('_merge', axis=1, inplace=True)

        merged_df = filtered_df_request

        agent_email = merged_df['agent_email'].tolist()
        agent_lark_user_ids = self.contact_manager.get_lark_user_ids(agent_email)
        merged_df['Agent Lark'] = merged_df['agent_email'].apply(lambda x: [agent_lark_user_ids.get(x)] if agent_lark_user_ids.get(x) else [])


        merged_df.to_csv('path_to_output_file.csv', index=False)

        # Split the DataFrame into batches
        batch_size = 100  # Set the desired batch size
        responses = []
        for i in range(0, len(merged_df), batch_size):
            batch = merged_df.iloc[i:i+batch_size]
            response = self.process_batch(batch)
            responses.append(response)
          
        backup_manager = BackupManager()
        user_access_token = self.lark_auth.refresh_user_access_token()
  


if __name__ == "__main__":
    
    while True:
        FieldReleasedClass().main()
        print("done processing.. will sleep for 5 minutes")
        time.sleep(600)
