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

# Assuming you've separated each class into its module.
from lark.lark_authenticator import LarkAuthenticator
from lark.lark_contact_manager import LarkContactManager
from lark.bitable_manager import BitableManager
from db.mysql_database import Database
from utilities.download_manager import DownloadManager
from utilities.file_manager import FileManager
from utilities.backup_manager import BackupManager

load_dotenv()

def main():

    # 1. Authenticate with Lark
    lark_auth = LarkAuthenticator()

    # 2. Retrieve records from a Bitable table.
    bitable_manager = BitableManager(lark_auth)
    table_id = "tblctx7p1jfSgmyT"
    records = bitable_manager.get_records(table_id,filter="CurrentValue.[Downloaded]!=%22Done%22")


    # 3. Download and update
    download_manager = DownloadManager()
    file_manager = FileManager()
    backup_manager = BackupManager()
    user_access_token = lark_auth.refresh_user_access_token()


    for record in records:
        
        if "Downloaded" not in record["fields"] or record["fields"]["Downloaded"] != "Done":
            headers = {'Authorization': 'Bearer ' + user_access_token}

            backup_manager.add_record(record)
            backup_manager.save_to_file()
        # Handle Transmittal Pic
        if "Transmittal Pic" in record["fields"]:
            transmittal = record["fields"]["Transmittal Pic"][0]
            link = transmittal["url"]
            file_name = transmittal["name"]
            reference_code = record["fields"]["reference_code"]
            
            downloaded_content = download_manager.download_with_retry(link, headers)
            if downloaded_content:
                path = file_manager.save_file('images', f'T~{reference_code}~{file_name}', downloaded_content)
                
                if file_manager.file_size_is_valid(path):
                    updated_fields = {"Downloaded": "Done"}
                    response = bitable_manager.update_rows_in_bitable(table_id, record["record_id"], updated_fields)
                    
                    print(response)
                else:
                    file_manager.delete_file(path)

        # Handle Pictures
        if "Pictures" in record["fields"]:
            house = record["fields"]["Pictures"][0]
            link = house["url"]
            file_name = house["name"]
            reference_code = record["fields"]["reference_code"]
            updated_fields = {"Downloaded": "Done"}

            downloaded_content = download_manager.download_with_retry(link, headers)
            if downloaded_content:
                path = file_manager.save_file('images', f'H~{reference_code}~{file_name}', downloaded_content)
                
                if file_manager.file_size_is_valid(path):
                    
                    response = bitable_manager.update_rows_in_bitable(table_id, record["record_id"], updated_fields)
                        

        # Handle Token Refresh
        user_access_token = lark_auth.refresh_user_access_token()

    time.sleep(60)

if __name__ == "__main__":
    main()
