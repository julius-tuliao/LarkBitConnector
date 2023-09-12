import os
from .api_request import APIRequest
from .lark_authenticator import LarkAuthenticator
from datetime import datetime
from utilities.retry_decorator  import Decorator
from dotenv import load_dotenv,find_dotenv
import time
import json

# Find .env file
load_dotenv(find_dotenv())

retry = Decorator()

class BitableManager(APIRequest):
    
    def __init__(self, authenticator: LarkAuthenticator):
        self.bitable_id = os.getenv('BITABLE_ID')
        self.authenticator = authenticator
        

    @retry.retry(tries=3, delay=10, backoff=2)
    def add_rows_to_bitable_batch(self, table_id, records_batch):
        tenant_token = self.authenticator.get_tenant_access_token()
        url = f"{os.getenv('BITABLE_BASE_URL')}/{self.bitable_id}/tables/{table_id}/records/batch_create?user_id_type=user_id"
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {tenant_token}'
        }
        payload = {"records": [{"fields": record} for record in records_batch]}
        return self.send_request('POST', url, headers, payload)

    @retry.retry(tries=3, delay=10, backoff=2)
    def add_rows_to_bitable(self, table_id, row):
        tenant_token = self.authenticator.get_tenant_access_token()
        url = f"{os.getenv('BITABLE_BASE_URL')}/{self.bitable_id}/tables/{table_id}/records?user_id_type=user_id"
        headers = {
            'Authorization': 'Bearer ' + tenant_token,
            'Content-Type': 'application/json'
        }
        payload = {"fields": row}
        print(payload)
        return self.send_request('POST', url, headers, payload)

    @retry.retry(tries=3, delay=10, backoff=2)
    def get_records(self, table_id, page_size=500, filter=None):
        tenant_token = self.authenticator.get_tenant_access_token()
        page_token = None
        has_more = True
        records = []
        headers = {'Authorization': 'Bearer ' + tenant_token}

       
        while has_more:
            try:
                base_url = f"{os.getenv('BITABLE_BASE_URL')}/{self.bitable_id}/tables/{table_id}/records"
                filter_str = f"&filter={filter}" if filter else ""
                
                url = f"{base_url}?page_size={page_size}{filter_str}"
                
                if page_token:
                    url += f"&page_token={page_token}"
                response = self.send_request('GET', url, headers)
                has_more = response["data"]["has_more"]
                # has_more = False
                page_token = response["data"]["page_token"] if has_more else None
                records.extend(response["data"]["items"])

                time.sleep(5)

            except Exception as e:
                print(e)
                print("retry getting records")

        return records
        

    @retry.retry(tries=3, delay=10, backoff=2)
    def update_rows_to_bitable_batch(self, table_id, records_batch):
        url = f"{os.getenv('BITABLE_BASE_URL')}/{self.bitable_id}/tables/{table_id}/records/batch_update"
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self._get_user_access_token()}'
        }
        payload = {'records': [{'fields': record['fields'], 'record_id': record['record_id']} for record in records_batch]}
    
        return self.send_request('POST', url, headers, payload)

    @retry.retry(tries=100, delay=15, backoff=2)
    def update_rows_to_bitable(self, table_id, record_id, updated_fields):
        
        url = f"{os.getenv('BITABLE_BASE_URL')}/{self.bitable_id}/tables/{table_id}/records/{record_id}"
        headers = {
            'Authorization': f'Bearer {self._get_user_access_token()}',
            'Content-Type': 'application/json'
        }
        payload = updated_fields
        return self.send_request('PUT', url, headers, payload)

    @retry.retry(tries=3, delay=10, backoff=2)
    def delete_record(self, table_id, record_id):
        tenant_token = self.authenticator.get_tenant_access_token()
        url = f"{os.getenv('BITABLE_BASE_URL')}/{self.bitable_id}/tables/{table_id}/records/{record_id}"
        headers = {
            'Authorization': 'Bearer ' + tenant_token,
            'Content-Type': 'application/json'
        }
        return self.send_request('POST', url, headers)
    
    @retry.retry(tries=3, delay=10, backoff=2)
    def delete_record_batch(self, table_id, records_batch):
        
        url = f"{os.getenv('BITABLE_BASE_URL')}/{self.bitable_id}/tables/{table_id}/records/batch_delete"
        
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self._get_user_access_token()}'
        }
        payload = {
            'records': records_batch
        }

        try:
            return self.send_request('POST', url, headers, payload)
            
        except Exception as e:
            print(f"Error occurred while trying to send request: {e}")
            raise  # re-raise the exception so the retry decorator can catch it


    @retry.retry(tries=3, delay=10, backoff=2)
    def _get_user_access_token(self):
        refresh_token_path = os.getenv('REFRESH_TOKEN_FILE_PATH')
        absolute_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', refresh_token_path)
        with open(absolute_path, 'r') as file:
            return file.readlines()[1].strip()
        
    @retry.retry(tries=3, delay=10, backoff=2)
    def process_dl_date(self,date_str):
        if not str(date_str).isalpha():
            time = datetime.strptime(str(date_str), "%Y-%m-%d")
            return int((time - datetime(1970, 1, 1)).total_seconds() * 1000)
        return 1568470400000  # Default date set for null date in CRM
