import os
from .api_request import APIRequest
from .lark_authenticator import LarkAuthenticator
from dotenv import load_dotenv,find_dotenv
import json
import requests
# Find .env file
load_dotenv(find_dotenv())

class LarkContactManager(APIRequest):

    def __init__(self, authenticator: LarkAuthenticator):
        self.authenticator = authenticator
    
    def get_lark_user_ids(self, emails):
        tenant_token = self.authenticator.get_tenant_access_token()
        url = os.getenv('BASE_URL') + "/contact/v3/users/batch_get_id?user_id_type=user_id"
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + tenant_token
        }

        user_ids = {}

        for i in range(0, len(emails), 50):
            batch_emails = emails[i:i + 50]
            payload = json.dumps({"emails": batch_emails})

            try:
                response = requests.post(url, headers=headers, data=payload)
                response.raise_for_status()
                response_data = response.json()
                
                user_ids_batch = {item['email']: item.get('user_id') for item in response_data['data']['user_list']}
                user_ids.update(user_ids_batch)
            
        
            except Exception as e:
                print(f"Error fetching Lark user_ids: {response.status_code} - {response.text}")

        return user_ids