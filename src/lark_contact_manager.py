import os
from .api_request import APIRequest
from .lark_authenticator import LarkAuthenticator
from dotenv import load_dotenv,find_dotenv

# Find .env file
load_dotenv(find_dotenv())

class LarkContactManager(APIRequest):

    def __init__(self, authenticator: LarkAuthenticator):
        self.authenticator = authenticator
    
    def get_lark_user_ids(self, agent_emails):
        tenant_token = self.authenticator.get_app_access_token()
        url = os.getenv('BASE_URL') + "/contact/v3/users/batch_get_id?user_id_type=user_id"
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + tenant_token
        }
        user_ids = {}

        for i in range(0, len(agent_emails), 50):
            batch_emails = agent_emails[i:i + 50]
            payload = {"emails": batch_emails}
            response = self.send_request('POST', url, headers, payload)
            user_ids_batch = {item['email']: item.get('user_id') for item in response['data']['user_list']}
            user_ids.update(user_ids_batch)

        return user_ids
