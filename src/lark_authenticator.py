import os
from .api_request import APIRequest

class LarkAuthenticator(APIRequest):

    def __init__(self):
        self.app_id = os.getenv('APP_ID')
        self.app_secret = os.getenv('APP_SECRET')
        self.tenant_access_token = self.get_tenant_access_token()

    def get_tenant_access_token(self):
        payload = {"app_id": self.app_id, "app_secret": self.app_secret}
        headers = {'Content-Type': 'application/json'}
        response = self.send_request('POST', os.getenv('TENANT_ACCESS_TOKEN_URL'), headers, payload)
        return response.get('tenant_access_token', None)

    def refresh_user_access_token(self, location):
        try:
            with open(location, 'r') as file:
                refresh_token = file.readline().strip()

            if not refresh_token:
                raise ValueError("Stored refresh token is blank")

            payload = {
                "grant_type": "refresh_token",
                "refresh_token": refresh_token
            }
            headers = {
                'Authorization': 'Bearer ' + self.tenant_access_token,
                'Content-Type': 'application/json'
            }

            response = self.send_request('POST', os.getenv('REFRESH_ACCESS_TOKEN_URL'), headers, payload)

            new_refresh_token = response["data"]["refresh_token"]
            new_access_token = response["data"]["access_token"]

            if not new_refresh_token:
                raise ValueError("Received refresh token is blank")

            with open(location, 'w') as file:
                file.write(f'{new_refresh_token}\n{new_access_token}')

            return new_access_token

        except Exception as e:
            print(f"An error occurred: {e}")
            return None

    def get_lark_user_ids(self, agent_emails):
        url = os.getenv('BASE_URL') + "/contact/v3/users/batch_get_id?user_id_type=user_id"
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + self.tenant_access_token
        }
        user_ids = {}

        for i in range(0, len(agent_emails), 50):
            batch_emails = agent_emails[i:i + 50]
            payload = {"emails": batch_emails}
            response = self.send_request('POST', url, headers, payload)
            user_ids_batch = {item['email']: item.get('user_id') for item in response['data']['user_list']}
            user_ids.update(user_ids_batch)

        return user_ids
