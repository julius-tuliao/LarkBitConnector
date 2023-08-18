import requests


class APIRequest:
    @staticmethod
    def send_request(method, url, headers, payload=None):
        try:
            if method == 'POST':
                response = requests.post(url, headers=headers, json=payload)
            elif method == 'GET':
                response = requests.get(url, headers=headers, params=payload)
            elif method == 'PUT':
                response = requests.put(url, headers=headers, json=payload)
            elif method == 'DELETE':
                response = requests.delete(url, headers=headers)
            else:
                raise ValueError("Unsupported method")
            
            response.raise_for_status()

            return response.json()
        except requests.RequestException as e:
            raise Exception(f"Error occurred: {e}")
