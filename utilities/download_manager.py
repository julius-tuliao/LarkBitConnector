import os
import requests
import time

class DownloadManager:
    def __init__(self, max_retries=3):
        self.max_retries = max_retries

    def download_with_retry(self, link, headers):
        # Create the 'images' folder if it doesn't exist
        if not os.path.exists('images'):
            os.makedirs('images')

            
        for _ in range(self.max_retries):
            try:
                response = requests.get(link, headers=headers)
                if response.status_code == 200:
                    return response.content
            except Exception as e:
                print(f"Error: {e}")
        return None


    