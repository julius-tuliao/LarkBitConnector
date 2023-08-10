import os
import requests
import time


class FileManager:
    @staticmethod
    def save_file(folder, file_name, content):
        path = f'{folder}/{file_name}'
        with open(path, 'wb') as f:
            f.write(content)
        return path

    @staticmethod
    def file_size_is_valid(path):
        return os.path.getsize(path) > 0

    @staticmethod
    def delete_file(path):
        os.remove(path)