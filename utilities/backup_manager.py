
import pandas as pd


class BackupManager:
    def __init__(self, backup_file_path="backup_records.xlsx"):
        self.backup_file_path = backup_file_path
        self.load_existing_backup()

    def load_existing_backup(self):
        try:
            self.backup_df = pd.read_excel(self.backup_file_path, engine='openpyxl')
        except FileNotFoundError:
            self.backup_df = pd.DataFrame()

    def add_record(self, record):
        # Directly append the record to the backup DataFrame
        new_record_df = pd.DataFrame([record])
        self.backup_df = pd.concat([self.backup_df, new_record_df], ignore_index=True)

    def save_to_file(self):
        self.backup_df.to_excel(self.backup_file_path, index=False, engine='openpyxl')