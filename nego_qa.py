import os
import re
import pandas as pd
import requests
from requests.exceptions import RequestException
import json
from dotenv import load_dotenv,find_dotenv
from functools import partial
import time
import glob
from PIL import Image
from PIL.ExifTags import TAGS
import numpy as np
from collections.abc import Sequence
import openai


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


load_dotenv(find_dotenv())

openai.api_key = os.getenv("OPEN_AI_API")

class NegoQAClass:

    def __init__(self):
        self.lark_auth = LarkAuthenticator()
        self.contact_manager = LarkContactManager(self.lark_auth)
        self.dataframe_manager = DataframeManager()
        self.bitable_manager = BitableManager(self.lark_auth)
        self.system_prompt = """You are a helpful assistant for the debt collection company SP Madrid and associates. 
        Your task is to correct any spelling discrepancies in the transcribed text. 
        Make sure that the names of the following products are spelled correctly:
        SP Madrid, utang, bayad, Atty. Madrid. this transcription is a debtor and agent collector negotiation . properly identify them 
        for each sentences .Only add necessary punctuation such as periods, commas, and capitalization, and use only the context provided."""
        self.grade_prompt = """You are provided with a transcription of a conversation 
        between an agent collector and a debtor. Your task is to grade the conversation based on the following parameters:"""
        self.initial_content_prompt = """You are provided with a transcription of a conversation between an agent collector and a debtor. Your task is to grade the conversation based on the following parameters:

A. OPENING THE CALL

Did the agent use an appropriate greeting?
B. POSITIVE IDENTIFICATION
2. Did the agent introduce himself/herself properly, stating they are from a debt collection agency?

Did the agent identify the person they contacted?
C. PROBLEM SOLVING/COLLECTION SKILLS (WITH RIGHT PARTY CONTACT)

Did the agent complete verification?
Did the agent follow the current PID set?
Did the agent explain the status of the account?
Did the agent provide complete and accurate account information?
Did the agent ask for the reason for Non-payment?
Did the agent follow the hierarchy of negotiation?
Did the agent explain the urgency of payment and consequences of Non-payment?
Was the agent able to obtain a Promise To Pay (PTP) within 3 days?
Did the agent probe for other contact numbers?
Did the agent escalate the call to the Team Leader (TL) as needed?
Did the agent effect or escalate to MCC, whichever is applicable, all necessary action points for the account in compliance with Standard Operating Procedures (SOP)?
D. NEGATIVE CONTACT

Did the agent probe for other contact details?
Did the agent request BTC from a 3rd party?
Did the agent leave a message for CH to return the call?
Did the agent comply with guidelines on account information disclosure to unauthorized third parties?
E. CLOSING THE CALL

Did the agent summarize the actions taken or agreements reached during the call?
Did the agent use the correct closing spiel?
G. COMMUNICATION SKILLS

Did the agent use the correct tone, volume, and rate of speech?
Did the agent match the customer's language and use the correct grammar, vocabulary, and pronunciation?
Did the agent use appropriate empathy and apology during the conversation?
Did the agent maintain professionalism and refrain from using verbally abusive language?
Did the agent exercise active listening?
Please provide a score for each parameter by marking it as "Y" for "Yes" or "N" for "No." If the parameter is not applicable, mark it as "NA."
Add also total score : 100 is the perfect score deduct 1 for every "N" mark. 
Add also the total count of "N":  Ex: 5
"""

        
    
    def get_filtered_df(self):
        records = self.bitable_manager.get_records("tblLmaT4GDT8rtpi", filter="CurrentValue.[Status]!=%22Done%22")
        field_map = {
            'Code': 'Id',
            'Record': 'Attachment',
            'Record Id': ['Record Id', 'text']
        }

        
        extracted_df = self.dataframe_manager.get_dataframe_from_records(records, field_map)
  
        return extracted_df
    
    def transcribe(self,mp3_file):
         # Step 1: Transcribe the audio using the Whisper API
        with open(mp3_file, "rb") as audio_file:
            transcript = openai.Audio.transcribe("whisper-1", audio_file,prompt="SP Madrid, Sp Madrid law firm ,utang, outstanding balance, pesos,MBTC,Metro Bank")
            transcription_text = transcript['text']

        return transcription_text


    def generate_corrected_transcript(self,temperature, audio_file):
        response = openai.ChatCompletion.create(
            model="gpt-4",
            temperature=temperature,
            messages=[
                {
                    "role": "system",
                    "content": self.system_prompt
                },
                {
                    "role": "user",
                    "content": self.transcribe(audio_file)
                }
            ]
        )
        return response['choices'][0]['message']['content']

    
    def generate_grade(self,transcription):
        response = openai.ChatCompletion.create(
            model="gpt-4",
            temperature=0,
            messages=[
                {
                    "role": "system",
                    "content": self.grade_prompt
                },
                {
                    "role": "user",
                    "content": self.initial_content_prompt + f" Transcription:{transcription}"
                }
            ]
        )
        return response['choices'][0]['message']['content']



    def main(self):

        filtered_df = self.get_filtered_df()

        # Create the 'recordings' folder if it doesn't exist
        if not os.path.exists('recordings'):
            os.makedirs('recordings')

                  
        backup_manager = BackupManager()
        user_access_token = self.lark_auth.refresh_user_access_token()


        # Download the files first
        filtered_records = []
        downloaded_links=[]
        for index, row in filtered_df.iterrows():

            nego = row['Record']
            
            link = nego[0]["url"]
            file_name = nego[0]["name"]
            headers = {'Authorization': 'Bearer ' + user_access_token}
            downloaded_link = requests.get(link, headers=headers)
            downloaded_links.append(downloaded_link)

            # Save the file to the 'recordings' folder
            with open(f'recordings/{file_name}', 'wb') as f:
                f.write(downloaded_link.content)
            
            # Append the record and file_name to the filtered_records list
            filtered_records.append((row, file_name))


        # Process all MP3 files in the 'recordings' folder
        folder_path = 'recordings'
        os.chdir(folder_path)
        mp3_files = glob.glob('*.mp3')
        
        for record, mp3_file in filtered_records:
            
            print(record["Record Id"])
            # Step 1: Transcribe the audio using the Whisper API
            corrected_text = self.transcribe(mp3_file)

            # Save the transcribed audio to a text file
            transcript_filename = f"transcript_{os.path.splitext(mp3_file)[0]}.txt"
            with open(transcript_filename, "w") as transcript_file:
                transcript_file.write(corrected_text)
            
            grade= self.generate_grade(corrected_text)

            with open("grade_" + transcript_filename, "w") as transcript_file:
                transcript_file.write(grade)

            update_record = {
                "fields": {
                    "Status": "Done",  
                    "Transcription": corrected_text,
                    "QA": grade
                } 
            }

            print(update_record)
            update_response = self.bitable_manager.update_rows_to_bitable( 'tblLmaT4GDT8rtpi', record["Record Id"],update_record)

            print(f'Update response: {update_response}')
        



if __name__ == "__main__":
    while True:
        NegoQAClass().main()
        print("done processing.. will sleep for 30 sec")
        time.sleep(30)
