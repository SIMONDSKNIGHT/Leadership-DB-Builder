import csv
import requests
import datetime 
from datetime import date
import time
import os
from tqdm.auto import tqdm
import json

class RestApiInterface:
    def __init__(self, rest_server_url, api_key, start_date, end_date):
        
        self.rest_server_url = rest_server_url
        
        self.api_key =  api_key
        self.start_date = start_date
        self.end_date = end_date
        self.reports = []

    def save_reports(self,mode=2,dlmode = 2, report_type=2):
        while self.end_date >= self.start_date:
            day_files= self.day_request(mode)
            day_files = self.sort_reports(day_files, report_type)
            self.start_date = self.start_date + datetime.timedelta(days=1)
            self.reports.extend(day_files)



    def sort_reports(self,day_files,report_type):
            
        terms = []
        if report_type == 0:
            terms = ["有価証券報告書"]
        elif report_type == 1:
            terms = ["四半期報告書"]
        elif report_type == 2:
            terms = ["有価証券報告書", "四半期報告書"]
        
        matching_reports = []
        # print the json

        for item in day_files['results']:
            
            
            if item['docDescription'] == None:
                continue
            elif any(term in item['docDescription'] for term in terms):
    
            # if any (term in item['docDescription'] for term in terms):
                matching_reports.append(item)
        return matching_reports
    

    def day_request(self,mode):
        formatted_date = self.start_date.strftime('%Y-%m-%d')
        response = requests.get(f'{self.rest_server_url}.json?date={formatted_date}&type={mode}&Subscription-Key={self.api_key}')
        
        json_data = response.json()
        return json_data
    
    def get_reports(self): #getter
        return self.reports
    def read_reports(self):
        for filepath in os.listdir('data/json'):
            with open(f'data/json/{filepath}','r') as f:
                data = json.load(f)
                self.reports=data
                break
    
    def write_reports(self):
       
        # get the file in the json folder and join it with the new one 
        for file in os.listdir('data/json'):
            try:
                with open(f'data/json/{file}','r') as f:
                    data = json.load(f)
                    
                    #append it to the start of reports
                    self.reports = data + self.reports
                    break
            except:
                print("no file found.")
        
        
        with open(f'data/json/{self.start_date.strftime("%Y-%m-%d")}.json','w') as f:
            json.dump(self.reports,f,indent=4,ensure_ascii=False)
        return "Wrote reports to json file."

    def download_report(self, doc_id,mode=2,tse_no):
        if mode == 2:
            extension = 'pdf'
        else:
            extension = 'zip'
        local_filename = f'files/{tse_no}/{doc_id}.{extension}'
        if os.path.exists(local_filename):
            print(f"File {local_filename} already exists. Skipping download.")
            return
        time.sleep(0.5)
        response = requests.get(f'{self.rest_server_url}/{doc_id}?type={mode}&Subscription-Key={self.api_key}')
        
        

        # Open the file in write-binary mode and write the content
        with open(local_filename, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:  # Filter out keep-alive new chunks
                    file.write(chunk)

if __name__ == '__main__':
    rest_server_url = 'https://api.edinet-fsa.go.jp/api/v2/documents'
    with open('main/api_key.txt', 'r') as file:
        api_key = file.read()
    start_date = datetime.datetime.now() - datetime.timedelta(days=2)
    end_date = datetime.datetime.now()
    rest_server_interface = RestApiInterface(rest_server_url, api_key, start_date, end_date)
    rest_server_interface.save_reports()
    rest_server_interface.write_reports()
   
        

