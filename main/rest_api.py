import csv
import requests
import datetime as dt
from datetime import datetime 
import time
import os
from tqdm.auto import tqdm
import json
import sys
from tqdm import tqdm


class RestApiInterface:
    def __init__(self, rest_server_url, api_key, id_list,start_date, end_date):
        self.id_list = id_list
        self.rest_server_url = rest_server_url
        
        self.api_key =  api_key
        self.start_date = start_date
        self.end_date = end_date
        self.reports = []

    def save_reports(self,mode=2,dlmode = 2, report_type=2):
        print(self.start_date)
        print(self.end_date)
        # create tqdm progress bar, noting that dates are strings in format yyyy/mm/dd
        pbar = tqdm(total=(datetime.strptime(self.end_date, "%Y/%m/%d") - datetime.strptime(self.start_date, "%Y/%m/%d")).days)
        while self.end_date >= self.start_date:
            day_files= self.day_request(mode)
            print(day_files)

            day_files = self.sort_reports(day_files, report_type)


            self.start_date = (datetime.strptime(self.start_date, "%Y/%m/%d") + dt.timedelta(days=1)).strftime("%Y/%m/%d")

            self.reports.extend(day_files)

            pbar.update(1)
        print('finished saving reports')


            



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
        try:
            for item in day_files['results']:

        
                if item['docDescription'] == None:
                    continue
                elif any(term in item['docDescription'] for term in terms):
                    if item['secCode'] == None:
                        continue
                    if item['secCode'][:4] not in self.id_list:
                        continue

        
                # if any (term in item['docDescription'] for term in terms):
                    matching_reports.append(item)
            
            return matching_reports
        except:
            print('no results')
            return []
        
    

    def day_request(self,mode):
        formatted_date = self.start_date.replace('/', '-')
        response = requests.get(f'{self.rest_server_url}.json?date={formatted_date}&type={mode}&Subscription-Key={self.api_key}')
        json_data = response.json()
        print(json_data)
        try:
            if json_data['results'] == []:
                print('no results')
                
        except:
            print('no results')
            return json_data
        time.sleep(0.1)

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
        #print error if no file is found
        for file in os.listdir('data/json'):
            this_filepath = f'data/json/{file}'
            try:
                with open(f'data/json/{file}','r') as f:
                    data = json.load(f)
                    
                    #append it to the start of reports
                    self.reports = data + self.reports
                    os.remove(this_filepath)
                    break
            except:
                print("no file found.")
        #delete old file

        
        
        with open(f'data/json/{self.end_date.replace('/', '-')}.json','w') as f:
            json.dump(self.reports,f,indent=4,ensure_ascii=False)
        return "Wrote reports to json file."

    def download_report(self, doc_id,tse_no,mode=2):
        if mode == 2:
            extension = 'pdf'
        else:
            extension = 'zip'
        local_filename = f'files/{tse_no}/{doc_id}/{doc_id}.{extension}'
        if os.path.exists(local_filename):
            print(f"File {local_filename} already exists. Skipping download.")
            return
        time.sleep(0.1)
        response = requests.get(f'{self.rest_server_url}/{doc_id}?type={mode}&Subscription-Key={self.api_key}')
        
        

        # Open the file in write-binary mode and write the content
        with open(local_filename, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:  # Filter out keep-alive new chunks
                    file.write(chunk)
        return local_filename
 

