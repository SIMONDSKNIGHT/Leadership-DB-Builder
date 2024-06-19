import csv
import requests
import datetime 
from datetime import date
import time

class RestApiInterface:
    def __init__(self, rest_server_url, api_key):
        
        self.rest_server_url = rest_server_url
        
        self.api_key =  api_key

    def search_reports(self, start_date, end_date, mode=1):
        # Read the CSV file
        
        # Iterate over each day in the date range
        current_date = start_date
        results = {}
        results['results'] = []
        while current_date <= end_date:
            # Get the response for the current date
            results['results']+=((self.get_response(current_date,mode)['results']))
            # Move to the next day
            current_date = current_date + datetime.timedelta(days=1)
            time.sleep(0.1)
        return results
    def get_response(self, date, mode=1):
        formatted_date = date.strftime('%Y-%m-%d')

        # Make a request to the REST server for the given date
        response = requests.get(f'{self.rest_server_url}.json?date={formatted_date}&type={mode}&Subscription-Key={self.api_key}')
        json_data = response.json()
        return json_data

    def download_reports(self, doc_id_list, mode=1):
        for item in doc_id_list:
            doc_id = item[3]
            tse_no = item[0]
            self.download_report(doc_id,mode,tse_no)
            time.sleep(0.1)
        return "Downloaded all reports."
            
    def download_report(self, doc_id,mode,tse_no):
        response = requests.get(f'{self.rest_server_url}/{doc_id}?type={mode}&Subscription-Key={self.api_key}')
        if mode == 2:
            extension = 'pdf'
        else:
            extension = 'zip'
        
        
        local_filename = f'files/{tse_no}-{mode}.{extension}'

        # Open the file in write-binary mode and write the content
        with open(local_filename, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:  # Filter out keep-alive new chunks
                    file.write(chunk)

        

