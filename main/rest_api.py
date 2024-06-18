import csv
import requests
import datetime 
from datetime import date

class RestApiInterface:
    def __init__(self):
        
        self.rest_server_url = 'https://api.edinet-fsa.go.jp/api/v2/documents.json?'
        self.api_key ="ce7040824a7b414e9328a6edc0e82200"

    def search_reports(self, start_date, end_date):
        # Read the CSV file
        
        # Iterate over each day in the date range
        current_date = date(start_date.year, start_date.month, start_date.day)
        end_date = date(end_date.year, end_date.month, end_date.day)
        while current_date <= end_date:
            # Format the date in the desired format for the REST server

            # Move to the next day
            current_date = current_date + datetime.timedelta(days=1)
    def get_response(self, date, mode=1):
        formatted_date = date.strftime('%Y-%m-%d')

        # Make a request to the REST server for the given date
        response = requests.get(f'{self.rest_server_url}date={formatted_date}&type={mode}&Subscription-Key={self.api_key}')
        json_data = response.json()
        return json_data

