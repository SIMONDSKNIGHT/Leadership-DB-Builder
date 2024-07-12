from typing import Any
import re
import pandas as pd
class WorkHistoryProcessor:
    def __init__(self):
        self.text = ''

    def process_work_history(self,text):
        self.text = text
        #remove full width characters
        self.replace_colons()
        self.standardise_dates()    




        #regex every YYYY年MM月 or YYYY年M月

    def split_text(self):

        entries = re.findall(r'\d{4}/\d{1,2}:.*?(?=\d{4}/\d{1,2}:|$)', self.text)
        # Clean up each entry and store it in a list
        self.text= [entry.strip() for entry in entries]
        
    def standardise_dates(self, ):
        # Replace full-width characters with half-width
        text = re.sub(r'[０-９]', lambda x: chr(ord(x.group(0)) - 65248), self.text)
        # Replace Japanese numerals with Arabic numerals
        text = text.translate(str.maketrans('０１２３４５６７８９　', '0123456789 '))
        text = text.translate(str.maketrans('〇一二三四五六七八九', '0123456789'))

        # Convert Japanese era years to Gregorian years
        era_dict = {
            '昭和': 1925,
            '平成': 1988,
            '令和': 2018
        }
        for era, offset in era_dict.items():
            text = re.sub(f'{era}(\d+)年', lambda x: f'{offset + int(x.group(1))}年', text)

        # Normalize date formats
        text = re.sub(r'(\d{4})年\s*(\d{1,2})月', lambda m: f'{m.group(1)}/{int(m.group(2)):02d}:', text)
        text = re.sub(r'(\d{4})年\s*(\d{1,2})月', lambda m: f'{m.group(1)}/{int(m.group(2)):02d}:', text)
        text = re.sub(r'(\d{4})/(\d{2})', lambda m: f'{m.group(1)}/{int(m.group(2)):02d}:', text)
        
        text = re.sub(r'(\d{4})年度', lambda m: f'{m.group(1)}/01:', text)  # Fix to automatically set month to 01

        
        self.text = text
        self.replace_same_year_month()
        self.loose_months()

    def replace_same_year_month(self):
        full_dates = re.findall(r'\d{4}/\d{2}:', self.text)
        # Replace "同年" with the last found year
        try:
            if full_dates:
                last_year = re.findall(r'\d{4}', full_dates[-1])[0]
                self.text = re.sub(r'同\s*年\s*(\d{1,2})月', lambda m: f'{last_year}/{int(m.group(1)):02d}:', self.text)
                self.text = re.sub(r'同年同月', f'{last_year}/{full_dates[-1][5:7]}:', self.text)
                self.text = re.sub(r'同\s*年\s*同\s*月', f'{last_year}/{full_dates[-1][5:7]}:', self.text)
            # Find all full dates again after replacement
            full_dates = re.findall(r'\d{4}/\d{2}:', self.text)
            
            # Replace "同月" with the last found month
            if full_dates:
                last_year_month = re.findall(r'\d{4}/\d{2}', full_dates[-1])[0]
                self.text = re.sub(r'同月', f'{last_year_month}:', self.text)
        
        except:
            print("######################################################################")
            print(self.text)
            exit()
    def loose_months(self):
        full_dates = re.findall(r'\d{4}/\d{2}:', self.text)
        if full_dates:
            last_year = re.findall(r'\d{4}', full_dates[-1])[0]
            # Replace loose months with the last known year, ignoring cases where the previous character is 年
            self.text = re.sub(r'(?<!年)(?<!\d)(\d{1,2})月', lambda m: f'{last_year}/{int(m.group(1)):02d}:', self.text)
    def replace_colons(self):

        self.text = self.text.replace(':', ',')
        self.text = self.text.replace(';', ',')

    def get_text(self):
        return self.text

        

if __name__ == '__main__':
    df = pd.read_csv('test1.csv')
    work_history = WorkHistoryProcessor()
    for index, row in df.iterrows():
        print(row['Work History'])
        text = row ['Work History']
        work_history.process_work_history(text)
        work_history.split_text()
        print("PROCESSING")
        for i in work_history.get_text():
            work_history.problem_identifier(i)
            #if something was printed, print row
        print(row['TSE:'], row['Name'])

        e = input('Press q to quit')
        if e == 'q':
            break