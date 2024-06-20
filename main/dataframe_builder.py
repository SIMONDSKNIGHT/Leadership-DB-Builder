import pandas as pd
import os
from csv_parser import CSVParser
import json

class DataFrameBuilder:
    def __init__(self):
        pass
    
    def build_dataframes(self,filepath):
        sumdf = pd.DataFrame()
        for TSENO in os.listdir(filepath):
            if TSENO == ".DS" or TSENO == ".DS_Store":
                continue
            for DocID in os.listdir(filepath+'/'+TSENO):
                if DocID == ".DS" or DocID == ".DS_Store":
                    continue
                csv_parser = CSVParser(filepath+'/'+TSENO+'/'+DocID+'/'+DocID+'.zip')
                
                df = csv_parser.get_df()
                #open the info text file
                with open(filepath+'/'+TSENO+'/'+DocID+'/'+DocID+'_info.txt', 'r') as file:
                    content = file.read()
                data = json.loads(content)
                sec_code = data.get('secCode')
                sec_code = sec_code[:4]
                #get the submission date but remove the time
                submission_date = data.get('submitDateTime')
                submission_date = submission_date.split(' ')[0]
                #get the company name
                company_name = data.get('filerName')
                #get the document title
                document_title = data.get('docDescription')
                #add the columns to the dataframe
                df['TSE:'] = TSENO
                df['Submission Date'] = submission_date
                df['Company Name'] = company_name
                df['Document Title'] = document_title
                sumdf = sumdf._append(df)
                del csv_parser
        #write to csv
        sumdf.to_csv('sumdf.csv')
if __name__ == '__main__':
    builder = DataFrameBuilder()
    builder.build_dataframes('files')


                