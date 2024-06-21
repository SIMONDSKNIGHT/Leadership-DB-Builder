import pandas as pd
import os
from csv_parser import CSVParser
import json

class DataFrameBuilder:
    def __init__(self):
        self.sumdf = pd.DataFrame()
    
    def build_dataframes(self,filepath):
        
        teisei= []
        
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
                
                #get the company name
                company_name = data.get('filerName')
                #get the document title
                document_title = data.get('docDescription')
                #add the columns to the dataframe
                
                    
                if df.empty:
                    print(f"Empty dataframe for {document_title} with Company Name: {company_name} and document ID: {DocID}")
                    teisei.append(DocID)
                df['TSE:'] = TSENO
                df['Submission Date'] = submission_date
                df['Company Name'] = company_name
                df['Document Title'] = document_title

                self.sumdf = self.sumdf._append(df)
                del csv_parser
        #write to csv
        #reorganise the csv so that the order is [TSE:, Company Name, Name, Job Title, DOB, Work History, Footnotes, Company Footnotes, Document Title, Submission Date]
        self.sumdf = self.sumdf[['TSE:', 'Company Name', 'Name', 'Job Title', 'DOB', 'Work History', 'Footnotes', 'Company Footnotes', 'Document Title', 'Submission Date']]
        print(teisei)
        print(len(teisei))

    def remove_outdated_info(self):
        #get all info about 1 company, find the most recent submission date, remove all rows for company that are not the most recent submission date from sumdf
        #iterate over unique TSENOs

        for TSENO in self.sumdf['TSE:'].unique():

            #most of this is depricated
            #get all rows for that TSENO
            df = self.sumdf[self.sumdf['TSE:'] == TSENO]
            #get the most recent submission date (note the date is in the format )
            most_recent_date = df['Submission Date'].max()
            #check the document name for these documents, and if they do not contain the character "-", find the second most recent date
            m_recent_date = most_recent_date
            bad_docu_name = df[df['Submission Date']==m_recent_date]['Document Title'][0]
            while True:
                #if "=" in documents with that submission date

                
                if "－" in df[df['Submission Date']==m_recent_date]['Document Title'][0]:
                    
                
                    break
                else:
                    
                    print('failure identified at ', m_recent_date, TSENO)
                    m_recent_date = df[df['Submission Date'] < m_recent_date]['Submission Date'].max()

                    print(m_recent_date)
                if df[df['Submission Date']==m_recent_date].empty:
                    print(f'No document found for {TSENO}')
                    break
            if df[df['Submission Date']==m_recent_date].empty:
                print(f'No document found for {TSENO}, document name at {bad_docu_name}')
                continue
                
            #remove all rows for that TSENO that are not the most recent submission date
            self.sumdf = self.sumdf[(self.sumdf['TSE:'] != TSENO) | (self.sumdf['Submission Date'] == most_recent_date)]
            #change the docuemt name for that TSENO to the document name gained
            if m_recent_date != most_recent_date:
                print(most_recent_date, m_recent_date, TSENO)
                #find document name of the most recent submission date
                document_suffix = df[df['Submission Date']==m_recent_date]['Document Title'][0].split("－")[1:]
                #add the document suffix to all rows where the submission date is the most recent date
                new_document_name = df[df['Submission Date']==most_recent_date]['Document Title'].apply(lambda x: x +" －"+ document_suffix)
                self.sumdf.loc[self.sumdf['TSE:'] == TSENO, 'Document Title'] = new_document_name               

    def to_csv(self, filepath=''):
        self.sumdf.to_csv(filepath, index=False)
if __name__ == '__main__':

    builder = DataFrameBuilder()
    builder.build_dataframes('files')
    builder.remove_outdated_info()
    builder.to_csv('sumdf.csv')
    print("Complete")


                