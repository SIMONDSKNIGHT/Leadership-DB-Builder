import pandas as pd
import os
from csv_parser import CSVParser
import json

class DataFrameBuilder:
    def __init__(self):
        
        self.sumdf = pd.DataFrame(columns=['TSE:', 'Company Name', 'Document Title', 'Submission Date', 'period end', 'type'])
        self.failed = pd.DataFrame(columns=['TSE:', 'Company Name', 'Document Title', 'Submission Date', 'period end', 'type'])
    
    def build_dataframes(self,filepath,quarterly=False):
        
        teisei= []
        latest ={}
        misformat = set()
        #sort list of directories
        for TSENO in os.listdir(filepath):
            if TSENO == ".DS" or TSENO == ".DS_Store":
                continue
            sorted_docIDs = sorted(os.listdir(filepath+'/'+TSENO))
            sorted_docIDs.reverse()
            for DocID in sorted_docIDs:
                if DocID == ".DS" or DocID == ".DS_Store":
                    continue
                csv_parser = CSVParser(filepath+'/'+TSENO+'/'+DocID+'/'+DocID+'.zip')
                print('csv made')
                
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
                period_end = data.get('periodEnd')

                #add the columns to the dataframe
                
                    
                if df.empty:
                    print(f"Empty dataframe for {document_title} with Company Name: {company_name} and document ID: {DocID}")
                    teisei.append(DocID)
                    continue
                df['TSE:'] = TSENO
                df['Submission Date'] = submission_date
                df['Company Name'] = company_name
                df['Document Title'] = document_title
                df['period end'] = period_end
                if quarterly == True:
                    df['type'] = 'quarterly'
                else:
                    df['type'] = 'annual'    
                if csv_parser.different_format:
                    misformat.add(DocID)
                    self.failed = pd.concat([self.failed, df], ignore_index=True)
                else:
                    self.sumdf = pd.concat([self.sumdf, df], ignore_index=True)
                    break

                        

                del csv_parser
        #write to csv
        #reorganise the csv so that the order is [TSE:, Company Name, Name, Job Title, DOB, Work History, Footnotes, Company Footnotes, Document Title, Submission Date]

        # self.sumdf = self.sumdf[['TSE:', 'Company Name', 'Name', 'Job Title', 'DOB', 'Work History', 'Footnotes', 'Company Footnotes','external' ,'Document Title', 'Submission Date']]
        print(teisei)
        print(len(teisei))
        print(misformat)
    def read_csv1   (self, filepath):
        self.sumdf = pd.read_csv(filepath)


    def tag_external_directors(self):
        #go through the CSV and for each row, check the value of the column company footnotes for the substring '社外取締役' and return the TSE value for those that do not
        #add a boolean column to the table called 'external?' set to false
        self.sumdf['external?'] = False
        ootpoot  = ""
        for index, row in self.sumdf.iterrows():
            
            if '社外' in row['Job Title']:
                self.sumdf.at[index, 'external?'] = True
            
            # check if row['external'] is not nan
            elif not pd.isna(row['external']):
                external_text = row['external']
                id = row['TSE:'] 

                ootpoot += f"{id} {external_text} \n"

        with open ('external_directors.txt', 'w') as file:
            file.write(ootpoot)

    def to_csv(self, filepath=''):
        self.sumdf.to_csv(filepath, index=False)
    def to_csv_failures(self, filepath=''):
        self.failed.to_csv(filepath, index=False)
if __name__ == '__main__':

    builder = DataFrameBuilder()
    # builder.build_dataframes('files')
    # builder.to_csv('sumdf.csv')
    # builder.to_csv_failures('failed.csv')
    builder.read_csv1('sumdf.csv')
    builder.tag_external_directors()
    print("Complete")


                