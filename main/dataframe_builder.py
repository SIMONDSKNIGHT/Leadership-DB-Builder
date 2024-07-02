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
                #split at every 。 to get the sentences
                external_text = external_text.split('。')
                #remove all spaces
                external_text = [text.replace(' ', '') for text in external_text]
                external_text = [text.replace('　', '') for text in external_text]
                for text in external_text:
                    if '社外取締役であ'or '「社外取締役」であ'or'社外取締役です' or '「社外取締役」です' in text:
                        name = row['Name']
                        name = name.replace(' ', '')
                        name = name.replace('　', '') 
                        if name in text:
                            self.sumdf.at[index, 'external?'] = True
                            break

                id = row['TSE:'] 
                external_text = ' '.join(external_text)
                job= row['Job Title']
                name = row['Name']
                ootpoot += f"{id}{name}, {job} at {id} was not internal: {external_text}   \n"

        with open ('external_directors.txt', 'w') as file:
            file.write(ootpoot)
    def scan_for_external_directors(self):
        director_dict = {}
        for index, row in self.sumdf.iterrows():
            if row['external?'] == True:
                director_dict[row['TSE:']] = True
            else:
                if director_dict.get(row['TSE:']) == True:
                    continue
                else:
                    director_dict[row['TSE:']] = False
        return director_dict
    def to_csv(self, filepath=''):
        self.sumdf.to_csv(filepath, index=False)
    def to_csv_failures(self, filepath=''):
        self.failed.to_csv(filepath, index=False)
    def when_joining(self):
        #for each of the non external directors, go through their
        pass
    def last_position(self):
        #for person, go through their work history and find the last position they held
        for index, row in self.sumdf.iterrows():
            if row['external?'] == False:
                work_history = row['Work History']
                work_history = work_history.split('。')
                work_history = [text.replace(' ', '') for text in work_history]
                work_history = [text.replace('　', '') for text in work_history]
                last_position = work_history[-1]

    def dataframe_rearranger(self):
         # TSE:,Name,Work History,Footnotes,Company Footnotes,external,Document Title,Company Name,Submission Date,Job Title,DOB,external?
        self.sumdf = self.sumdf[['year joined','current job','TSE:','Name','Work History', 'Company Name',  'Job Title', 'DOB',  'Footnotes', 'Company Footnotes','external' ,'Document Title', 'Submission Date', 'external?','error']]
    def lastjob(self):
        id = []
        #add empty current job column to self.sumd
        self.sumdf['current job'] = ""
        self.sumdf['error'] = ""
       
        for index, row in self.sumdf.iterrows():
            if row['TSE:'] not in id:
                id.append(row['TSE:'])
            text = row['Work History']
            #find index of every instance of 年
            index_nen = [pos for pos, char in enumerate(text) if char == '年']
            #split the text by the position of the 年
            text = [text[i-4:j-4] for i, j in zip(index_nen, index_nen[1:]+[len(text)+4])]
            #remove space from all items
            text = [t.replace(' ', '') for t in text]
            text = [t.replace('　', '') for t in text]
            current_job = ""
            
            for t in text:
                
                if '現' in t:

                    t=t.split('月')
                    
                    if len(t)>1:
                        t=t[1]
                    else:
                        t=t[0]
                        
                        self.sumdf.loc[index, 'error'] += 'Error job formatting could be incorrect, '
                    t = str(t)

                    

                    t = t.replace('現役', '')
                    t = t.replace('現任', '')
                    t = t.replace('現', '')
                    t = t.replace('（）', '')
                    t = t.replace('()', '')

                    current_job += t
                    current_job += ', '
            
                    
                #add current jobs to column in df
            if current_job == '':
                try:
                    t = text[-1]
                    t=t.split('月')
                    
                    if len(t)>1:
                        t=t[1]
                    else:
                        t=t[0]
                        print(row['TSE:'], row['Name'])
                    t = str(t)
                    current_job += t
                except:
                    self.sumdf.loc[index, 'error'] += 'Error no current job found: no most recent job, '
                    print('exception',row['TSE:'], row['Name'])
                    

                

            self.sumdf.loc[index, 'current job'] = current_job   
    def joined_company_when(self):
        #for each of the non external directors, go through their
        self.sumdf['year joined'] = ""
        self.sumdf['error'] = ""
        for index, row in self.sumdf.iterrows():
            counter = 0
            if row['external?'] == False:
                text = row['Work History']
                index_nen = [pos for pos, char in enumerate(text) if char == '年']
                #split the text by the position of the 年
                text = [text[i-4:j-4] for i, j in zip(index_nen, index_nen[1:]+[len(text)+4])]
                #remove space from all items
                text = [t.replace(' ', '') for t in text]
                text = [t.replace('　', '') for t in text]      
                output = ''
                for t in text:
                    if '当社入社' in t:
                        counter += 1
                        #break off the first section of the text that includes the date
                        if '月' in t:
                            
                            t= t.split('月')
                            
                            t= t[0]+'月'
                        else:
                            self.sumdf.loc[index, 'error'] += 'Error job  year formatting could be incorrect, '
                        output = str(t)
                    if counter > 1:
                        self.sumdf.loc[index, 'error'] += 'Error multiple join dates found, '
                        print('exception',row['TSE:'], row['Name'])
                print(output)
                if output == '':
                    self.sumdf.loc[index, 'error'] += 'Error no year joined found, '
                    print('exception',row['TSE:'], row['Name'])
                self.sumdf.loc[index, 'year joined'] = output
    def get_latest_employee(self):
        #go throught the arraay and return the most recent employee
        largest_date = '0'
        index_list = [] 
        for index, row in self.sumdf.iterrows():
            date = row['year joined']
            if str(date) > str(largest_date):
                try:
                    number = int(date[:4])
                    largest_date = date
                    index_list = [index]
                except:
                    print('error', date, row['TSE:'], row['Name'])
            elif date == largest_date:
                index_list.append(index)
        print(largest_date)
        for i in index_list:
            print(self.sumdf.loc[i, 'TSE:'], self.sumdf.loc[i, 'Name'])
           
            
                
                        
                        
                        

    
if __name__ == '__main__':

    builder = DataFrameBuilder()
    # builder.build_dataframes('files')
    # builder.to_csv('sumdf.csv')
    # builder.to_csv_failures('failed.csv')
    builder.read_csv1('sumdf.csv')
    # builder.lastjob()
    builder.get_latest_employee()
    builder.to_csv('sumdf.csv')


    # builder.dataframe_rearranger('sumdf.csv')
    # builder.tag_external_directors()
    # builder.to_csv('sumdfe.csv')
    # print("Complete")
    # dicto = builder.scan_for_external_directors()
    # for key in dicto:
    #     if dicto[key] == False:
    #         print(key)


                