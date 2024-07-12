import pandas as pd
import os
from csv_parser import CSVParser
import json
from datetime import datetime
import sys
import time
from work_history_processor import WorkHistoryProcessor
import re
from q_parser import QParser
from datetime import datetime
from dateutil.relativedelta import relativedelta
import re





###########################
### this one will undoubtably be made obsolete soon: reasoning is that we need to be building this csv document by document
###########################
class DataFrameBuilder:
    def __init__(self):
        
        self.sumdf = pd.DataFrame(columns=['TSE:', 'Company Name', 'Document Title', 'Submission Date', 'Period End', 'type','error'])
        self.sumdf['error'] = ""
        self.failed = pd.DataFrame(columns=['TSE:', 'Company Name', 'Document Title', 'Submission Date', 'Period End', 'type','error'])
        self.failed['error'] = ""
    
    def build_dataframes(self,filepath):
        
        teisei= []
        TEST_counter =0
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

                #open the info text file
                with open(filepath+'/'+TSENO+'/'+DocID+'/'+DocID+'.json', 'r') as file:
                    data = json.load(file)
                sec_code = data.get('secCode')
                sec_code = sec_code[:4]
                #get the submission date but remove the time
                submission_date = data.get('submitDateTime')
                document_code = data.get('docID')
                
                #get the company name
                company_name = data.get('filerName')
                #get the document title
                document_title = data.get('docDescription')
                period_end = data.get('periodEnd')
                print(period_end)
                if period_end == None:

                    period_end = document_title[document_title.find("(")+1:document_title.find(")")]
                    period_end = period_end.split('－')
                    period_end = period_end[1]  

                    print(f'period end is {period_end}')
                del data



                
                if "有価証券報告書" not in document_title:
                    continue
                

                #add the columns to the dataframe
                csv_parser = CSVParser(filepath+'/'+TSENO+'/'+DocID+'/'+DocID+'.zip')

                csv_parser.director_parse()
                df = csv_parser.get_directordf()
                
                
                    
                if df.empty:
                    print(f"Empty dataframe for {document_title} with Company Name: {company_name} and document ID: {DocID}")
                    teisei.append(DocID)
                    print(df)
                    
                
                df['TSE:'] = TSENO
                df['Submission Date'] = submission_date
                df['Company Name'] = company_name
                df['Document Title'] = document_title
                df['document code'] = document_code
                df['Period End'] = period_end

                df['type'] = 'annual'    
                print(document_code)
                TEST_counter+=1

                # logic that handles officers in 
                

                ofdf= csv_parser.get_officerdf()
                if not ofdf.empty:
                    ofdf['TSE:'] = TSENO
                    ofdf['Submission Date'] = submission_date
                    ofdf['Company Name'] = company_name
                    ofdf['Document Title'] = document_title
                    ofdf['document code'] = document_code
                    ofdf['Period End'] = period_end
                    ofdf['type'] = 'annual'
                    self.sumdf = pd.concat([self.sumdf, ofdf], ignore_index=True)

                if df.empty and ofdf.empty:
                    print(f"Empty dataframe for {document_title} with Company Name: {company_name} and document ID: {DocID}")
                    teisei.append(DocID)

                    continue
                




                # if TEST_counter == 5:
                #     self.to_csv('test.csv')
                #     sys.exit()


                if csv_parser.different_format:
                    misformat.add(DocID)
                    self.failed = pd.concat([self.failed, df], ignore_index=True)
                    print(f"Document {DocID} contains mismatched columns")
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
        print(self.sumdf.columns)
        self.sumdf = self.sumdf[['TSE:', 'Company Name','Name','DOB','Job Title','Work History', 'Document Title', 'Submission Date',
       'Period End', 'type', 'error','External Info',
     'Footnotes',  'which table?',
       'Company Footnotes', 'document code']]
    
    def add_to_dataframe(self, tseno, docid):
        csvpath = 'files/'+tseno+'/'+docid+'/'+docid+'.zip'
        parse = CSVParser(csvpath)
    
    
    def read_csv1   (self, filepath):
        self.sumdf = pd.read_csv(filepath)
        self.sumdf["error"] = ""   
    def rearrange_columns(self,column_name):
        columns = self.sumdf.columns
        columns = list(columns)
        #remove column name from columns
        columns.remove(column_name)
        #reorganise the columns
        columns.insert(0,column_name)
            #reorganise the dataframe


    def tag_external_directors(self):
        #go through the CSV and for each row, check the value of the column company footnotes for the substring '社外取締役' and return the TSE value for those that do not
        #add a boolean column to the table called 'external?' set to false
        self.sumdf['External'] = False
        ootpoot  = ""
        for index, row in self.sumdf.iterrows():
            if row['which table?'] == 'officer':
                continue
            if '社外' in row['Job Title']:
                self.sumdf.at[index, 'External'] = True
            
            # check if row['external'] is not nan
            elif not pd.isna(row['External Info']):
                external_text = row['External Info']
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
                            self.sumdf.at[index, 'External'] = True
                            break

                id = row['TSE:'] 
                external_text = ' '.join(external_text)
                job= row['Job Title']
                name = row['Name']
                ootpoot += f"{id}{name}, {job} at {id} was not internal: {external_text}   \n"

        with open ('external_directors.txt', 'w') as file:
            file.write(ootpoot)
        self.sumdf= self.sumdf[['TSE:', 'Company Name', 'Name', 'DOB', 'Job Title', 'Work History','External',
       'Document Title', 'Submission Date', 'Period End', 'type', 'error',
        'Footnotes', 'Company Footnotes',
       'document code']]
    def scan_for_external_directors(self):
        director_dict = {}
        for index, row in self.sumdf.iterrows():
            if row['External'] == True:
                director_dict[row['TSE:']] = True
            else:
                if director_dict.get(row['TSE:']) == True:
                    continue
                else:
                    director_dict[row['TSE:']] = False
        return director_dict
    def drop_auditors(self):
        self.sumdf = self.sumdf[self.sumdf['Job Title'] != '監査役']
        self.sumdf = self.sumdf[self.sumdf['Job Title'] != '監査役員']
        self.sumdf = self.sumdf[self.sumdf['Job Title'] != '常勤監査役']
        self.sumdf = self.sumdf[self.sumdf['Job Title'] != '常勤監査役員']
        self.sumdf = self.sumdf[self.sumdf['Job Title'] != '監査役等']
    def to_csv(self, filepath=''):
        self.sumdf.to_csv(filepath, index=False)
    def to_csv_failures(self, filepath=''):
        print(self.failed)
        self.failed.to_csv(filepath, index=False)

    def get_sumdf(self):
        return self.sumdf
    def dataframe_rearranger(self):
         # TSE:,Company Name,Document Title,Submission Date,period end,type,Name,Job Title,Work History,Footnotes,external,DOB,which table?,Company Footnotes,document code,external?,year joined,current job,last job,error

        self.sumdf = self.sumdf[["TSE:","Company Name","Name","DOB","Job Title","External","year joined","current job","last job","Work History","Footnotes","External Info","which table?","Company Footnotes","error","Document Title","Submission Date","period end","type","document code"
]]
    def ep_parser(self):
        id = []
        #add empty current job column to self.sumd
        self.sumdf['current job'] = ""
        
       
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
                        if pd.isna(self.sumdf.loc[index, 'error']):
                            self.sumdf.loc[index, 'error'] = ''
                        
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
                    if pd.isna(self.sumdf.loc[index, 'error']):
                        self.sumdf.loc[index, 'error'] = ''

                    self.sumdf.loc[index, 'error'] += 'Error no current job found: no most recent job, '
                    # print('exception line 271',row['TSE:'], row['Name'])
                    

                

            self.sumdf.loc[index, 'current job'] = current_job   

    def previous_jobs(self):
        self.sumdf['Last Internal Job'] = ""
        self.sumdf['Last External Job'] = ""
        self.sumdf['Year Joined'] = ""
        self.sumdf['Concurrent Roles'] = ""
        self.sumdf['Recent Job Change'] = ""
        #for each of the non external directors, go through their

        for index, row in self.sumdf.iterrows():
            counter = 0
            print(row['WH Error'])
            if row['WH Error']!=1:
                continue
            if row ['External'] == True:
                text = row ['Work History'].split(';')

                output = ''
    
                for i, t in enumerate(text):

                    if '現役'in t or '現任' in t or '兼職' in t or '兼'in t or '現' in t[-4:]or '現在に至る'in t or i == len(text):
                        

                        
                        t.replace('現在に至る', '')
                        t.replace('現役', '')
                        t.replace('重要な兼職', '')
                        t.replace('兼職', '')
                        t.replace('兼', '')
                        t.replace('現任', '')
                        t.replace('現', '')
                        t.replace('（）', '')
                        t.replace('()', '')

                        output += t
                        output+= ', '
                if output == '':
                    if pd.isna(self.sumdf.loc[index, 'error']):
                        self.sumdf.loc[index, 'error'] = ''

                    self.sumdf.loc[index, 'error'] += 'Error no current job found: no most recent job, '
                    # print('exception line 271',row['TSE:'], row['Name'])
                self.sumdf.loc[index, 'Concurrent Roles'] = output
            if row['External'] == False:
                #first figure out the most recent internal job, then figure out the most recent external job
                text = row['Work History'].split(';')    
                internal = ''
                external = ''
                concurrent = ''
                year_joined = ''
                last_job_change = ''
                joined = False
                for i, t in enumerate(text):
                    if i == len(text) - 1:
                        if internal != '':
                           internal += ', '
                        internal +=text[i-1]
                        last_job_change = t.split('::')[0]
                        if last_job_change[4]!='/':
                            print(last_job_change)
                        self.sumdf.loc[index, 'Recent Job Change'] = last_job_change
                        break

                    
                    if '当社入社' in t:
                        year_joined = t.split('::')[0]
                        counter += 1
                        #break off the first section of the text that includes the date
                        external = text[i-1]
                        joined = True

                    
                    if '現役'in t or '現任' in t or '兼職' in t or '兼'in t or '現' in t[-4:]or '現在に至る'in t or i == len(text):
                        

                        t.replace('現在に至る', '')
                        t.replace('現役', '')
                        t.replace('重要な兼職', '')
                        t.replace('兼職', '')
                        t.replace('兼', '')
                        t.replace('現任', '')
                        t.replace('現', '')
                        t.replace('（）', '')
                        t.replace('()', '')
                        # if joined:
                        #     if internal != '':
                        #         internal += ', '
                        #     internal += t
                        # else:
                        if concurrent != '':
                            concurrent += ', '
                        concurrent +=t
                    if internal == '':
                        print('error: no', row['TSE:'], row['Name'])
                self.sumdf.loc[index, 'Last Internal Job'] = internal
                self.sumdf.loc[index, 'Last External Job'] = external
                self.sumdf.loc[index, 'Year Joined'] = year_joined
                self.sumdf.loc[index, 'Concurrent Roles'] = concurrent
                self.sumdf.loc[index, 'Recent Job Change'] = last_job_change


        print('DONE')
    def sort_officers(self):
        for id in self.sumdf['TSE:'].unique():
            df = self.sumdf[self.sumdf['TSE:'] == id].copy()
            df['name_normalised'] = df['Name'].str.replace(' ', '').replace('　', '')

            for name in df['name_normalised'].unique():
                df2 = df[df['name_normalised'] == name]
                if len(df2) > 1:
                    # Filter rows based on 'which table?' value
                    row1 = df2[df2['which table?'] == 'officer'].iloc[0]
                    row2 = df2[df2['which table?'] == 'director'].iloc[0]
                    row1Jtitle = row1['Job Title'].replace(' ', '').replace('　', '')
                    row2Jtitle = row2['Job Title'].replace(' ', '').replace('　', '')

                    if row1Jtitle in row2Jtitle or row2Jtitle in row1Jtitle:
                        # Append row1 footnotes to row2 footnotes in sumdf and delete row1
                        self.sumdf.at[row2.name, 'Footnotes'] += ' ' + row1['Footnotes']
                        self.sumdf = self.sumdf.drop(row1.name)
                        print('row dropped')
                    else:
                        # Append row1 title and footnotes to row2 and delete row1
                        self.sumdf.at[row2.name, 'Job Title'] += ' / ' + row1['Job Title']
                        self.sumdf.at[row2.name, 'Footnotes'] += ' ' + row1['Footnotes']
                        self.sumdf = self.sumdf.drop(row1.name)
                        print('row dropped')

            # Update the instance variable with the new dataframe

  
    def work_history_processer(self):
        issues = 0
        self.sumdf['WH Error'] = 0
        processor = WorkHistoryProcessor()
        for index, row in self.sumdf.iterrows():
            problem = 0
            text = row['Work History']
            processor.process_work_history(text)
            processor.split_text()
            for line in processor.get_text():
                problem_cur = self.verifier(line)
                
                if problem_cur>problem:

                    problem = problem_cur
                    
            self.sumdf.loc[index, 'WH Error'] = problem
            self.sumdf.loc[index, 'Work History'] = ';'.join(processor.get_text())
        print(issues)   
    def period_fix(self):
        #check that date formatting is yyyy-mm-dd
        for index, row in self.sumdf.iterrows():
            date = row['Period End']

            try:
                parsed_date = datetime.strptime(date, '%Y-%m-%d')
            except ValueError:
                try:
                    parsed_date = datetime.strptime(date, '%Y/%m/%d')
                except ValueError:
                    # Handle incorrect date format if necessary
                    print(f"Invalid date format: {date} for {row['Company Name']} {row['Document Title']}")
                    continue
            # If successful, format it back to yyyy-mm-dd to ensure consistent formatting
            formatted_date = parsed_date.strftime('%Y-%m-%d')
            self.sumdf.at[index, 'period end'] = formatted_date
            
            

    def verifier(self,text):

        
        #screen for problems
        #if 年 appears after :: then it is a problem
        if re.search(r':.*年', text):
            #ignore if next kanji is 金
            if re.search(r':.*年金', text):
                pass
                
            elif re.search(r':.*年齢', text):
                pass
            elif re.search(r':.*(?<!\d)年', text):
                pass
            elif re.search(r':.*年\s*退', text):
                pass
            else:
                print("Problem: 年 appears after ::", text)
                
                return 2
        #if 月 appears after :: then it is a problem
        if re.search(r':.*月', text):
            if re.search(r':.*月島', text):
                pass
            elif re.search(r':.*(?<!\d)月', text):
                pass
            else:
                print("Problem: 月 appears after ::", text)
                return 2
        #if there is no content after :: 
        if re.search(r':$', text):
            print("Problem: No content after ::", text)
            return 3
        return 1
    def create_quarterly_reports(self):
        QDF = pd.DataFrame()
        
        period_end = {}
        for index, row in self.sumdf.iterrows():
            if row['type'] == 'annual':
                if row["TSE:"]not in period_end:
                    period_end[row["TSE:"]] = row['Period End']
        for TSE in period_end:
            #go to that documents folder in the quarterly reports
            filedir = os.listdir('files/'+str(TSE))
            #reverse order of filedir
            filedir = sorted(filedir)
            period_end_date = period_end[TSE]
            # note the day is formatted in the format yyyy-mm-dd
            #use datetime to parse the date
            try:
                period_end_date = datetime.strptime(period_end_date, '%Y-%m-%d')
            except:
                period_end_date = datetime.strptime(period_end_date, '%Y/%m/%d')
            
            
            for file in filedir:
                if file == ".DS" or file == ".DS_Store":
                    continue
            
                #read the json file
                jsonfile = 'files/'+str(TSE)+'/'+file+'/'+file+'.json'

                with open(jsonfile, 'r') as file1:
                    data = json.load(file1)
                #get the period end date and add a day
                
                try:
                    file_date=  datetime.strptime(data['periodStart'], '%Y-%m-%d')
                except:
                    try:
                        file_date = re.findall(r'\d{4}/\d{2}/\d{2}', data['docDescription'])[0]
                        
                    except:
                        print(data['docDescription'])
                        continue
                    
                    file_date = datetime.strptime(file_date, '%Y/%m/%d')
                
                if TSE ==1980 and'四半期報告書' in data['docDescription'] :
                    print(file_date)

                    
                
                # file_date = file_date.strftime('%Y-%m-%d')
                # period_end_date = (period_end_date).strftime('%Y-%m-%d')
                if file_date > period_end_date:

                    if '四半期報告書' in data['docDescription']:
                        print(TSE)
                        csv_filepath = 'files/'+str(TSE)+'/'+str(file)+'/'+str(file)+'.zip'
                        qparser = QParser(csv_filepath)
                        qparser.parse()
                



