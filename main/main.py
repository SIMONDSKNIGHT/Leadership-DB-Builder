import argparse
import excel_reader
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
import rest_api
import json
from tqdm import tqdm
import sys
import dataframe_builder
import pandas as pd
import csv_querier
import time
import subprocess

dl_file_path = 'files'

# format for the commandline command is python main.py --forceupdate --default --test
# --forceupdate updates the list of reports
# --default uses default settings where it doesn't require you to type stuff out
# --test runs the test logic instead of the main program
def main():
    data_directory = 'data/'
    try:
        with open('main/api_key.txt', 'r') as file:
            api_key = file.read()
    except FileNotFoundError:
        print('api_key.txt not found')
        api_key = input('Input API key and press Enter to continue')
    parser = argparse.ArgumentParser(description='Determine behavior based on an input number')
    parser.add_argument('--forceupdate', action='store_true', help='Update the list of reports')
    parser.add_argument('--default', action='store_true', help='Use default settings')
    parser.add_argument('--test', action='store_true', help='Run test instead of main program')
    args=parser.parse_args()
    #identify if certain files exist here


    #####
    if not args.default:
        excel_filepath= input('address of excel file: DEPRICATED, REFER TO ids.txt')
        rest_server_url = input("address of rest server(Hint it's https://api.edinet-fsa.go.jp/api/v2/documents. try --default): ")
    else:
        excel_filepath = "/Users/dagafed/Library/CloudStorage/OneDrive-Personal/Documents/top 500 by liquidity.xlsx"
        rest_server_url = 'https://api.edinet-fsa.go.jp/api/v2/documents'
    #####
    #### the excelparser is currently a 
    # excel_parser = excel_reader.ExcelReader()
    # try:
    #     excel_parser.read(excel_filepath)
    # except:
    #     print('failure')
    #     Exception('Error setting rest server')
    #     #kill process
    #     sys.exit()
    # ids = []
    # for i in excel_parser.get_file_ids():
    #     ids.append(i[0][4:])
        
    # del excel_parser
    # with open('ids.txt','w') as f:
    #     for i in ids:
    #         f.write(i+'\n')
    with open ('ids.txt','r') as f:
        ids = f.readlines()
    #make ids be string and int versions #if possible
    
    newids = []
    for i in ids:
        inti = i[:4]
        newids.append(inti)
        try:
            inti = int(i)
            newids.append(inti)
        except:
            pass
    ids += newids
    


    if args.test:

        #TEST CODE THAT JUMPS DOWN TO THE BELOW TEST CODE SECTION skipped for, would you believe it, testing;
        test_logic(ids)
        print("forgot to disable test logic, bonehead.")
        exit()
    #write ids to file



    today = datetime.now().strftime("%Y/%m/%d")
    backdate = datetime.now() - relativedelta(years=1)
    backdate = backdate.strftime("%Y/%m/%d")
    if args.forceupdate:
        pass
    else:
        #todays date in "%Y/%m/%d"
        
        download_directory = data_directory + 'json/'
        if not os.path.exists(download_directory):
            os.makedirs(download_directory)
        for document in os.listdir(download_directory):
            document= document.split('.')[0]
            document = document.replace('-','/')

            #parse the name which is in format yyyy-mm-dd and compare to the year ago date
            #if the document is older than a year, delete it


            if document == today:
                #make backdate tomorrow
                backdate = datetime.now() + relativedelta(days=1)
                backdate = backdate.strftime("%Y/%m/%d")
                break
            if document > backdate:
                backdate = document
            break
        
    rest_server_interface = rest_api.RestApiInterface(rest_server_url, api_key,ids,backdate,today)
    rest_server_interface.save_reports()

    rest_server_interface.write_reports()
    rest_server_interface.read_reports()
    reports = rest_server_interface.get_reports()
    # if main.csv does not exist but there is a file in data then read in the old data, append new data and write to 
    print(len(reports))
    for jsonobject in tqdm(reports):

        try:
            tseNO = jsonobject['secCode'][:4]
        except:
            pass
        docID = jsonobject['docID']
        #check that that tseno folder exists
        if not os.path.exists(f'files/{tseNO}'):
            os.makedirs(f'files/{tseNO}')
        #check if the document folder exist
        if not os.path.exists(f'files/{tseNO}/{docID}'):
            os.makedirs(f'files/{tseNO}/{docID}')
        #print tseno if directory is empty
        if len(os.listdir(f'files/{tseNO}/{docID}')) == 0:
            print(f"Document {docID} for {tseNO} does not exist.  Downloading.")
        else:
            # behaviour only needed for initial install. TODO: remove and make the file delete itself
            print(f"Document {docID} for {tseNO} already exists. Skipping download.")
            continue
        #write the jsonobject to the folder

        with open(f'files/{tseNO}/{docID}/{docID}.json','w') as f:
            json.dump(jsonobject,f,indent=4,ensure_ascii=False)

    ### call this one the loop, this is the yearly loop
    
    dataframe_builder_instance = dataframe_builder.DataFrameBuilder()




    # ###this is the code for the downloading of the CSV's

    # #for failed parsing, consider downloading the information from the PDF's
    if not os.path.exists(dl_file_path):
        os.makedirs(dl_file_path)
    files_sorted = sorted(os.listdir(dl_file_path))
    for tseno in files_sorted:
        success = False
        
        if tseno == '.DS_Store':
            continue
        TSE_path = os.path.join(dl_file_path, tseno)
        sorted_docIDS=sorted(os.listdir(TSE_path))
        sorted_docIDS = reversed(sorted_docIDS)
        
        for docid in sorted_docIDS :
            if docid == ".DS" or docid == ".DS_Store":
                continue
            #open the info text file
            with open(dl_file_path+'/'+tseno+'/'+docid+'/'+docid+'.json', 'r') as file:
                data = json.load(file)
            document_title = data.get('docDescription')
        
            del data
            if "有価証券報告書" not in document_title:
                continue

            
            #if document does not exist download it
            check_file = dl_file_path+'/'+tseno+'/'+docid+'/'+docid+'.zip'
            if not os.path.exists(check_file):
                filename= rest_server_interface.download_report(docid,tseno,5)
            success = dataframe_builder_instance.add_to_dataframe(tseno,docid)
            
            
            

            #delete the dowloaded document in the files folder whos filepath is called filename
            
            os.remove(filename) 


            if success:
                print(f"document {docid} downloaded and added to df")
                success = True
                break
        
    dataframe_builder_instance.to_csv("TEST_CSV_UTIL_1.csv")
    dataframe_builder_instance.read_csv1("TEST_CSV_UTIL_1.csv")
    dataframe_builder_instance.sort_officers()
    dataframe_builder_instance.period_fix()
    

    # dataframe_builder_instance.to_csv("TEST_CSV_UTIL_2.csv")
    

    







    # exit()


    ###LOGIC BELOW IS FOR PROCESSING QUARTERLY REPORTS, CURRENTLY NON FUNCTIONAL, HANDLING DATES



    files_sorted = sorted(os.listdir(dl_file_path))

    for tseno in files_sorted:
        
        success = False
        
        if tseno == '.DS_Store' or tseno == '.DS':
            continue
        TSE_path = os.path.join(dl_file_path, tseno)
        sorted_docIDS=sorted(os.listdir(TSE_path))
        #GET THE DATE OF THE TSE'S REPORT IN THE DATAFRAME BUILDERS DATAFRAME
        date = dataframe_builder_instance.get_latest_date(tseno)
        
        
            
        if date == 'NA':
            print(f"no date found for {tseno}")
            continue   
        
        date = datetime.strptime(date, '%Y-%m-%d')
        
        
        
        
        for docid in sorted_docIDS :
            if docid == ".DS" or docid == ".DS_Store":
                continue
            #open the info text file
            with open(dl_file_path+'/'+tseno+'/'+docid+'/'+docid+'.json', 'r') as file:
                data = json.load(file)
            document_title = data.get('docDescription')
            doc_date = data.get('PeriodEnd')
            if "四半期報告書" not in document_title:
                del data
                continue
            
            
            
            #check the format of docdate and convert to datetime
            try:
                parsed_date = datetime.strptime(doc_date, '%Y-%m-%d')
            except:
                try:
                    parsed_date = datetime.strptime(doc_date, '%Y/%m/%d')
                except:
                    # Handle incorrect date format if necessary
                    try:
                        if doc_date == None:

                            doc_date = document_title[document_title.find("(")+1:document_title.find(")")]
                            doc_date = doc_date.split('－')
                            doc_date = doc_date[1]  
                    except:
                        parsed_date = dataframe_builder_instance.date_rounder(data['submitDateTime'])
                        continue
                    try:
                        parsed_date = datetime.strptime(doc_date, '%Y-%m-%d')
                    except:
                        try:
                            parsed_date = datetime.strptime(doc_date, '%Y/%m/%d')
                        except:
                            print(f"Date format error for {tseno} {docid}")
                            parsed_date = dataframe_builder_instance.date_rounder(data['submitDateTime'])
                            
                            

                    
                    # 
            if parsed_date == None:
                print("oo")
                exit()

            
            del data
            if parsed_date < date:
                continue
            
            

            # filename = rest_server_interface.download_report(docid,tseno,5) # UNCOMMENT THIS FOR ACTUAL OPERATION
            #if document does not exist download it
            check_file = dl_file_path+'/'+tseno+'/'+docid+'/'+docid+'.zip'
            if not os.path.exists(check_file):
                del_path = rest_server_interface.download_report(docid,tseno,5)
            success = dataframe_builder_instance.check_qr(tseno,docid)
            if success:
                
                print(docid)
                pdf_file = dl_file_path+'/'+tseno+'/'+docid+'/'+docid+'.pdf'
                if not os.path.exists(pdf_file):
                    rest_server_interface.download_report(docid,tseno,2)
                dataframe_builder_instance.parse_qr_pdf(tseno,docid,pdf_file,parsed_date)

                
                

                    #now you need to download the information from the tables in the pdf


                #this is 
                os.remove(pdf_file)
            try:
                os.remove(del_path)
            except:
                print(f"no file found for {tseno} {docid}")

            
            # #delete the dowloaded document in the files folder whos filepath is called filename
        
             #figure out what this does
    dataframe_builder_instance.output_df("TEST_QUARTERLY_CSV_UTIL_1.csv")
    
    dataframe_builder_instance.append_qr_info()   #add the quarterly reports to the dataframe


    dataframe_builder_instance.to_csv("TEST_QUARTERLY_CSV_UTIL_2.csv")
    print("Downloaded and parsed all yearly reports.")
    dataframe_builder_instance.drop_auditors() #drops any auditors from the dataframe
    dataframe_builder_instance.work_history_process() #processes the work history by
    dataframe_builder_instance.tag_external_directors()
    dataframe_builder_instance.external_dates()
    
    dataframe_builder_instance.to_csv("TEST_CSV_UTIL_3.csv")
    dataframe_builder_instance.date_fixer('Year Joined')
    dataframe_builder_instance.to_csv("TEST_CSV_UTIL_4.csv")
    dataframe_builder_instance.drop_external()
    
    dataframe_builder_instance.to_csv("TEST_CSV_UTIL_4.csv")
    
    dataframe_builder_instance.create_output_df('202401')
    dataframe_builder_instance.output_df("TEST_CSV_UTIL_5.csv")
    dataframe_builder_instance.find_last_tse()
    dataframe_builder_instance.tse_matcher()

    dataframe_builder_instance.tse_fixer()
    dataframe_builder_instance.capitaliq_columns()
    dataframe_builder_instance.encoding_fixer('UTF-8')

    dataframe_builder_instance.output_df("TEST_CSV_UTIL_9.csv")
    dataframe_builder_instance
    # # #edate is todays date in the format YYYYMMDD WITH 0s
    # edate = datetime.now().strftime("%Y%m%d")
    # sdate = datetime.now() - relativedelta(months=1)
    # sdate = sdate.strftime("%Y%m%d")
    # qry = '異動'
    # run_tdnet_scraper(sdate, edate, qry)



    # dataframe_builder_instance.parse_tdnet_pdfs(edate, qry)
   
        


    

def run_tdnet_scraper(start_date, end_date, query):
    print('running tdnet scraper...')
    tdnet_main_path = 'Scraper/main.py'
    
    subprocess.run(['python3','-u', tdnet_main_path,  start_date, end_date, query])


    
def test_logic(ids):
    dataframe_builder_instance = dataframe_builder.DataFrameBuilder()
    # dataframe_builder_instance.build_dataframes("files")
    # dataframe_builder_instance.to_csv("test.csv")
    dataframe_builder_instance.read_csv1("TEST_CSV_UTIL_1.csv")
    
    
    """stuff that works"""
    # # # # print(len(df))
    # #remove duplicates of the same person
    dataframe_builder_instance.sort_officers()

    # #parses incorrectly input periods

    dataframe_builder_instance.period_fix()
        
    dataframe_builder_instance.to_csv("TEST_CSV_UTIL_2.csv")
    # dataframe_builder_instance.to_csv("test1.csv")
    # #adds external directors column
    
    
    # dataframe_builder_instance.to_csv("test1.csv")
    """stuff that works end"""
    


    # dataframe_builder_instance.to_csv("test2.csv")
    dataframe_builder_instance.work_history_processer()
    dataframe_builder_instance.previous_jobs()
    # dataframe_builder_instance.external_dates() #Honestly you could not tell me about what this does
    df = dataframe_builder_instance.get_sumdf()
    # # TSE:,Company Name,Name,DOB,Job Title,Work History,External,Document Title,Submission Date,Period End,type,error,Footnotes,Company Footnotes,document code,Last Internal Job,Last External Job,Year Joined,Concurrent Roles,year joined,WH Error


    df = df[['TSE:', 'Company Name', 'Name', 'Job Title','DOB', 'External', 'Last Internal Job', 'Last External Job', 'Year Joined', 'Concurrent Roles','WH Error', 'Recent Job Change']]
    df = df.sort_values(by='Recent Job Change', ascending=False)
    #drop columns with no year joined
    df['Year Joined'] = df['Year Joined'].str.strip()
    #only if 
    df.to_csv("testo.csv")

    # #get the number of rows in testo that either have External == True or External == False and WH Error ==3
    # print(df[(df['External']==True) | ((df['External']==False) & (df['WH Error']!=3))].shape[0])
    # print(df[((df['External']==False) & (df['WH Error']==3))].shape[0])
    # print(df[((df['External']==False) & (df['WH Error']==3))]['TSE:'].unique())
    # print(df[((df['External']==False) & (df['WH Error']==3))]['TSE:'].nunique())
    df = dataframe_builder_instance.get_sumdf()
    #write a csv of just the work histories in the df
    with open('work_histories.csv','w') as f:
        df['Work History'].to_csv(f)

    
    indices_to_drop = []
    for index, row in df.iterrows():
        if row["WH Error"]==3 and "取締" not in row["Job Title"]:
            indices_to_drop.append(index)
    
    df.drop(indices_to_drop, inplace=True)


    for tse in df["TSE:"].unique() :
        total_rows = len(df[df['TSE:'] == tse])
        external_rows = len(df[(df['TSE:'] == tse) & (df['External'] == True)])
        percentage = (external_rows / total_rows) * 100
        company_name = df[df['TSE:'] == tse]['Company Name'].iloc[0]
        print(f"company {company_name}, External Percentage: {percentage}%")

    # dataframe_builder_instance.add_quarterly_reports()
    # dataframe_builder_instance.to_csv("toomany.csv")
    # dataframe_builder_instance.read_csv1("toomany.csv")
    # dataframe_builder_instance.create_quarterly_reports()





    # dataframe_builder_instance.to_csv("test.csv")
    # dataframe_builder_instance.dataframe_rearranger()
    # dataframe_builder_instance.to_csv("test.csv")
    #
    # dataframe_builder_instance.to_csv("test.csv")
    # dataframe_builder_instance.to_csv_failures("failed.csv")
    #read all of the ids in from ids.txt
    # with open ("ids.txt",'r') as f:
    #     ids = f.readlines()

    # df = dataframe_builder_instance.get_sumdf()
    
    # for index, row in df.iterrows():
    #     # get rid of every row that has external? == True or external? == False and date joined == nan
    #     print
    #     if row['external?'] == True:
    #         df.drop(index, inplace=True)

    #     elif pd.isna(row['year joined'])==False :
            
    #         df.drop(index, inplace=True)
    # with open('fails.csv','w') as f:
    #     df.to_csv(f)
    


    # missing_ids = ['5595','141A'] #8952 has no files, 3226 has no files, 8963 has no files, 1326 has no files
    # for id in missing_ids:
    #     print(id)
    #     for document in os.listdir('files/'+id):
    #         if document == '.DS_Store':
    #             continue
    #         with open('files/'+id+'/'+document+'/'+document+'.json','r')as file:
    #             data = json.load(file)
    #             print(data['docDescription'])
    # #FIX THE FUCKING JOB HISTORY FORMAT LMAO code
    # for index,row in df.iterrows():
    #     text = row ['Work History']
    #     index_nen = [pos for pos, char in enumerate(text) if char == '年']
    #     #split the text by the position of the 年
    #     text = [text[i-4:j-4] for i, j in zip(index_nen, index_nen[1:]+[len(text)+4])]
    #     print(row['TSE:'], row['Name'])
    #     for i in text:
    #         print(i)
    #         time.sleep(1)

    # # dataframe_builder_instance.rearrange_columns('Name')
    # # dataframe_builder_instance.rearrange_columns('TSE:')
    # # dataframe_builder_instance.to_csv('sumdf.csv')
    # # df = dataframe_builder_instance.get_sumdf()
    # dataframe_builder_instance.joined_company_when()
    # # dataframe_builder_instance.to_csv('sumdf.csv')
    querier = csv_querier.CSVQuerier('test1.csv')# column condition contains sort
    # # querier.query("columns$TSE:$Name$Company Name$Job Title")
    querier.query("columns$TSE:$Name$External$Work History;boolCondition$External$1")
    # # df = dataframe_builder_instance.get_sumdf()
    # # dicto={}
    # # df["clean_names"] = df["Name"].str.replace(' ','').replace('.','').replace('　','')
    # # for unique in df['clean_names'].unique():
    # #     dicto[unique] = df[df['clean_names']==unique].shape[0]
    # #sort dict
    # dicto = dict(sorted(dicto.items(), key=lambda item: item[1],reverse=True))
    # for key in dicto:
    #     if dicto[key]>1:
    #         print(f"{key}:{dicto[key]}")

    
    




    #this is simply a way to ignore the body of the code above if the test loop is run.
    








    


if __name__ == "__main__":
    main()