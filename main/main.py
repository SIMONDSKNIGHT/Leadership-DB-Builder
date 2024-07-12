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
        excel_filepath= input('address of excel file: ')
        rest_server_url = input('address of rest server: ')
    else:
        excel_filepath = "/Users/dagafed/Library/CloudStorage/OneDrive-Personal/Documents/top 500 by liquidity.xlsx"
        rest_server_url = 'https://api.edinet-fsa.go.jp/api/v2/documents'
    #####

    excel_parser = excel_reader.ExcelReader()
    try:
        excel_parser.read(excel_filepath)
    except:
        print('what')
        Exception('Error setting rest server')
        #kill process
        sys.exit()
    ids = []
    for i in excel_parser.get_file_ids():
        ids.append(i[0][4:])
        
    del excel_parser
    with open('ids.txt','w') as f:
        for i in ids:
            f.write(i+'\n')

    if args.test:
        test_logic(ids)
        print("Test complete.")
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
    reports = rest_server_interface.get_reports()
    for jsonobject in tqdm(reports):
        """ in actual operation what needs to be done
        is that the csv parser is generated before this loop
        and then for each id, it needs to find the most recent
        the yearly securities report, and download the requisite
        information and checking it for the presence of the iunfo
        while also creating a dataframe that lists the date of publishing 
        of the latest dataframe, and scan more if it fails

        then it needs to go through all of the lists, pick the one with
        least up to date information, download those quarterlies, and then
        update that one before moving onto the next one.

        """
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
        else:
            # behaviour only needed for initial install. TODO: remove and make the file delete itself
            print(f"Document {docID} for {tseNO} already exists. Skipping download.")
            continue
        #write the jsonobject to the folder
        with open(f'files/{tseNO}/{docID}/{docID}.json','w') as f:
            json.dump(jsonobject,f,indent=4,ensure_ascii=False)
        # rest_server_interface.download_report(docID,tseNO,2)
        # rest_server_interface.download_report(docID,tseNO,5)
    ### call this one the loop, this is the yearly loop
    file_path = 'files/'
    dataframe_builder_instance = dataframe_builder.DataFrameBuilder()
    for tseno in os.listdir(file_path):
        if tseno == '.DS_Store':
            continue
        TSE_path = os.path.join(file_path, tseno)
        sorted_docIDS=sorted(os.listdir(TSE_path))
        sorted_docIDS = reversed(sorted_docIDS)
        for docid in sorted_docIDS :
            if docid == ".DS" or docid == ".DS_Store":
                continue

            #open the info text file
            with open(file_path+'/'+tseno+'/'+tseno+'/'+docid+'.json', 'r') as file:
                data = json.load(file)
            document_title = data.get('docDescription')
        
            del data
            if "有価証券報告書" not in document_title:
                continue
            # rest_server_interface.download_report(document_code,sec_code,5)  UNCOMMENT THIS FOR ACTUAL OPERATION
            success = dataframe_builder_instance.add_to_dataframe(tseno,docid)
            if success:
                pass
                

            

    print("Downloaded all reports.")
    
def test_logic(ids):
    dataframe_builder_instance = dataframe_builder.DataFrameBuilder()
    # dataframe_builder_instance.build_dataframes("files")
    # dataframe_builder_instance.to_csv("test.csv")
    # dataframe_builder_instance.read_csv1("test.csv")
    
    
    # # # # 
    # # # # print(len(df))
    # #remove duplicates of the same person
    # dataframe_builder_instance.sort_officers()

    # dataframe_builder_instance.to_csv("test1.csv")
    # #parses incorrectly input periods
    # dataframe_builder_instance.period_fix()
    # dataframe_builder_instance.to_csv("test1.csv")
    # #adds external directors column
    # dataframe_builder_instance.tag_external_directors()
    # dataframe_builder_instance.to_csv("test1.csv")
    
    
    # dataframe_builder_instance.to_csv("test2.csv")
    # dataframe_builder_instance.work_history_processer()
    # dataframe_builder_instance.previous_jobs()
    # df = dataframe_builder_instance.get_sumdf()
    # # TSE:,Company Name,Name,DOB,Job Title,Work History,External,Document Title,Submission Date,Period End,type,error,Footnotes,Company Footnotes,document code,Last Internal Job,Last External Job,Year Joined,Concurrent Roles,year joined,WH Error


    # df = df[['TSE:', 'Company Name', 'Name', 'DOB', 'External', 'Last Internal Job', 'Last External Job', 'Year Joined', 'Concurrent Roles','WH Error', 'Recent Job Change']]
    # df = df.sort_values(by='Recent Job Change', ascending=False)
    # df.to_csv("testo.csv")
    # #get the number of rows in testo that either have External == True or External == False and WH Error ==3
    # print(df[(df['External']==True) | ((df['External']==False) & (df['WH Error']!=3))].shape[0])
    # print(df[((df['External']==False) & (df['WH Error']==3))].shape[0])
    # print(df[((df['External']==False) & (df['WH Error']==3))]['TSE:'].unique())
    # print(df[((df['External']==False) & (df['WH Error']==3))]['TSE:'].nunique())


    # dataframe_builder_instance.add_quarterly_reports()
    # dataframe_builder_instance.to_csv("toomany.csv")
    dataframe_builder_instance.read_csv1("toomany.csv")
    dataframe_builder_instance.create_quarterly_reports()





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