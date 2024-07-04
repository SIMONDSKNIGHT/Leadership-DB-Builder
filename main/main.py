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
    if args.test:
        test_logic(ids)
        print("Test complete.")
        exit()



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
    ### call this one the loop
    print("Downloaded all reports.")
    
def test_logic(ids):
    dataframe_builder_instance = dataframe_builder.DataFrameBuilder()
    # # dataframe_builder_instance.build_dataframes("files")
    # # dataframe_builder_instance.to_csv("test.csv")
    dataframe_builder_instance.read_csv1("test.csv")
    # # 
    # # print(len(df))
    dataframe_builder_instance.sort_officers()

    dataframe_builder_instance.to_csv("test.csv")

    dataframe_builder_instance.tag_external_directors()
    dataframe_builder_instance.to_csv("test.csv")
    dataframe_builder_instance.joined_company_when()
    dataframe_builder_instance.lastjob()
    dataframe_builder_instance.to_csv("test.csv")
    dataframe_builder_instance.dataframe_rearranger()
    dataframe_builder_instance.to_csv("test.csv")

    # # dataframe_builder_instance.rearrange_columns('Name')
    # # dataframe_builder_instance.rearrange_columns('TSE:')
    # # dataframe_builder_instance.to_csv('sumdf.csv')
    # # df = dataframe_builder_instance.get_sumdf()
    # dataframe_builder_instance.joined_company_when()
    # dataframe_builder_instance.to_csv('sumdf.csv')
    querier = csv_querier.CSVQuerier('test.csv')# column condition contains sort
    # querier.query("columns$TSE:$Name$Company Name$Job Title")
    querier.query("columns$TSE:$Name$external?$Job Title$year joined$Company Name$last job;boolCondition$external?$True;sort$year joined$desc")
    # df = dataframe_builder_instance.get_sumdf()
    # dicto={}
    # df["clean_names"] = df["Name"].str.replace(' ','').replace('.','').replace('　','')
    # for unique in df['clean_names'].unique():
    #     dicto[unique] = df[df['clean_names']==unique].shape[0]
    # #sort dict
    # dicto = dict(sorted(dicto.items(), key=lambda item: item[1],reverse=True))
    # for key in dicto:
    #     if dicto[key]>1:
    #         print(f"{key}:{dicto[key]}")
    
    




    #this is simply a way to ignore the body of the code above if the test loop is run.
    








    


if __name__ == "__main__":
    main()