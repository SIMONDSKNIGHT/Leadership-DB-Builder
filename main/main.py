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
        rest_server_interface.download_report(docID,tseNO,2)
        rest_server_interface.download_report(docID,tseNO,5)
    print("Downloaded all reports.")
    
def test_logic(ids):
    
    dataframe_builder_instance = dataframe_builder.DataFrameBuilder()
    dataframe_builder_instance.build_dataframe("files")
    dataframe_builder_instance.to_csv("test.csv")

    #this is simply a way to ignore the body of the code above if the test loop is run.
    








    


if __name__ == "__main__":
    main()