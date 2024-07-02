import argparse
import excel_reader
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
import rest_api
import json


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
        excel_parser.set_rest_server(rest_server_url, api_key)
    except:
        
        Exception('Error setting rest server')
        exit()
    excel_parser.read(excel_filepath)
    excel_parser.get_file_ids()
    excel_parser.write_csv('file_ids.csv')
    del excel_parser
    today = datetime.datetime.now().strftime("%Y/%m/%d")
    backdate = datetime.datetime.now() - relativedelta(years=1)
    backdate = backdate.strftime("%Y/%m/%d")
    if args.forceupdate:
        pass
    else:
        #todays date in "%Y/%m/%d"
        
        download_directory = data_directory + 'downloads/'
        for document in os.listdir(download_directory):
            document= document.split('.')[0]
            #parse the name which is in format yyyy-mm-dd and compare to the year ago date
            #if the document is older than a year, delete it
            docu_date = datetime.datetime.strptime(document, "%Y-%m-%d").strftime("%Y/%m/%d")

            if docu_date == today:
                #make backdate tomorrow
                backdate = datetime.datetime.now() + relativedelta(days=1)
                backdate = backdate.strftime("%Y/%m/%d")
                break
            if docu_date > backdate:
                backdate = docu_date
            break
    rest_server_interface = rest_api.RestApiInterface(rest_server_url, api_key,backdate,today)
    rest_server_interface.save_reports()
    rest_server_interface.write_reports()
    reports = rest_server_interface.get_reports()
    for jsonobject in reports:
        tseNO = jsonobject['secCode'][:4]
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
            break
        #write the jsonobject to the folder
        with open(f'files/{tseNO}/{docID}/{docID}.json','w') as f:
            json.dump(jsonobject,f,indent=4,ensure_ascii=False)
        rest_server_interface.download_report(docID,tseNO,2)
        rest_server_interface.download_report(docID,tseNO,5)
    








    


    pass
if __name__ == "__main__":
    main()