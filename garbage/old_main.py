import excel_reader
import rest_api 
import datetime
import garbage.edinet_code as edinet_code
import pretty_printer
import json
import argparse
import csv
import sys
import time
import os
from dateutil.relativedelta import relativedelta
import dataframe_builder
file_path = "/Users/dagafed/Library/CloudStorage/OneDrive-Personal/Documents/top 500 by liquidity.xlsx"
rest_server_url = 'https://api.edinet-fsa.go.jp/api/v2/documents'






def write_csv(file_ids, filename):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['File ID', 'Value'])
        writer.writerows(file_ids)
def find_next_quarter(date):
    date = date - relativedelta(months=+3)
    if date.month <= 3:
        return datetime.datetime(date.year, 3, 31)
    elif date.month <= 6:
        return datetime.datetime(date.year, 6, 30)
    elif date.month <= 9:
        return datetime.datetime(date.year, 9, 30)
    else:
        return datetime.datetime(date.year, 12, 31)
#I know this is not best practice but this only needs to be run once
def main():
    
    try:
        with open("main/api_key.txt", "r") as file:
            api_key = file.read()
    except FileNotFoundError:
        print("api_key.txt not found.")
        api_key=input("Input API key and press Enter to continue.")

    parser = argparse.ArgumentParser(description="Determine behavior based on an input number.")
    
    # Add a positional argument for the number
    
    parser.add_argument('--forceupdate', action='store_true', help="Update the list of reports")
    args = parser.parse_args()
    
    


    reader = excel_reader.ExcelReader()
    rest_server = rest_api.RestApiInterface(rest_server_url, api_key)
    tagger=edinet_code.EdinetCodeAdder()
    reader.read(file_path)
    file_ids = reader.get_file_ids()
    write_csv(file_ids, "file_ids.csv")
    del reader

    tagger.add_edinet_code_to_file_ids()

    
    #todays date
    enddate = datetime.datetime.now().strftime("%Y/%m/%d")
    #figure out the last day of the last quarter before the end date eg if this was 2023/06/01, the last day of the last quarter would be 2023/03/31
    #this is done by subtracting 3 months from the end date and then finding the next date of either march, june, september or december
    last_quarter_end = find_next_quarter(enddate)
    two_quarters_ago = find_next_quarter(last_quarter_end)
    three_quarters_ago = find_next_quarter(two_quarters_ago)
    four_quarters_ago = find_next_quarter(three_quarters_ago)
    five_quarters_ago = find_next_quarter(four_quarters_ago)


    #5 quarters ago
    #TODO Change Logic so that on startup it downloads 5 quarters of reports IF folder does not exist; if it exists, find the most recent folder and download for after this date
    #figure out the most recent previous update that was completed
    if args.forceupdate:
        startdate = (datetime.datetime.now() - datetime.timedelta(days=457)).strftime("%Y/%m/%d")
    else:
        try:
            file_names = os.listdir("json")
            #identify the most recent file IE the file for whom the last1 10 values correspond to the most recent date
            for i in range(len(file_names)):
                file_names[i] = file_names[i].split("~")[1].split(".")[0]
                #check to see if the file is unfinished by Presense of I at start of filename
                if file_names[i][0] == "I":
                    continue
                else:
                    file_names[i] = datetime.datetime.strptime(file_names[i], "%Y/%m/%d")
            latest_file = max(file_names).strftime("%Y/%m/%d")
            #name format is "2023-04-01~2024-06-01.txt"
            startdate = latest_file.split("~")[1].split(".")[0].replace("-", "/")
            startdate = (datetime.datetime.strptime(startdate, "%Y/%m/%d") + datetime.timedelta(days=1)).strftime("%Y/%m/%d")
        except FileNotFoundError:
            print("No past JSON files found. Downloading reports for 5 quarters.")
            startdate = (datetime.datetime.now() - datetime.timedelta(days=457)).strftime("%Y/%m/%d")

    
    parsedstart = datetime.datetime.strptime(startdate, "%Y/%m/%d")

    parsedend = datetime.datetime.strptime(enddate, "%Y/%m/%d")
    
    output = rest_server.search_reports(parsedstart, parsedend, mode=2)
    if output=={}:
        print("no reports found OR dates are invalid. Exiting...")
        print(f"startdate: {startdate}, enddate: {enddate}. Check that startdate is before enddate.")
        sys.exit(0)
    startdate = startdate.replace("/", "-")
    enddate = enddate.replace("/", "-") 

    

    
    doc_id_list = []
    
    for file_id in file_ids:
        #this refers to 1514 住石ホールディングス株式会社
        id = file_id[0]
        
        name = file_id[1]
        id = id[4:]
        this_output = {}
        this_output['results'] = list(filter(
            lambda item: item['secCode'] is not None and item['secCode'][:-1] == id and "有価証券報告書" in item['docDescription'],
            output['results']
        ))
        if this_output['results']==[]:
            print(f" failed to find yearly report for 2022/23 for {name}")
        else:
            folder_path = f'files/{id}'
            if not os.path.exists(folder_path):
                os.makedirs(folder_path)
            for i in this_output['results']:
                if not os.path.exists(f'files/{id}/{i["docID"]}_info.txt'):
                    with open(f'files/{id}/{i["docID"]}_info.txt', 'w') as file:
                        file.write(json.dumps(i, indent=4,ensure_ascii=False))
                    with open(f'json/I{startdate}~{enddate}.txt', 'a') as file:
                        file.write(json.dumps(i, indent=4,ensure_ascii=False) + "\n")
                doc_id_list.append((id,name,i["docDescription"],i['docID']))
        # Write doc_id_list to a JSON file
        
        
        
    rbool=input("download reports?:")
    if rbool.lower() != "yes" and rbool.lower() != "y":
        print("exiting...")
        sys.exit(0)
    print("downloading...")

    write_csv(doc_id_list, "doc_id_list.csv")

    #rest_server.download_reports( doc_id_list,1)
    rest_server.download_reports( doc_id_list,2)
    
    #rest_server.download_reports( doc_id_list,3)
    #rest_server.download_reports( doc_id_list,4)
    rest_server.download_reports( doc_id_list,5)
    #rename json file to reflect completion
    os.rename(f'json/I{startdate}~{enddate}.txt', f'json/{startdate}~{enddate}.txt')
    print('complete')



        
if __name__ == "__main__":
    main()