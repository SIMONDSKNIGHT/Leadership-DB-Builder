import excel_reader
import rest_api 
import datetime
import edinet_code
import pretty_printer
import json
import argparse
import csv
import sys
import time

file_path = "/Users/dagafed/Library/CloudStorage/OneDrive-Personal/Documents/top 500 by liquidity.xlsx"
rest_server_url = 'https://api.edinet-fsa.go.jp/api/v2/documents'


def write_csv(file_ids, filename):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['File ID', 'Value'])
        writer.writerows(file_ids)
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
    parser.add_argument('number', type=int, help="A number to determine behavior",default=1)
    args = parser.parse_args()
    number = args.number

    reader = excel_reader.ExcelReader()
    rest_server = rest_api.RestApiInterface(rest_server_url, api_key)
    tagger=edinet_code.EdinetCodeAdder()
    reader.read(file_path)
    file_ids = reader.get_file_ids()
    write_csv(file_ids, "file_ids.csv")
    del reader

    tagger.add_edinet_code_to_file_ids()

    startdate ="2023/04/01"

    enddate = "2024/06/01"
    if number != 1:
        parsedstart = datetime.datetime.strptime(startdate, "%Y/%m/%d")

        parsedend = datetime.datetime.strptime(enddate, "%Y/%m/%d")
        
        output = rest_server.search_reports(parsedstart, parsedend, mode=2)
        startdate = startdate.replace("/", "-")
        enddate = enddate.replace("/", "-") 
        with open(startdate+"~"+enddate+".txt", "w") as file:
            file.write(json.dumps(output, indent=4,ensure_ascii=False))

        print("downloaded info from "+startdate+" to "+enddate)
        sys.exit(0)
    else:
        startdate = startdate.replace("/", "-")
        enddate = enddate.replace("/", "-") 
        with open(startdate+"~"+enddate+".txt", "r") as file:
            output = json.load(file)
    
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
            for i in this_output['results']:
                doc_id_list.append((id,name,i["docDescription"],i['docID']))
        # Write doc_id_list to a JSON file
        
        
    rbool=input("download reports?")
    if rbool.lower() != "yes" or rbool.lower() != "y":
        sys.exit(0)


    write_csv(doc_id_list, "doc_id_list.csv")

    #rest_server.download_reports( doc_id_list,1)
    rest_server.download_reports( doc_id_list,2)
    
    #rest_server.download_reports( doc_id_list,3)
    #rest_server.download_reports( doc_id_list,4)
    rest_server.download_reports( doc_id_list,5)
    print('complete')



        
if __name__ == "__main__":
    main()