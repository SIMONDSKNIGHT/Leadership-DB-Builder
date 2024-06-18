import excel_reader
import rest_api 
import datetime
import edinet_code
import pretty_printer
import json
import csv
import time

file_path = "/Users/dagafed/Library/CloudStorage/OneDrive-Personal/Documents/top 500 by liquidity.xlsx"
rest_server_url = 'https://api.edinet-fsa.go.jp/api/v2/documents'
api_key = "ce7040824a7b414e9328a6edc0e82200"

def write_csv(file_ids):
    with open('file_ids.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['File ID', 'Value'])
        writer.writerows(file_ids)
#I know this is not best practice but this only needs to be run once
def main():
    reader = excel_reader.ExcelReader()
    rest_server = rest_api.RestApiInterface(rest_server_url, api_key)
    tagger=edinet_code.EdinetCodeAdder()
    reader.read(file_path)
    file_ids = reader.get_file_ids()
    write_csv(file_ids)
    del reader
    tagger.add_edinet_code_to_file_ids()
    startdate ="2024/02/07"
    enddate = "2024/02/07"
    parsedstart = datetime.datetime.strptime(startdate, "%Y/%m/%d")
    parsedend = datetime.datetime.strptime(enddate, "%Y/%m/%d")

    output = rest_server.search_reports(parsedstart, parsedend, mode=2)

    doc_id_list = []
    for file_id in file_ids:
        #this refers to 1514 住石ホールディングス株式会社
        id = file_id[0]
        
        name = file_id[1]
        id = id[4:]
        this_output = output
        this_output['results'] = list(filter(
            lambda item: item['secCode'] is not None and item['secCode'][:-1] == id and "第3四半期" in item['docDescription'],
            this_output['results']
        ))
        if this_output['results']==[]:
            print(f" failed to find a quarterly report for the third quarter of 2024 for {name}")
        else:
            for i in this_output['results']:
                doc_id_list.append((id,name,i["docDescription"],i['docID']))
        # Write doc_id_list to a JSON file
        
        break
    write_csv(doc_id_list)
    rest_server.download_reports( doc_id_list,1)
    rest_server.download_reports( doc_id_list,2)
    rest_server.download_reports( doc_id_list,3)
    rest_server.download_reports( doc_id_list,4)
    rest_server.download_reports( doc_id_list,5)
    print('complete')



        
if __name__ == "__main__":
    main()