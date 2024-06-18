import excel_reader
import rest_api 
import datetime
import edinet_code
import pretty_printer

import csv
import time
file_path = "/Users/dagafed/Library/CloudStorage/OneDrive-Personal/Documents/top 500 by liquidity.xlsx"

def write_csv(file_ids):
    with open('file_ids.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['File ID', 'Value'])
        writer.writerows(file_ids)

def main(test=False):
    reader = excel_reader.ExcelReader()
    rest_server = rest_api.RestApiInterface()
    tagger=edinet_code.EdinetCodeAdder()
    reader.read(file_path)
    file_ids = reader.get_file_ids()
    write_csv(file_ids)
    del reader
    tagger.add_edinet_code_to_file_ids()


    for file_id in file_ids:
        #this refers to 1514 住石ホールディングス株式会社
        id = file_id[0]
        name = file_id[1]
        id = id[4:]
        date ="2024/02/07"
        parsed = datetime.datetime.strptime(date, "%Y/%m/%d")
        output = rest_server.get_response( parsed,mode=2)
        pretty_printer.pretty_print(output)

        time.sleep(1)  # Delay for 1 second

        break
    



        
if __name__ == "__main__":
    main(test=True)