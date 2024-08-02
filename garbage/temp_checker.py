import excel_reader
import sys
import json
excel_filepath = "/Users/dagafed/Library/CloudStorage/OneDrive-Personal/Documents/top 500 by liquidity.xlsx"
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
with open("data/json/2024-07-03.json", "r") as f:
    data = json.load(f)
#print number of json objects
print(len(data))
dicto = {}
dicto_count = {}
dicto_yearly = {}
dicto_submission = {}


for jsonobject in data:
    object_id = jsonobject['secCode'][:4]
    file_name = jsonobject['docDescription']
    submission_date = jsonobject['submitDateTime']
    period_end = jsonobject['periodEnd']
    if object_id not in dicto:
        dicto[object_id] = []
        dicto_count[object_id] = 0
        dicto_yearly[object_id] = False
    if  "有価証券報告" in file_name:
        dicto_yearly[object_id] = True
        dicto_submission[object_id] = submission_date

    
    
    dicto[object_id] += [file_name]
    dicto_count[object_id] += 1
for i in dicto:
    print(i, dicto_count[i],dicto_yearly[i], dicto[i])
#print number of trues in dicto_yearly
print(sum(dicto_yearly.values()))
#print number of false in dicto_yearly  
print(len(dicto_yearly) - sum(dicto_yearly.values()))
#sort the dicto_submission by date
sorted_dicto_submission = sorted(dicto_submission.items(), key=lambda x: x[1])
for i in sorted_dicto_submission:
    print(i)
    



