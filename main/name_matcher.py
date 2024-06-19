import csv
import sys

def match_names(h_file, doc_id_list):
    matched_names = []
    name_only = []
    comp_names = []

    with open(h_file, 'r') as h_file:
        h_lines = h_file.readlines()
        for line in h_lines:
            #split the line and get the last string
            comp_name = line[45:]
            if comp_name[-1] == '\n':
                comp_name = comp_name[:-1]
            comp_names.append(comp_name)
    print(comp_names)


    with open(doc_id_list, 'r') as doc_id_file:
        csv_reader = csv.reader(doc_id_file)
        #skip first row
        next(csv_reader)
        for row in csv_reader:
            this_name = row[1]
            
            if this_name in comp_names:
                
                name_only.append(this_name)
                matched_names.append(row)
        
        
        if len(matched_names) != len(comp_names):
            print(f"Failed to find a match for the following company names:")
            for name in comp_names:
                if name not in name_only:
                    print(name)
            sys.exit(1)

    with open('rejected.csv', 'w', newline='') as output_file:
        csv_writer = csv.writer(output_file)
        csv_writer.writerows(matched_names)

    print("Matching names completed. The matched names are saved in 'matched_doc_id_list.csv'.")

if __name__ == '__main__':
    match_names('rejected.txt', 'file_ids.csv')