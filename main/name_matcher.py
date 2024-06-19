import csv

def match_names(h_file, doc_id_list):
    matched_names = []

    with open(h_file, 'r') as h_file:
        h_lines = h_file.readlines()

    with open(doc_id_list, 'r') as doc_id_file:
        csv_reader = csv.reader(doc_id_file)
        for row in csv_reader:
            last_string = row[-1]
            for line in h_lines:
                if line.strip().endswith(last_string):
                    matched_names.append(row)

    with open('matched_doc_id_list.csv', 'w', newline='') as output_file:
        csv_writer = csv.writer(output_file)
        csv_writer.writerows(matched_names)

    print("Matching names completed. The matched names are saved in 'matched_doc_id_list.csv'.")

if __name__ == '__main__':
    match_names('h.txt', 'doc_id_list.csv')