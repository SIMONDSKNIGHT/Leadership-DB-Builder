import csv

class EdinetCodeAdder:
    def __init__(self,  file_ids_csv_path= "file_ids.csv", output_csv_path="edinetcodeDlInfo.csv"):
        self.edinet_csv_path = output_csv_path
        self.file_ids_csv_path =  file_ids_csv_path
        self.output_csv_path = "file_ids_with_edinet.csv"

    def add_edinet_code_to_file_ids(self):
        # Read the edinet code CSV file
        edinet_codes = {}
        with open(self.edinet_csv_path, 'r') as edinet_file:
            edinet_reader = csv.reader(edinet_file)
            next(edinet_reader)  # Skip header row
            for row in edinet_reader:
                tse_value = row[11]
                edinet_code = row[0]
                if tse_value != '':
                    tse_value = tse_value[:-1]
                edinet_codes[tse_value] = edinet_code

        # Read the file IDs CSV file and add the edinet code
        with open(self.file_ids_csv_path, 'r') as file_ids_file, \
                open(self.output_csv_path, 'w', newline='') as output_file:
            file_ids_reader = csv.reader(file_ids_file)
            output_writer = csv.writer(output_file)
            # Add column title "edinet code" to the output CSV
            header_row = next(file_ids_reader)
            header_row.append("edinet code")
            output_writer.writerow(header_row)
            for row in file_ids_reader:
                tse_value = row[0][4:]
                if tse_value in edinet_codes:
                    edinet_code = edinet_codes[tse_value]
                    row.append(edinet_code)
                else:
                    row.append('')
                output_writer.writerow(row)
