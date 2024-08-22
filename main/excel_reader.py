import openpyxl

class ExcelReader():
    def __init__(self):
        self.file_ids = []

    def read(self, file_path):
        workbook = openpyxl.load_workbook(file_path)
        sheet = workbook.active

        values = []
        
        for row in sheet.iter_rows(min_row=9, min_col=1, max_col=4):
            if row[1].value :
                if ("Common Stock" in row[1].value or "Common Share" in row[1].value):
                    item = (row[0].value, row[3].value)  # Append Japanese company name from column 7
                    values.append(item)
            else:
                break
        self.file_ids=values
    def get_file_ids(self):
        return self.file_ids
    