import exporter
import openpyxl

class ExcelExporter(exporter.Exporter):
    def __init__(self, filename):
        super().__init__(filename)
        self.workbook = openpyxl.Workbook()
        self.sheet = self.workbook.active

    def export_data(self, data):
        row = 1
        for item in data:
            self.sheet.cell(row=row, column=1, value=item)
            row += 1

    def save(self):
        self.workbook.save(self.filename)