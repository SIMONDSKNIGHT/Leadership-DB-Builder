import PyPDF2

class PDFParser:
    def __init__(self, pdf_path):
        self.pdf_path = pdf_path

    def find_table_in_pdf(self, target_page, target_header):
        with open(self.pdf_path, 'rb') as file:
            reader = PyPDF2.PdfFileReader(file)
            page = reader.getPage(target_page - 1)
            text = page.extract_text()

            # Find the row with the target header
            table_rows = text.split('\n')
            header_row = None
            for row in table_rows:
                if target_header in row:
                    header_row = row
                    break

            if header_row is None:
                raise ValueError("Table with the given header not found")

            # Assuming the table has fixed number of columns
            table = []
            for row in table_rows:
                if row == header_row:
                    continue
                table.append(row.split('\t'))  # Assuming tab-separated columns

            return table

# Usage example
pdf_path = '/path/to/your/pdf/file.pdf'
target_page = 3
target_header = 'Name'

try:
    parser = PDFParser(pdf_path)
    table = parser.find_table_in_pdf(target_page, target_header)
    print(table)
except ValueError as e:
    print(str(e))