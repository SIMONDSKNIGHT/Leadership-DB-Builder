import pandas as pd
import matplotlib.pyplot as plt
from pandas import ExcelWriter

class FinancialReportGenerator:
    def __init__(self, csv_file_path):
        self.csv_file_path = csv_file_path
        self.data = None
    
    def read_csv(self):
        """Reads the CSV file into a pandas DataFrame."""
        self.data = pd.read_csv(self.csv_file_path)
        self.data['Date'] = pd.to_datetime(self.data['Date'])
    
    def generate_chart(self, output_chart_path='eps_chart.png'):
        """Generates a chart from the data and saves it as an image."""
        if self.data is None:
            raise ValueError("Data not loaded. Call read_csv() first.")
        
        plt.figure(figsize=(10, 6))
        plt.plot(self.data['Date'], self.data['EPS'], label='EPS', color='blue')
        plt.xlabel('Date')
        plt.ylabel('EPS')
        plt.title('EPS Over Time')
        plt.legend()
        plt.grid(True)
        plt.savefig(output_chart_path)
        plt.close()
    
    def export_to_excel(self, excel_file_path='output.xlsx', chart_image_path='eps_chart.png'):
        """Exports the data and chart to an Excel file."""
        if self.data is None:
            raise ValueError("Data not loaded. Call read_csv() first.")
        
        with ExcelWriter(excel_file_path, engine='xlsxwriter') as writer:
            self.data.to_excel(writer, sheet_name='Financial Data', index=False)
            workbook  = writer.book
            worksheet = writer.sheets['Financial Data']
            worksheet.insert_image('H2', chart_image_path)
    
    def create_report(self, excel_file_path='output.xlsx', chart_image_path='eps_chart.png'):
        """Full process: Read data, generate chart, export to Excel."""
        self.read_csv()
        self.generate_chart(chart_image_path)
        self.export_to_excel(excel_file_path, chart_image_path)

# Usage example
csv_file_path = 'toomany.csv'  # Replace with your CSV file path
report_generator = FinancialReportGenerator(csv_file_path)
report_generator.create_report(excel_file_path='financial_report.xlsx')