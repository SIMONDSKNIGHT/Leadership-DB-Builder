import csv
import os
import zipfile
import pandas as pd
import shutil
from file_parser import FileParser

class CSVParser(FileParser):
    def parse(self, file_path):
    
        zip_dir = os.path.dirname(file_path)
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(zip_dir)
        #open the csvfile in the folder that starts with 'jpcrp03'
        new_dir = zip_dir+'/XBRL_TO_CSV'
        for file in os.listdir(new_dir):
            if file.startswith('jpcrp03'):
                csv_file = os.path.join(new_dir, file)
                break
        #read the csv file
        df = pd.read_csv(
            csv_file,
            encoding='UTF-16LE',
            delimiter='\t',
            engine='python'  # Use Python engine to handle potential parsing issues
        )

        # Display the column names and the first few rows
        print("Column Names:", df.columns.tolist())
        print(df.head())
        #display the first row with the 要素ID "jpcrp_cor:OfficialTitleOrPositionInformationAboutDirectorsAndCorporateAuditors"
        print(df.loc[df['要素ID'] == 'jpcrp_cor:OfficialTitleOrPositionInformationAboutDirectorsAndCorporateAuditors'])
        

        
        


if __name__ == '__main__':
    parser = CSVParser()
    parser.parse('files/1332/S100R66L.zip')
    print("CSV file extracted successfully.")

