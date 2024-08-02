import csv
import os
import zipfile
import pandas as pd
import shutil


class QParser():
    def __init__(self, csv_file_path):
        self.different_format = False  
        self.csv_file_path = csv_file_path
        self.quarterdf = pd.DataFrame(columns=['Job Title', 'Name', 'DOB', 'Work History', 'Footnotes'])
        

         
        
    def parse(self):
        try:
            zip_dir = os.path.dirname(self.csv_file_path)
            with zipfile.ZipFile(self.csv_file_path, 'r') as zip_ref:
                zip_ref.extractall(zip_dir)
            #open the csvfile in the folder that starts with 'jpcrp03'
            new_dir = zip_dir+'/XBRL_TO_CSV'
            for file in os.listdir(new_dir):
                if file.startswith('jpcrp04'):
                    csv_file = os.path.join(new_dir, file)
                    break
            #read the csv file
            stock = pd.read_csv(
                csv_file,
                encoding='UTF-16LE',
                delimiter='\t',
                engine='python'  # Use Python engine to handle potential parsing issues
            )
            # find the addresses of the  rows with the 要素ID "jpcrp_cor:OfficialTitleOrPositionInformationAboutDirectorsAndCorporateAuditors" and store them
            # in a list
            rows = []
            content = ""
            for index, row in stock.iterrows():
                
                if 'InformationAboutOfficersTextBlock' in row['要素ID']:
                    
                    if "該当事項はありません" in row['値'] :
                        content = "NO_CHANGE"
                    # rows.append(index)
            print(content)
            
        except Exception as e:
            print(f"Error parsing CSV file: {self.csv_file_path} ", e)
            with open('failed.txt', 'a') as file:
                file.write(f"Error parsing CSV file: {self.csv_file_path} {e}")
            shutil.rmtree(new_dir)



    def get_df(self):
        return self.quarterdf
    def get_officerdf(self):
        return self.officerdf
    def formatQ(self):

        return self.different_format


    
    

