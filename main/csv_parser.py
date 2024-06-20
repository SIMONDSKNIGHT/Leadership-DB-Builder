import csv
import os
import zipfile
import pandas as pd
import shutil


class CSVParser():
    def __init__(self, csv_file_path):
        self.csv_file_path = csv_file_path
        self.df = pd.DataFrame(columns=['Job Title', 'Name', 'DOB', 'Work History', 'Footnotes'])
        self.parse(csv_file_path)
        
    def parse(self,file_path):
        try:
            zip_dir = os.path.dirname(self.csv_file_path)
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                zip_ref.extractall(zip_dir)
            #open the csvfile in the folder that starts with 'jpcrp03'
            new_dir = zip_dir+'/XBRL_TO_CSV'
            for file in os.listdir(new_dir):
                if file.startswith('jpcrp03'):
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
            for index, row in stock.iterrows():
                if row['要素ID'] == 'jpcrp_cor:OfficialTitleOrPositionInformationAboutDirectorsAndCorporateAuditors':
                    rows.append(index)
            #find the distance between the rows/ it is the same every time
            distance = rows[1] - rows[0]
            #create a new dataframe with the columns Job Title, Name, DOB, Work History, Footnotes
            self.df = pd.DataFrame(columns=['Job Title', 'Name', 'DOB', 'Work History', 'Footnotes'])
            #iterate through the rows and extract the data
            for row in rows:
                job_title = stock.iloc[row ]['値']
                name = stock.iloc[row + 1]['値']
                dob = stock.iloc[row + 2]['値']
                work_history = stock.iloc[row +3]['値']
                footnotes = stock.iloc[row + 4]['値']
                
                self.df = self.df._append({'Job Title': job_title, 'Name': name, 'DOB': dob, 'Work History': work_history,
                                        'Footnotes': footnotes}, ignore_index=True)
            #merge the footnotes into 1 text file
            i= rows[-1]+distance
            footnotes = ""
            while stock.iloc[i]['要素ID'] == 'jpcrp_cor:FootnotesDirectorsAndCorporateAuditorsTextBlock':
                footnotes += stock.iloc[i]['値']
                i += 1
                
            #write the footnotes in as a column value in the new dataframe
            self.df['Company Footnotes'] = footnotes
            #delete the extracted files
            shutil.rmtree(new_dir)
        except Exception as e:
            print(f"Error parsing CSV file: {file_path} ", e)


    def get_df(self):
        return self.df

if __name__ == '__main__':
    parser = CSVParser('files/1332/S100R66L/S100R66L.zip')
    
    

