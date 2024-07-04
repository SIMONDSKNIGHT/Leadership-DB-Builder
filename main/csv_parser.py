import csv
import os
import zipfile
import pandas as pd
import shutil


class CSVParser():
    def __init__(self, csv_file_path):
        self.different_format = False  
        self.csv_file_path = csv_file_path
        self.directordf = pd.DataFrame(columns=['Job Title', 'Name', 'DOB', 'Work History', 'Footnotes'])
        self.officerdf= pd.DataFrame(columns=['Job Title', 'Name', 'DOB', 'Work History', 'Footnotes'])
        

         
        
    def director_parse(self):
        try:
            zip_dir = os.path.dirname(self.csv_file_path)
            with zipfile.ZipFile(self.csv_file_path, 'r') as zip_ref:
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
            if rows == []:
                for index, row in stock.iterrows():
                    if  'OfficialTitleOrPositionInformationAboutDirectorsAndCorporateAuditors' in row['要素ID']:
                        rows.append(index)
                if rows != []:
                    print(f"document {self.csv_file_path} contains mismatched columns")
                else:
                    print(f"document {self.csv_file_path} does not contain the required columns")
            #find the distance between the rows/ it is the same every time
            distance = rows[1] - rows[0]
            #create a new dataframe with the columns Job Title, Name, DOB, Work History, Footnotes
            self.directordf = pd.DataFrame(columns=['Job Title', 'Name', 'DOB', 'Work History', 'Footnotes','external','which table?'])
            #iterate through the rows and extract the data

            try:
                if rows[0]+5!=rows[1]:
                    print(f"document {self.csv_file_path} contains mismatched columns")
                    self.different_format = True
            except:
                pass
            for row in rows:
                
                if 'OfficialTitleOrPositionInformationAboutDirectorsAndCorporateAuditors' in stock.iloc[row]['要素ID']:
                    job_title = stock.iloc[row ]['値']
                else:
                    job_title = 'misaligned columns'
                    self.different_format = True
                if 'NameInformationAboutDirectorsAndCorporateAuditors' in stock.iloc[row+1]['要素ID']:
                    name = stock.iloc[row + 1]['値']
                else:
                    name = 'misaligned columns'
                    self.different_format = True
                if  'DateOfBirthInformationAboutDirectorsAndCorporateAuditors' in stock.iloc[row+2]['要素ID']:

                    dob = stock.iloc[row + 2]['値']
                else:
                    dob = 'misaligned columns'
                    self.different_format = True
                if 'jpcrp_cor:CareerSummaryInformationAboutDirectorsAndCorporateAuditorsTextBlock' in stock.iloc[row+3]['要素ID']:
                    work_history = stock.iloc[row +3]['値']
                else:
                    work_history = 'misaligned columns'
                    self.different_format = True
                if 'TermOfOfficeInformationAboutDirectorsAndCorporateAuditors' in stock.iloc[row+4]['要素ID']:
                    work_history = stock.iloc[row +3]['値']
                else:
                    work_history = 'misaligned columns'
                    self.different_format = True

                footnotes = stock.iloc[row + 4]['値']
                
                
                
                self.directordf = self.directordf._append({'Job Title': job_title, 'Name': name, 'DOB': dob, 'Work History': work_history,
                                        'Footnotes': footnotes}, ignore_index=True)
            #merge the footnotes into 1 text file
            i= rows[-1]+distance
            footnotes = ""
            external_director = ""
            while stock.iloc[i]['要素ID'].__contains__('FootnotesDirectorsAndCorporateAuditorsTextBlock'):
                if '社外取締役' in stock.iloc[i]['値']:
                    external_director += stock.iloc[i]['値']
                    
            
                footnotes += stock.iloc[i]['値']
                i += 1
                
            #write the footnotes in as a column value in the new dataframe
            self.directordf['Company Footnotes'] = footnotes
            self.directordf['external'] = external_director
            self.directordf['which table?'] = 'director'
            #delete the extracted files
            
            if self.different_format==True:
                #create document called you're not crazy
                with open('youre_not_crazy.txt', 'a') as file:
                    file.write('youre not crazy')
            rows = []
            #build officer df
            for index, row in stock.iterrows():
                if row['要素ID'] == 'jpcrp_cor:OfficialTitleOrPositionInformationAboutExecutiveDirectors':
                    rows.append(index)
            if rows == []:
                for index, row in stock.iterrows():
                    if  "OfficialTitleOrPositionInformationAboutExecutiveDirectors" in row['要素ID']:
                        rows.append(index)
                if rows != []:
                    print(f"document {self.csv_file_path} contains mismatched columns")
                else:
                    print(f"document {self.csv_file_path} does not contain the required columns")
            #find the distance between the rows/ it is the same every time
            distance = rows[1] - rows[0]
            #create a new dataframe with the columns Job Title, Name, DOB, Work History, Footnotes
            self.officerdf = pd.DataFrame(columns=['Job Title', 'Name', 'DOB', 'Work History', 'Footnotes','external','which table?'])
            #iterate through the rows and extract the data

            try:
                if rows[0]+5!=rows[1]:
                    print(f"document {self.csv_file_path} contains mismatched columns")
                    self.different_format = True
            except:
                pass
            for row in rows:
                
                if 'OfficialTitleOrPositionInformationAboutExecutiveDirectors' in stock.iloc[row]['要素ID']:
                    job_title = stock.iloc[row ]['値']
                else:
                    job_title = 'misaligned columns'
                    print('1')
                    self.different_format = True
                if 'NameInformationAboutExecutiveDirectors' in stock.iloc[row+1]['要素ID']:
                    name = stock.iloc[row + 1]['値']
                else:
                    name = 'misaligned columns'
                    print('2')
                    self.different_format = True
                if  'DateOfBirthInformationAboutExecutiveDirectors' in stock.iloc[row+2]['要素ID']:

                    dob = stock.iloc[row + 2]['値']
                else:
                    dob = 'misaligned columns'
                    print('3')
                    self.different_format = True
                if 'CareerSummaryInformationAboutExecutiveDirectors' in stock.iloc[row+3]['要素ID']:
                    work_history = stock.iloc[row +3]['値']
                else:
                    work_history = 'misaligned columns'
                    print('4')
                    self.different_format = True
                if 'TermOfOfficeInformationAboutExecutiveDirectors' in stock.iloc[row+4]['要素ID']:
                    work_history = stock.iloc[row +3]['値']
                else:
                    work_history = 'misaligned columns'
                    print('5')
                    self.different_format = True

                footnotes = stock.iloc[row + 4]['値']
                
                
                
                self.officerdf = self.officerdf._append({'Job Title': job_title, 'Name': name, 'DOB': dob, 'Work History': work_history,
                                        'Footnotes': footnotes}, ignore_index=True)
            #merge the footnotes into 1 text file
            i= rows[-1]+distance
            footnotes = ""
            external_director = ""
            while stock.iloc[i]['要素ID'].__contains__('FootnotesExecutiveOfficersTextBlock'):
                footnotes += stock.iloc[i]['値']
                i += 1
                
            #write the footnotes in as a column value in the new dataframe
            self.officerdf['Company Footnotes'] = footnotes
            self.officerdf['external'] = external_director
            self.officerdf['which table?'] = 'officer'
            #delete the extracted files
            

            shutil.rmtree(new_dir)
        except Exception as e:
            print(f"Error parsing CSV file: {self.csv_file_path} ", e)
            shutil.rmtree(new_dir)



    def get_directordf(self):
        return self.directordf
    def get_officerdf(self):
        return self.officerdf
    def formatQ(self):

        return self.different_format

if __name__ == '__main__':
    parser = CSVParser('files/1332/S100R66L/S100R66L.zip')
    
    

