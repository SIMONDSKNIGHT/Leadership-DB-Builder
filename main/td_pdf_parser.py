import pikepdf
import json
import traceback
import pdfplumber
import re
import traceback
import pandas as pd
import os
employment_keywords = ['役員', '取締', '執行役','人事異動']

class PDFParser:
    def __init__(self ):
        self.df = pd.DataFrame()
        self.metadata = []
        """
        #note the metadata is in format:
        "filename"
        "company_name"
        "company_code
        "file_timestamp"
        "document_name"
        """
        
        
    

    def check_pdf_metadata(self, filepath):
        
        metadata_filepath = '/Users/dagafed/Documents/GitHub/Leadership-DB-Builder/Scraper/downloads/metadata.json'
        with open(metadata_filepath, 'r') as file:
            metadata = json.load(file)
        #check if the json file contains the file id
        # Check the metadata of the single pdf file
        #note that metadata is structured as a list of dictionaries
        file_id = filepath.split('/')[-1]
        file_metadata = next((item for item in metadata if item['filename'].endswith(file_id)), None)
        
        if file_metadata:
            
            return file_metadata, True
            
        else:
            metadata, success= self.extract_pdf_metadata(filepath)

            return metadata, success
    def extract_pdf_metadata(self, pdf_path):
        sucbool = True
        fn = pdf_path.split('/')[-1]
        output_metadata = {
             "filename": fn,
            "company_name": "",
            "company_code": "",
            "file_timestamp": "",
            "document_name": ""
        }
        try:
            with pikepdf.Pdf.open(pdf_path) as pdf:
                metadata = pdf.docinfo  # q
                ###Extracting the document info, which contains metadata
                if metadata:
                    for key, value in metadata.items():
                        #check if /ModDate is in the metadata
                        if key == '/ModDate':
                            #change to format YYYY/MM/DD HH:MM
                            value = str(value)
                            output_metadata["file_timestamp"] = value[2:6] + '/' + value[6:8] + '/' + value[8:10] + ' ' + value[10:12] + ':' + value[12:14]
                            
                        if key == '/CreationDate' and output_metadata["file_timestamp"] != "":
                            value = str(value)
                            output_metadata["file_timestamp"] = value[2:6] + '/' + value[6:8] + '/' + value[8:10] + ' ' + value[10:12] + ':' + value[12:14]
                    
                else:
                    print("No metadata found in the PDF.")
                    return output_metadata, False


                    
                ### BELOW IS THE CODE TO EXTRACT INFO 'company_name': '', 'company_code': '' and 'document_name': ''
                with pdfplumber.open(pdf_path) as pdfplumb:
                    first_page = pdfplumb.pages[0]
                    
                    text = first_page.extract_text()
                
                    if len(pdfplumb.pages) > 1:
                        second_page = pdfplumb.pages[1]
                        text2 = second_page.extract_text()
                        
                        if text is None:
                            text = text2
                        else:
                            text = text + text2

                    # Split the text into lines
            lines = text.splitlines()
            #check if parsed document is (cid:2343) for example
            

            # Initialize variables to store extracted information
            company_name = None
            company_id = None
            title = None

            # Regex pattern to find the company ID
            company_id_pattern = r"[（\(ｺｰﾄﾞ番号]\s*[:：]?\s*(\d{4})"
            company_id_pattern2 = r"[（\(ｺｰﾄﾞ]\s*[:：]?\s*(\d{4})"



            # Extract the information line by line
            for line in lines:
                # Extract company name (look for the line starting with "会社名")
                if "会社名" in line.replace(" ", "").replace("　", ""):
                    company_name = line.split("会社名")[-1].strip()
                if '企業名' in line.replace(" ", "").replace("　", ""):
                    company_name = line.split("企業名")[-1].strip()
                    
                if company_name is None:
                    if "株式会社" in line.replace(" ", "").replace("　", ""):
                        company_name = line.strip()
                    
                company_id_pattern2 = r"[（\(コード：\s]*(\d{4})"

# Try the first pattern
                # Extract company ID using regex
                id_match = re.search(company_id_pattern, line.replace(" ", "").replace("　", ""))
                if not id_match:
                    # If the first pattern fails, try the second pattern
                    id_match = re.search(company_id_pattern2, line)

                if id_match:
                    company_id = id_match.group(1)

                # Extract the title containing "お知らせ"
                if "お知らせ" in line:
                    title = line.strip()

            # Update the metadata dictionary
            if company_name is None:
                sucbool = False
            if company_id is None:
                sucbool = False
            else:
                #format the id to 4 digits in half-width
                company_id.translate(str.maketrans('０１２３４５６７８９', '0123456789'))
                company_id = company_id[:4]
            if title is None:
                sucbool = False
            output_metadata["company_name"] = company_name
            output_metadata["company_code"] = company_id
            output_metadata["document_name"] = title
            

            return output_metadata, sucbool
        except Exception as e:
            print(f"An error occurred: {e}")
            traceback.print_exc()
    def load_metadata(self, metadata = "Scraper/downloads/metadata.json"):
        if type(metadata)==str:
            with open(metadata, 'r') as file:
                metadata = json.load(file)
            self.metadata = metadata
        else:
            self.metadata = metadata

    def get_movement_info(self, download_folder):
        self.sort_metadata()
        self.get_from_tables(download_folder)



    def sort_metadata(self):
        listfit = []
        print(len(self.metadata))
        for item in self.metadata:
            if item['document_name'] and  any(keyword in item['document_name'] for keyword in employment_keywords):
                

                listfit.append(item)
        self.metadata = listfit
    
    def get_from_tables(self, download_folder):
        fail_tracker = []
        num = 1
        #sort self metadata by filename
        self.metadata = sorted(self.metadata, key = lambda x: x['filename'])
        for item in self.metadata:
            filepath = f"{download_folder}/{item['filename']}"
            num, result = self.get_doc_tables(filepath, item, num)
            if not result:
                fail_tracker.append(item)
        self.df.to_csv('AAAAAAAAA.csv')
        kill = input('kill?: y/n ')
        if  kill != 'n':
            #delete every file in the download folder that starts with TEST_{number}.csv where regex is used
            
            for file in os.listdir():
                if re.match(r'TEST_\d+.csv', file):
                    os.remove(f"{file}")
        
    def get_doc_tables(self, file_path, item, num = 1):
        tracker = False
        try:

            with pdfplumber.open(file_path) as pdf:
                
                for page_number in range(len(pdf.pages)):
                    page = pdf.pages[page_number]
                    tables = page.extract_tables()
                    for table in tables:
                        columns = table[0]
                        columns = self.column_cleaner(columns)

                        
                        df = pd.DataFrame(table[1:], columns=columns)
                        #drop empty columns
                        df = df.dropna(axis=1, how='all')
                        

                        
                        while  os.path.exists(f'TEST_{num}.csv')==True:
                            num += 1
                        df.to_csv(f'TEST_{num}.csv', index=False)
                        #add new filename column to the dataframe
                        df['filename'] = f'TEST_{num}.csv'
                        df['pdf_filename'] = item['filename']
                        df['Company Name'] = item['company_name']
                        df['Company Code'] = item['company_code']
                        df['Document Name'] = item['document_name']
                        df['File Timestamp'] = item['file_timestamp']
                        pr_str = f'TEST_{num}.csv for {file_path.split("/")[-1]}'
                        if tracker == False:
                           pr_str += ' (First table found)'
                        print(pr_str)
                        #if csv is empty


                    # Ensure unique column names
                        df.columns = pd.Index([f"{col}_{i}" if df.columns.duplicated()[i] else col for i, col in enumerate(df.columns)])
                        if not hasattr(self, 'df') or self.df is None:
                            self.df = pd.DataFrame()
                        self.df.columns = pd.Index([f"{col}_{i}" if self.df.columns.duplicated()[i] else col for i, col in enumerate(self.df.columns)])

                        # Reset index to avoid index conflicts
                        df = df.reset_index(drop=True)
                        self.df = self.df.reset_index(drop=True)
                        
                        # Reindex DataFrames to have the same columns
                        all_columns = self.df.columns.union(df.columns)
                        self.df = self.df.reindex(columns=all_columns)
                        df = df.reindex(columns=all_columns)

                        # Concatenate DataFrames, ignoring the index
                        if not df.empty:
                            self.df = pd.concat([self.df, df], ignore_index=True)

                        tracker = True
            if tracker == False:
                print(f'No tables found in {file_path.split("/")[-1]}')
            return num, tracker
        except:
            print('error')
            traceback.print_exc()

    def get_from_text(self):
        pass
    def output_df_test(self):
        counter = 1
        #check if a file called td_info_{counter}.csv exists, and if not iterate counter 1 higher
        while os.path.exists(f'td_info_{counter}.csv'):
            counter += 1
        #output the dataframe to a csv file
        self.df.to_csv(f'td_info_{counter}.csv')

    def detect_and_reorient_table(self,df):
        # Example logic to detect misaligned tables
        misaligned_columns = []
        
        for col in df.columns:
            # Detect if a column name is a date or something unusual
            if re.match(r'\d{4}年', col) or len(col) > 50:
                misaligned_columns.append(col)
        
        if len(misaligned_columns) > 0:
            # If we detect misaligned columns, we will extract and reorient the table
            reoriented_data = {}
            
            for col in misaligned_columns:
                # Extract the data from these misaligned columns
                col_data = df[col].dropna().tolist()
                if len(col_data) > 0:
                    reoriented_data[col] = col_data
            
            # Create a new DataFrame from the reoriented data
            reoriented_df = pd.DataFrame(reoriented_data)
            
            # Optionally, drop the misaligned columns from the original DataFrame
            df.drop(columns=misaligned_columns, inplace=True)
            
            return reoriented_df
        
        # If no misaligned columns are detected, return None or the original DataFrame
        return 

    
    def column_cleaner(self, columns):
        # Step 1: Strip spaces, full-width spaces, and line breaks
        stripped_columns = [i.replace(' ', '').replace('　', '').replace('\n', '') if i is not None else '' for i in columns]

        # Step 2: Standardize common column names
        standardized_columns = []
        for col in stripped_columns:
            if '氏名' == col:
                standardized_columns.append('氏名')

            elif '役職' == col:
                standardized_columns.append('役職')
            elif '略歴' ==col:
                standardized_columns.append('略歴')
            elif '新任' == col or '就任' in col:
                standardized_columns.append('新任')
            elif '旧任' == col or '退任' == col:
                standardized_columns.append('旧任')
            elif '生年月日' == col or '誕生日' == col:
                standardized_columns.append('生年月日')
            elif '現体制' == col and '新体制' in stripped_columns:
                #gentaisei is now　旧任 and shintaisei is now 新任
                standardized_columns.append('旧任')
            elif '現体制' == col and '新体制' not in stripped_columns:
                standardized_columns.append('新任')
            elif '新体制' == col and '現体制' in stripped_columns:
                standardized_columns.append('新任')



            else:
                standardized_columns.append(col)

        # Step 3: Ensure unique column names to avoid conflicts
        unique_columns = []
        seen = {}
        for i, col in enumerate(standardized_columns):
            if col in seen:
                seen[col] += 1
                unique_columns.append(f"{col}_{seen[col]}")
            else:
                seen[col] = 0
                unique_columns.append(col)

        return unique_columns




