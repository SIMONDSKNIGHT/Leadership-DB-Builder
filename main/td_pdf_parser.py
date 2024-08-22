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

    def get_movement_info(self):
        self.sort_metadata()


    def sort_metadata(self):
        listfit = []
        print(len(self.metadata))
        for item in self.metadata:
            if item['document_name'] and  any(keyword in item['document_name'] for keyword in employment_keywords):
                

                listfit.append(item)
        self.metadata = listfit
    
    def get_from_tables(self):
        pass
    def get_from_text(self):
        pass
    def output_df_test(self):
        counter = 1
        #check if a file called td_info_{counter}.csv exists, and if not iterate counter 1 higher
        while os.path.exists(f'td_info_{counter}.csv'):
            counter += 1
        #output the dataframe to a csv file
        self.df.to_csv(f'td_info_{counter}.csv')




