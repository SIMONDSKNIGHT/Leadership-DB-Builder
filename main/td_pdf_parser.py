import pikepdf
import json
import traceback


class PDFParser:
    def __init__(self ):
        pass
        
    

    def check_pdf_metadata(self, filepath):
        metadata_filepath = '/Users/dagafed/Documents/GitHub/Leadership-DB-Builder/TDnet-Scraper/downloads/metadata.json'
        with open(metadata_filepath, 'r') as file:
            metadata = json.load(file)
        #check if the json file contains the file id
        # Check the metadata of the single pdf file
        #note that metadata is structured as a list of dictionaries
        file_id = filepath.split('/')[-1]
        file_metadata = next((item for item in metadata if item['filename'].endswith(file_id)), None)
        
        if file_metadata:
            
            return file_metadata 
            
        else:
            metadata= self.extract_pdf_metadata(filepath)
        return metadata
        


    def extract_pdf_metadata(self, pdf_path):
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
                metadata = pdf.docinfo  # Extracting the document info, which contains metadata
                if metadata:
                    print("PDF Metadata:")
                    for key, value in metadata.items():
                        print(f"{key}: {value}")
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
                    return "FAILED"
        
                





                return output_metadata
        except Exception as e:
            print(f"An error occurred: {e}")
            



