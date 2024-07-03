import os
import shutil
import json

# Define the path to the parent directory
parent_dir = 'files'  # Replace with the correct path

# Function to organize files by document ID
def organize_files_by_document_id(parent_dir):
    for subdir in os.listdir(parent_dir):
        subdir_path = os.path.join(parent_dir, subdir)

        if os.path.isdir(subdir_path):
            # Iterate over the files in the subdirectory
            for file_name in os.listdir(subdir_path):
                file_path = os.path.join(subdir_path, file_name)
                
                # Extract the document ID from the file name (e.g., S100QOLT from S100QOLT.zip)
                document_id = file_name.split('_')[0]  # Assuming document ID is the first part of the file name

                # Create a new directory for the document ID if it does not exist
                document_id_dir = os.path.join(parent_dir, document_id)
                if not os.path.exists(document_id_dir):
                    os.makedirs(document_id_dir)

                # Move the file to the new directory
                shutil.move(file_path, os.path.join(document_id_dir, file_name))

    print("Files have been organized by document ID.")
def unfuck_this_shit(parent_dir):
    docIDtoTSEno={}
    for subdir in os.listdir(parent_dir):
        subdir_path = os.path.join(parent_dir, subdir)
        if subdir[0]!="S":
            continue
        docID=subdir[:8]
        #if subdir name contains a .
        if '.' in subdir:
            filetype = subdir.split('.')[1]
        else:
            filetype = 'txt'
            #open the pdf file inside the directory
            # Open the PDF file inside the directory
            with open(os.path.join(subdir_path, f"{docID}_info.{filetype}"), "rb") as file:
                content = file.read()
            data = json.loads(content)
            sec_code = data.get('secCode')
            sec_code = sec_code[:4]
            if docID not in docIDtoTSEno:
                docIDtoTSEno[docID]=sec_code
    for docID in docIDtoTSEno:
        # Get the TSE number for the document ID
        tse_no = docIDtoTSEno[docID]

        # Create the directory for the TSE number if i  does not exist
        
        # Move the content files to the document ID directory inside the TSE number directory
        new_dir = os.path.join(tse_no, docID)

        if not os.path.exists(new_dir):
            os.makedirs(new_dir)
        old_location_txt = os.path.join(parent_dir, docID)
        old_location_txt = f'{old_location_txt}/{docID}_info.txt'

        #move the txt files
        shutil.move(old_location_txt, new_dir)
        #move the pdf files
        old_location_pdf = os.path.join(parent_dir, docID)
        old_location_pdf = f'{old_location_pdf}.pdf/{docID}.pdf'

        shutil.move(old_location_pdf, new_dir)
        #move the zip files
        old_location_zip = os.path.join(parent_dir, docID)
        old_location_zip = f'{old_location_zip}.zip/{docID}.zip'
        shutil.move(old_location_zip, new_dir)




        #merge all directories that start with the same 8 digit document ID

def document_verifier(json_files, files):
    #open the json file
    json_file_path = "json/2023-04-01~2024-06-01_有価証券報告書.txt"
    doc_dict = {}
    doc_valid_bool = {}
    with open(json_file_path, "r") as file:
        content = file.read()
    data = json.loads(content)
    for doc in data:
        doc_dict[doc['docID']] = doc['secCode'][:4]
        doc_valid_bool[doc['docID']] = False
    

    
    #spell out documents in files

    
    for doc in doc_dict:
        print(doc)

def json_fixer(json):
    pass

        


# Call the function to organize files
if __name__ == '__main__':
    json_files = 'json'
    files = 'files'
    json_fixer(json_files)
    # document_verifier(json_files, files)