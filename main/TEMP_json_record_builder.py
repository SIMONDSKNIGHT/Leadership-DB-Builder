import os
import json

def process_files_folder():
    files_folder = 'files'
    

    # Iterate through each directory in the files folder
    json_file_path = "json/2023-04-01~2024-06-01.txt"
    output = {}
    output['results'] = []
    for dir_name in os.listdir(files_folder):
        #skip if .ds_store
        if dir_name == '.DS_Store':
            continue
        dir_path = os.path.join(files_folder, dir_name)
        #grab all txt files in the directory
        
        
        for file_name in os.listdir(dir_path):
            if file_name.endswith('.txt'):
                file_path = os.path.join(dir_path, file_name)
                #open the file and read the contents
                with open(file_path, 'r') as file:
                    file_contents = file.read()
                    #create a json file with the same name as the txt file
                    output['results'].append(file_contents)
                    
    with open(json_file_path, 'w') as json_file:
        for json_str in output['results']:
            json_file.write(json_str + "\n")

        

    

if __name__ == '__main__':
    process_files_folder()

