import os
import shutil

def organize_files(base_dir):
    # Iterate through all directories in the base directory
    for folder in os.listdir(base_dir):
        if folder == '.DS_Store':
            continue

        folder_path = os.path.join(base_dir, folder)
        if os.path.isdir(folder_path):
            # Iterate through files and subdirectories in the current directory
            for item in os.listdir(folder_path):
                if item == '.DS_Store':
                    continue
                item_path = os.path.join(folder_path, item)
                if os.path.isfile(item_path):
                    # Extract the file name without extension
                    file_name, file_extension = os.path.splitext(item)
                    # Create the subdirectory path
                    sub_dir = os.path.join(folder_path, file_name)
                    # Create the subdirectory if it does not exist
                    if not os.path.exists(sub_dir):
                        os.makedirs(sub_dir)
                    # Move the file to the subdirectory
                    shutil.move(item_path, os.path.join(sub_dir, item))
import os
import shutil

def code2(base_dir):
    # Iterate through all directories in the base directory
    # Iterate through all directories in the base directory
    for folder in os.listdir(base_dir):
        if folder == '.DS_Store':  
            continue
        folder_path = os.path.join(base_dir, folder)
        if os.path.isdir(folder_path):
            # Iterate through subdirectories in the current directory
            for sub_folder in os.listdir(folder_path):
                if sub_folder == '.DS_Store':  
                    continue

                sub_folder_path = os.path.join(folder_path, sub_folder)
                if os.path.isdir(sub_folder_path):
                    # Iterate through files in the subdirectory
                    for item in os.listdir(sub_folder_path):
                        if item == '.DS_Store':
                            continue
                        item_path = os.path.join(sub_folder_path, item)
                        if os.path.isfile(item_path):
                            # Check if the file ends with _info.txt and rename it to .json
                            if item.endswith("_info.txt"):
                                new_item = item.replace("_info.txt", ".json")
                                new_item_path = os.path.join(sub_folder_path, new_item)
                                os.rename(item_path, new_item_path)


base_directory = "files"  # Replace this with the actual path
if __name__ == "__main__":  
    code2(base_directory)