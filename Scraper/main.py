import argparse
import os
# main.py
from scraper.document_finder import DocumentFinder
from scraper.pdf_downloader import PDFDownloader
from config.settings import BASE_URL, DOWNLOAD_PATH, HEADERS
import re
import os
import re



def initialize_dates_and_folder(start_date, end_date, query):
    # Compile regex to match folder names in the format YYYYMMDD~YYYYMMDD-query_term
    pattern = re.compile(r"(\d{8})~(\d{8})-(.+)")
    new_folder_name_with_path = None
    folder_exists = False

    for folder_name in os.listdir(DOWNLOAD_PATH):
        if folder_name == "metadata.json":
            continue
        match = pattern.match(folder_name)
        if match:
            existing_start_date, existing_end_date, existing_query = match.groups()
            if existing_query == query:
                folder_exists = True
                # Update the start_date if it falls within the range of existing dates
                if start_date < existing_end_date:
                    start_date = existing_end_date
                
                # Rename folder with the new end_date
                new_folder_name = f"{existing_start_date}~{end_date}-{query}"
                new_folder_name_with_path = os.path.join(DOWNLOAD_PATH, new_folder_name)
                os.rename(os.path.join(DOWNLOAD_PATH, folder_name), new_folder_name_with_path)
                break
    
    if not folder_exists:
        # If the query is different or folder doesn't exist, create a new folder
        new_folder_name = f"{start_date}~{end_date}-{query}"
        new_folder_name_with_path = os.path.join(DOWNLOAD_PATH, new_folder_name)
        os.makedirs(new_folder_name_with_path)

    return start_date, end_date, new_folder_name_with_path
def main(start_date, end_date, query):
    start_date, end_date, new_file_path = initialize_dates_and_folder(start_date, end_date, query)
    

    search_criteria = {
        "t0": start_date,
        "t1": end_date,
        "q": query,
        "m": "0"
    }
    """if the downloads folder doesn't exist, make it"""
    
    if not os.path.exists(DOWNLOAD_PATH):
        os.makedirs(DOWNLOAD_PATH)




    finder = DocumentFinder(BASE_URL,HEADERS, search_criteria)
    downloader = PDFDownloader(new_file_path,BASE_URL)


    documents = finder.find_documents()
    for link, metadata in documents:
        downloader.download_pdf(link, metadata)

    # Save metadata to a file
    metadata_file = os.path.join(DOWNLOAD_PATH, 'metadata.json')
    downloader.save_metadata(metadata_file)
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download PDF documents from a website.")
    #todays date in YYMMDD format
    parser.add_argument("start_date", help="Start date for the search in YYYYMMDD format")
    parser.add_argument("end_date", help="End date for the search in YYYYMMDD format")
    parser.add_argument("query", help="Search query")
    
    args = parser.parse_args()
    #string start to end date separated by tilde
    
    main(args.start_date, args.end_date, args.query)