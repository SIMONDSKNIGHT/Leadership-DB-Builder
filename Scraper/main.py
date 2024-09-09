import os
import re
import tkinter as tk
from tkinter import filedialog, messagebox
from datetime import datetime, timedelta
from scraper.document_finder import DocumentFinder
from scraper.pdf_downloader import PDFDownloader
from config.settings import BASE_URL, DOWNLOAD_PATH, HEADERS

ID_SOURCE = "ids.txt"

def initialize_dates_and_folder(start_date, end_date, query):
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
                if start_date < existing_end_date:
                    start_date = existing_end_date
                new_folder_name = f"{existing_start_date}~{end_date}-{query}"
                new_folder_name_with_path = os.path.join(DOWNLOAD_PATH, new_folder_name)
                os.rename(os.path.join(DOWNLOAD_PATH, folder_name), new_folder_name_with_path)
                break

    if not folder_exists:
        new_folder_name = f"{start_date}~{end_date}-{query}"
        new_folder_name_with_path = os.path.join(DOWNLOAD_PATH, new_folder_name)
        os.makedirs(new_folder_name_with_path)

    return start_date, end_date, new_folder_name_with_path

def download_pdfs(start_date, end_date, query, filter_query, use_id_list, reset_metadata):
    start_date, end_date, new_file_path = initialize_dates_and_folder(start_date, end_date, query)

    search_criteria = {
        "t0": start_date,
        "t1": end_date,
        "q": query,
        "m": "0"
    }
    
    if not os.path.exists(DOWNLOAD_PATH):
        os.makedirs(DOWNLOAD_PATH)

    if reset_metadata:
        metadata_file = os.path.join(DOWNLOAD_PATH, 'metadata.json')
        if os.path.exists(metadata_file):
            os.remove(metadata_file)
            messagebox.showinfo("Metadata Reset", "Metadata file has been reset.")

    id_list = None
    if use_id_list:
        with open(ID_SOURCE, "r") as f:
            id_list = f.read().splitlines()

    finder = DocumentFinder(BASE_URL, HEADERS, search_criteria, id_list, use_id_list)
    downloader = PDFDownloader(new_file_path, BASE_URL)

    documents = finder.find_documents()
    filtered_documents = finder.apply_filters(documents, filter_query or "")
    found = len(filtered_documents)

    for link, metadata in filtered_documents:
        downloader.download_pdf(link, metadata)

    metadata_file = os.path.join(DOWNLOAD_PATH, 'metadata.json')
    downloader.save_metadata(metadata_file)
    
    messagebox.showinfo("Success", f"{found} PDFs downloaded successfully!")

def on_download_click():
    start_date = start_date_entry.get()
    end_date = end_date_entry.get()
    query = query_entry.get()
    filter_query = filter_entry.get()
    use_id_list = use_id_var.get()
    reset_metadata = reset_metadata_var.get()

    if not start_date or not end_date or not query:
        messagebox.showwarning("Input Error", "Please fill in all required fields.")
        return

    download_pdfs(start_date, end_date, query, filter_query, use_id_list, reset_metadata)

def get_default_dates():
    end_date = datetime.today()
    start_date = end_date - timedelta(days=30)
    return start_date.strftime('%Y%m%d'), end_date.strftime('%Y%m%d')

# Initialize default dates
default_start_date, default_end_date = get_default_dates()

app = tk.Tk()
app.title("PDF Downloader")

tk.Label(app, text="Start Date (YYYYMMDD)").grid(row=0, column=0, padx=10, pady=10)
start_date_entry = tk.Entry(app)
start_date_entry.insert(0, default_start_date)
start_date_entry.grid(row=0, column=1, padx=10, pady=10)

tk.Label(app, text="End Date (YYYYMMDD)").grid(row=1, column=0, padx=10, pady=10)
end_date_entry = tk.Entry(app)
end_date_entry.insert(0, default_end_date)
end_date_entry.grid(row=1, column=1, padx=10, pady=10)

tk.Label(app, text="Primary Query").grid(row=2, column=0, padx=10, pady=10)
query_entry = tk.Entry(app)
query_entry.insert(0, "異動")  # Set default query to "異動"
query_entry.grid(row=2, column=1, padx=10, pady=10)

tk.Label(app, text="Filter Query (Optional)\nUse '+' as AND and ',' as OR ").grid(row=3, column=0, padx=10, pady=10)
filter_entry = tk.Entry(app)
filter_entry.insert(0, "役員, 取締, 執行, 人事異動")  # Set default filter query options
filter_entry.grid(row=3, column=1, padx=10, pady=10)

use_id_var = tk.BooleanVar()
use_id_checkbox = tk.Checkbutton(app, text="Use ID List", variable=use_id_var)
use_id_checkbox.grid(row=4, column=0, columnspan=2, pady=10)

reset_metadata_var = tk.BooleanVar()
reset_metadata_checkbox = tk.Checkbutton(app, text="Reset Metadata", variable=reset_metadata_var)
reset_metadata_checkbox.grid(row=5, column=0, columnspan=2, pady=10)

download_button = tk.Button(app, text="Download PDFs", command=on_download_click)
download_button.grid(row=6, column=0, columnspan=2, pady=20)

app.mainloop()
