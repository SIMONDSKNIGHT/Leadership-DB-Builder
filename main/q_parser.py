import csv
import os
import zipfile
import pandas as pd
import shutil
from pdfminer.high_level import extract_text
import rest_api
import pikepdf
import pdfplumber
import fitz
import traceback
import re


class QParser():
    def __init__(self, file_path, pdf = False):
        self.pdf = pdf 
        self.file_path = file_path
        self.quarterdf = pd.DataFrame(columns=['TSE:','document code','type','Period End'])
        self.leavedf = pd.DataFrame(columns=['TSE:','document code'])
        self.movedf = pd.DataFrame(columns=['TSE:','document code'])
        self.joindf = pd.DataFrame(columns=['TSE:','document code'])
    def check_for_changes(self):
        #ensure output is deleted
        try:
            zip_dir = os.path.dirname(self.file_path)
            with zipfile.ZipFile(self.file_path, 'r') as zip_ref:
                zip_ref.extractall(zip_dir)
            #open the csvfile in the folder that starts with 'jpcrp03'
            new_dir = zip_dir+'/XBRL_TO_CSV'
            for file in os.listdir(new_dir):
                if file.startswith('jpcrp04'):
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
            content = ""

            officer_info = True
            for index, row in stock.iterrows():
                
                if 'InformationAboutOfficersTextBlock' in row['要素ID']:
                    infoo = True
                    row_content = row['値']
                    
                    while row_content.endswith(' '):
                        row_content = row_content[:-1]
                    while row_content.endswith('　'):
                        row_content = row_content[:-1]
                    if "該当事項はありません" in row_content:
                        officer_info = False
                    
                    elif (row_content.endswith('役員の異動はありません。') 
                       or row_content.endswith('役員に異動はありません。')
                    ):
                        
                        if len(row_content) < 60:
                            
                            officer_info = False
                            continue
                    # force open the pdf file in the csvfile folder in preview for mac
                    # to see the contents of the csv file, note the directory address is in a variable called csv_file
            os.remove(self.file_path)
            return officer_info

                    
                
            
            
        except Exception as e:
            print(f"Error parsing CSV file: {self.file_path} ", e)
            with open('failed.txt', 'a') as file:
                file.write(f"Error parsing CSV file: {self.file_path} {e}")
            os.remove(self.file_path)
            shutil.rmtree(new_dir)
    def remove_pdf_restrictions(self):
        if not self.pdf:
            print(f"File is not a PDF: {self.file_path}")
            return
        try:
            with pikepdf.open(self.file_path,allow_overwriting_input=True) as pdf:
                pdf.save(self.file_path)
        except Exception as e:
            print(f"Error removing PDF restrictions: {self.file_path} ", e)
            with open('failed.txt', 'a') as file:
                file.write(f"Error removing PDF restrictions: {self.file_path} {e}")
    def extract_tables(self,tseno, docid,date):
        with fitz.open(self.file_path) as pdf:
            toc = pdf.get_toc(simple=False)
            target_chapter = None
            
            # Identify the "役員の状況" chapter
            for i, entry in enumerate(toc):
                level, title, start_page = entry[:3]
                if "役員の状況" in title and isinstance(start_page, int):
                    end_page = toc[i + 1][2] if i + 1 < len(toc) else pdf.page_count
                    target_chapter = {
                        "title": title,
                        "start_page": start_page,
                        "end_page": end_page
                    }
                    break

        if not target_chapter:
            print("The chapter '役員の状況' was not found in the TOC.")
            return

        start_page = target_chapter['start_page']
        end_page = target_chapter['end_page']

        try:

            with pdfplumber.open(self.file_path) as pdf:
                num = 1
                for page_number in range(start_page - 1, end_page):
                    page = pdf.pages[page_number]
                    tables = page.extract_tables()
                    last_table = None
                    for table in tables:
                        if table == last_table:
                            continue
                        last_table = table
                        columns = self.make_unique(table[0])
                        #if there is a column with name 区分, then drop the table

                       
                        if '区分' in columns:
                            continue
                        # #if any column title contains '所有者' then drop the table
                        
                        
                        
                        #if any value in the first non-header row is '区分' then drop the table
                        if '区分' in table[1]:
                            continue
                        
                        if any('所有者' in col for col in columns if col!=None):
                            continue
                        if any ('所有者' in row for row in table[1] if row!=None):
                            continue
                        # Convert the first row of the table to column headers
                        columns = self.column_fixing(columns)


                        
                        
                        if '略歴' in columns and None in columns:
                            
                            temp_table = self.align_and_merge_work_history(table)
                            df = pd.DataFrame(temp_table[1:], columns=columns)
                        else:
                            df = pd.DataFrame(table[1:], columns=columns)
                        
                        # Ensure all columns are present even if not all tables have them
                        if not hasattr(self, 'quarterdf'):
                            self.quarterdf = pd.DataFrame(columns=df.columns)
                        # num = 1
                        # while  os.path.exists(f'TEST_{num}.csv')==True:
                            # num += 1
                        # df.to_csv(f'TEST_{num}.csv', index=False)

                        # Merge the dataframes, adding columns as necessary
                        #if the df is not empty
                        if not df.empty:
                            #add the tse number and document code to the dataframe
                            df['TSE:'] = tseno
                            df['document code'] = docid
                            df['Period End'] = date.strftime('%Y-%m-%d')
                        self.quarterdf = pd.concat([self.quarterdf, df], ignore_index=True)
        except Exception as e:
            print(f"Error extracting tables from PDF: {self.file_path} ", e)
            print(traceback.print_exc())
            if table:
                print(table)
            with open('failed.txt', 'a') as file:
                file.write(f"Error extracting tables from PDF: {self.file_path} {e}")

    def make_unique(self,columns):
        """Ensure ll column names are unique by appending a suffix where needed."""
        seen = {}
        for i, col in enumerate(columns):

            if col in seen:
                
                seen[col] += 1
                columns[i] = f"{col}_{seen[col]}"
            else:
                seen[col] = 0
        return columns

    def align_and_merge_work_history(self, table):
        """Aligns work history data and merges years with history for each row individually, updating the 略歴 column and removing the None column."""
        
        # Assume the first row is the header
        header = table[0]
        
        # Ensure that the "略歴" column exists and there's a column after it
        if '略歴' in header and header.index('略歴') + 1 < len(header):
            # Find the indexes for the "略歴" column and the column after it
            ryakureki_index = header.index('略歴')
            history_index = ryakureki_index + 1
            
            # Process each row individually
            for i, row in enumerate(table[1:]):
                years_column = row[ryakureki_index]
                history_column = row[history_index]

                # Split the years and history into lines
                years_lines = years_column.split("\n") if isinstance(years_column, str) else []
                history_lines = history_column.split("\n") if isinstance(history_column, str) else []

                # If there is a mismatch between the number of years and history lines, realign the history lines
                if len(years_lines) != len(history_lines):
                    print(f"Row {i + 1}: The number of lines in the years and history columns do not match.")
                    if len(history_lines) > len(years_lines):
                        print(f"Row {i + 1}: History lines exceed years lines, attempting realignment.")
                        while len(history_lines) > len(years_lines):
                            history_lines, failed = self.realigner(history_lines)
                            if failed:
                                print(f"Row {i + 1}: Realignment failed.")
                                return table  # Return the original table if realignment fails

                # Combine year and history lines for this row
                combined_lines = [f"{year.strip()} {job.strip()}" for year, job in zip(years_lines, history_lines)]

                # Update the table row with the combined lines in the "略歴" column
                row[ryakureki_index] = "\n".join(combined_lines)

            # Remove the column after "略歴" (which was used for history)
            for row in table:
                row.pop(history_index)

            # Remove any rows where all columns are empty (assuming None or empty strings)
            table = [row for row in table if any(row)]

        else:
            print("No column exists after '略歴' to align and merge.")

        return table                        

    def realigner(self, history_lines):
        """Realign history lines by attaching the row with the lowest score to the line before it."""
        # Calculate the scores for each line
        scores = [self.calculate_score(line) for line in history_lines]

        # Find the index of the row with the lowest score
        min_index = scores.index(min(scores))
        if all(score == float('inf') for score in scores):
            # No lines to realign, all lines have inf scores
            return history_lines, True  # Indicate that no realignment was possible


        # Ensure we can attach the line before it
        if min_index == 0:
            # If it's the first line, we can't attach it to anything before it, so return as is
            return history_lines, True

        # Attach the row with the lowest score to the line before it
        history_lines[min_index - 1] += f"{history_lines[min_index]}"
        print(f"Realigning: {history_lines[min_index]}")
        # Remove the line that was attached
        history_lines.pop(min_index)

        return history_lines, False


    def count_lines(cell):
        """Counts the number of lines in a cell."""
        if isinstance(cell, str):
            return len(cell.split("\n"))
        return 0
    def column_fixing(self,columns):
        #fixes complicated name variants of columns
        if '新役名及び職名' in columns:
            columns[columns.index('新役名及び職名')] = '新役職名'
        if '旧役名及び職名' in columns:
            columns[columns.index('旧役名及び職名')] = '旧役職名'
        return columns
    def extract_info(self):
        #gets the rows from quarterdf and turns them into information that can be used to make a new dataframe
        #types are join, move, leave
        #if nothing in column 氏名, then drop the row
        
        for index, row in self.quarterdf.iterrows():
            row_columns = row.index
            

            # try:
            #     if row['氏名'] == None:
            #         self.quarterdf.drop(index, inplace=True)
            #         continue
            # except:
            #     print(row)
            if '退任年月日'  in row_columns:
                if not pd.isna(row['退任年月日']):
                    
                    self.quarterdf.at[index, 'type'] = 'L'
                    continue
            if '旧役職名' in row_columns:
                if not pd.isna(row['旧役職名']):
                    self.quarterdf.at[index, 'type'] = 'M'
                    continue
            
            self.quarterdf.at[index, 'type'] = 'J'
            
                
        

    def get_df(self):
        #remove columns '任務' if it exists
        #remove any columns that are not TSE:	document code	type	新役職名	旧役職名	氏名	異動年月日	役職名	生年月日	略歴	任期	所有株式数 （株）	退任年月日	所有株式数 （千株）	就任 年月日	役名	職名
        return self.quarterdf
    def get_officerdf(self):
        return self.officerdf
    def formatQ(self):

        return self.different_format


    def is_kanji(self,char):
        """Check if a character is a Kanji."""
        return re.match(r'[\u4e00-\u9fff]', char) is not None
    def output_subdfs(self):
        
        self.leavedf.to_csv('QDF_1.csv', index=False)
        self.movedf.to_csv('QDF_2.csv', index=False)
        self.joindf.to_csv('QDF_3.csv', index=False)

    def calculate_score(self,line):
        """Calculate the score for a line based on the given criteria."""
        if len(line) > 6:
            return float('inf')  # Assign infinity if the line length is greater than 6
        score = 0
        for char in line:
            if self.is_kanji(char):
                score += 1
            else:
                score -= 0.5
        return score



    
    

