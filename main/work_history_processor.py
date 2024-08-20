from typing import Any
import re
import pandas as pd
import company_identifier as ci
class WorkHistoryProcessor:
    def __init__(self):
        self.text = ''

    def process_work_history(self,text):
        self.text = text
        #remove full width characters
        self.replace_colons()
        self.standardise_dates() 
        
        self.split_text()
        self.reorder_text()   
        #regex every YYYY年MM月 or YYYY年M月

    def split_text(self):

        entries = re.findall(r'\d{4}/\d{1,2}:.*?(?=\d{4}/\d{1,2}:|$)', self.text)
        # Clean up each entry and store it in a list
        self.text= [entry.strip() for entry in entries]
        
    def standardise_dates(self, ):
        # Replace full-width characters with half-width
        text = re.sub(r'[０-９]', lambda x: chr(ord(x.group(0)) - 65248), self.text)
        # Replace Japanese numerals with Arabic numerals
        text = text.translate(str.maketrans('０１２３４５６７８９　', '0123456789 '))
        # text = text.translate(str.maketrans('〇一二三四五六七八九', '0123456789'))　very commedically turned 九州 into 9州

        # Convert Japanese era years to Gregorian years
        era_dict = {
            '昭和': 1925,
            '平成': 1988,
            '令和': 2018
        }
        for era, offset in era_dict.items():
            text = re.sub(f'{era}(\d+)年', lambda x: f'{offset + int(x.group(1))}年', text)

        # Normalize date formats
        text = re.sub(r'(\d{4})年\s*(\d{1,2})月', lambda m: f'{m.group(1)}/{int(m.group(2)):02d}:', text)
        text = re.sub(r'(\d{4})年\s*(\d{1,2})月', lambda m: f'{m.group(1)}/{int(m.group(2)):02d}:', text)
        text = re.sub(r'(\d{4})/(\d{2})', lambda m: f'{m.group(1)}/{int(m.group(2)):02d}:', text)
        
        text = re.sub(r'(\d{4})年度', lambda m: f'{m.group(1)}/01:', text)  # Fix to automatically set month to 01

        
        self.text = text
        self.replace_same_year_month()
        self.loose_months()

    def replace_same_year_month(self):
        full_dates = re.findall(r'\d{4}/\d{2}:', self.text)
        # Replace "同年" with the last found year
        try:
            if full_dates:
                last_year = re.findall(r'\d{4}', full_dates[-1])[0]
                self.text = re.sub(r'同\s*年\s*(\d{1,2})月', lambda m: f'{last_year}/{int(m.group(1)):02d}:', self.text)
                self.text = re.sub(r'同年同月', f'{last_year}/{full_dates[-1][5:7]}:', self.text)
                self.text = re.sub(r'同\s*年\s*同\s*月', f'{last_year}/{full_dates[-1][5:7]}:', self.text)
            # Find all full dates again after replacement
            full_dates = re.findall(r'\d{4}/\d{2}:', self.text)
            
            # Replace "同月" with the last found month
            if full_dates:
                last_year_month = re.findall(r'\d{4}/\d{2}', full_dates[-1])[0]
                self.text = re.sub(r'同月', f'{last_year_month}:', self.text)
        
        except:
            print("######################################################################")
            print(self.text)
            exit()
    def loose_months(self):
        full_dates = re.findall(r'\d{4}/\d{2}:', self.text)
        if full_dates:
            last_year = re.findall(r'\d{4}', full_dates[-1])[0]
            # Replace loose months with the last known year, ignoring cases where the previous character is 年
            self.text = re.sub(r'(?<!年)(?<!\d)(\d{1,2})月', lambda m: f'{last_year}/{int(m.group(1)):02d}:', self.text)
    def replace_colons(self):

        self.text = self.text.replace(':', ',')
        self.text = self.text.replace(';', ',')
    def join_text(self):
        final_string = ''
        for i in self.text:
            final_string += i
            final_string += '; '
        final_string = final_string[:-2]
        self.text = final_string
    def concurrent_roles(self):
        pattern = re.compile(r'\(現\)|（現）|\(現在に至る\)|（現在に至る）|\(現役\)|（現役）|\(現任\)|（現任）|\(兼\)|（兼）|\(兼職\)|（兼職）|\(重要な兼職\)|（重要な兼職）')
        self.text = [pattern.sub('(現)', i) for i in self.text]
        #print if there was an example
        print(self.text)
    

    def get_text(self):
        return self.text


    
    def when_joined(self, company_name):
        #the goal of this one is to simply detect when they joined this company, what their last company was.
        # company_name = '関西電力株式会社'
        # identifier = ci.CompanyIdentifier(company_name)
        # name = '関西電力株式会社社外取締役（現在）'
        # print(identifier.calculate_closest_name(name))
        # exit()

        identifier = ci.CompanyIdentifier(company_name)
 
        
        for t in self.text:
            year =""


            if '当社' in t or '当行' in t or '提出会社' in t or '現職に'in t:
                
                year = t.split('::')[0]
            if year != "":
                return year
            if len(t.split('::')) == 1:
                print('broken text')
                continue
            this_name = t.split('::')[1]
            if identifier.calculate_closest_name(this_name) < 0.3:
                # print(f'Company name: {this_name} is a match with {company_name}')
                
                year = t.split('::')[0]
                return year
                #set yearjoined as the year value
        return 'ERROR'
    def reorder_text(self):
        #reorder the text so that the most recent job is at the top. all items in text are in format yyyy/mm::text
        
        self.text = sorted(self.text, key=lambda x: x.split('::')[0])




    def last_company(self, yyyy_mm):
        #passed a date, get the name of the previous company they worked for and what they did there
        #MAKE SURE TEXT IS SPLIT WHEN DOING THIS
        identifier = ci.CompanyIdentifier()
        
        for i, t in enumerate(self.text):
            
            if len(t.split('::')) == 1:
                print('broken text')
                continue
            this_name = t.split('::')[1]
            lc_val = None
            year = t.split('::')[0]
            last_company = ''
            role = ''
            start_year = ''
            if '同社退任' in self.text[-1]:
                last_company = self.text[-2].split('::')[1]
                return 'NA', 'NA', self.text[-1].split('::')[0]

            if year == yyyy_mm:
                #get the text of the last poisition they held
                if i == 0:
                    
                    return 'CURR', role, start_year
                j=i-1
                while j >= 0:
                   

                    if '同社' in self.text[j]:
                        
                        if role == '':
                            role = self.text[j].split('::')[1]
                        j -= 1
                        #continue loop without leaving while
                        continue
                    try:
                        if last_company == '':
                            last_company = self.text[j].split('::')[1]
                            # print(last_company)
                            
                            identifier.set_company_names(last_company)
                            start_year = self.text[j].split('::')[0]
                            j -= 1
                            continue
                        else:
                            if identifier.calculate_closest_name(self.text[j].split('::')[1]) > 0.3:
                                
                                start_year = self.text[j].split('::')[0]
                                return last_company, role, start_year
                            
                                
                            
                                
                        
                    except:
                        print('error in last company; fetching previous...')
                    j -= 1
                    if j == 0:
                        print('no previous company')


                        return 'ERROR', role, start_year

                return last_company, role, start_year
        return 'ERROR', role, start_year
 ################## DEPRECATED ##################
















    def old_prev_job(self):
        self.sumdf['Last Internal Job'] = ""
        self.sumdf['Last External Job'] = ""
        self.sumdf['Year Joined'] = ""
        self.sumdf['Concurrent Roles'] = ""
        self.sumdf['Recent Job Change'] = ""
        #for each of the non external directors, go through their

        for index, row in self.sumdf.iterrows():
            counter = 0
            
            if row['WH Error']!=1:
                continue
            if row ['External'] == True:
                text = row ['Work History'].split(';')

                output = ''
    
                for i, t in enumerate(text):

                    if '現役'in t or '現任' in t or '兼職' in t or '兼'in t or '現' in t[-4:]or '現在に至る'in t or i == len(text):
                        

                        
                        t.replace('現在に至る', '')
                        t.replace('現役', '')
                        t.replace('重要な兼職', '')
                        t.replace('兼職', '')
                        t.replace('兼', '')
                        t.replace('現任', '')
                        t.replace('現', '')
                        t.replace('（）', '')
                        t.replace('()', '')

                        output += t
                        output+= ', '
                if output == '':
                    if pd.isna(self.sumdf.loc[index, 'error']):
                        self.sumdf.loc[index, 'error'] = ''

                    self.sumdf.loc[index, 'error'] += 'Error no current job found: no most recent job, '
                    # print('exception line 271',row['TSE:'], row['Name'])
                concurrent = output
            if row['External'] == False:

                #first figure out the most recent internal job, then figure out the most recent external job
                text = row['Work History'].split(';')    
                internal = ''
                external = ''
                concurrent = ''
                year_joined = ''
                last_job_change = ''
                joined = False
                for i, t in enumerate(text):
                    if i == len(text) - 1:
                        offset = 1
                        if internal != '':
                            internal += ', '
                        internal =text[i-1]
                        
                        last_job_change = t.split('::')[0]
                        loop_bool = True
                        while loop_bool:
                            if offset == len(text):
                                print(1)
                                print("I am bad at my job")
                                break
                            if '社外' in internal:
                                print(2)
                                offset += 1
                                internal = text[i-offset].split('::')[0]
                                last_job_change = text[i-offset+1].split('::')[0]
                            else:loop_bool = False
                        if last_job_change[4]!='/':
                            print(last_job_change)
                        self.sumdf.loc[index, 'Recent Job Change'] = last_job_change
                        break

                    
                    if '当社入社' in t:
                        year_joined = t.split('::')[0]
                        counter += 1
                        #break off the first section of the text that includes the date
                        if joined == False:
                            if i==0:
                                external = "N/A"
                            else:
                                external = text[i-1]
                        joined = True

                    
                    if '現役'in t or '現任' in t or '兼職' in t or '兼'in t or '現' in t[-4:]or '現在に至る'in t or i == len(text):
                        

                        t.replace('現在に至る', '')
                        t.replace('現役', '')
                        t.replace('重要な兼職', '')
                        t.replace('兼職', '')
                        t.replace('兼', '')
                        t.replace('現任', '')
                        t.replace('現', '')
                        t.replace('（）', '')
                        t.replace('()', '')
                        # if joined:
                        #     if internal != '':
                        #         internal += ', '
                        #     internal += t
                        # else:
                        if concurrent != '':
                            concurrent += ', '
                        concurrent +=t
                    if internal == '':
                        print('error: no internal info', row['TSE:'], row['Name'])
                    if external == '':
                        print('error: no external info', row['TSE:'], row['Name'])
                self.sumdf.loc[index, 'Last Internal Job'] = internal
                self.sumdf.loc[index, 'Last External Job'] = external
                self.sumdf.loc[index, 'Year Joined'] = year_joined
                self.sumdf.loc[index, 'Concurrent Roles'] = concurrent
                self.sumdf.loc[index, 'Recent Job Change'] = last_job_change
                if joined ==False:
                    text = row['Work History'].split(';')    
                    internal = ''
                    external = ''
                    concurrent = ''
                    year_joined = ''
                    last_job_change = ''
                    joined = False
                    for i, t in enumerate(text):
                        if i == len(text) - 1:
                            offset = 1
                            if internal != '':
                                internal += ', '
                            internal =text[i-1]
                            
                            last_job_change = t.split('::')[0]
                            loop_bool = True
                            while loop_bool:
                                if offset == len(text):
                                    print("I am bad at my job")
                                    break
                                if '社外' in internal:
                                    offset += 1
                                    internal = text[i-offset].split('::')[0]
                                    last_job_change = text[i-offset+1].split('::')[0]
                                else:loop_bool = False
                            if last_job_change[4]!='/':
                                print(last_job_change)
                            self.sumdf.loc[index, 'Recent Job Change'] = last_job_change
                            break

                        
                        if '当社' in t and joined == False:
                            year_joined = t.split('::')[0]
                            counter += 1
                            #break off the first section of the text that includes the date
                            if joined == False:
                                if i==0:
                                    external = "N/A"
                                else:
                                    external = text[i-1]
                            joined = True

                        
                        if '現役'in t or '現任' in t or '兼職' in t or '兼'in t or '現' in t[-4:]or '現在に至る'in t or i == len(text):
                            

                            t.replace('現在に至る', '')
                            t.replace('現役', '')
                            t.replace('重要な兼職', '')
                            t.replace('兼職', '')
                            t.replace('兼', '')
                            t.replace('現任', '')
                            t.replace('現', '')
                            t.replace('（）', '')
                            t.replace('()', '')
                            # if joined:
                            #     if internal != '':
                            #         internal += ', '
                            #     internal += t
                            # else:
                            if concurrent != '':
                                concurrent += ', '
                            concurrent +=t
                        if internal == '':
                            print('error: no internal info still unfixed', row['TSE:'], row['Name'])
                        else:
                            print('internal issue fixed')
                        if external == '':
                            print('error: no external info still unfixed', row['TSE:'], row['Name'])
                        else:
                            print('external issue fixed')
                    self.sumdf.loc[index, 'Last Internal Job'] = internal
                    self.sumdf.loc[index, 'Last External Job'] = external
                    self.sumdf.loc[index, 'Year Joined'] = year_joined
                    self.sumdf.loc[index, 'Concurrent Roles'] = concurrent
                    self.sumdf.loc[index, 'Recent Job Change'] = last_job_change
    
                
                

    

        