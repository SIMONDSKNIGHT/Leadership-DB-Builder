import os
import zipfile
import pandas as pd
import shutil
from lxml import etree

class XBRLParser:
    def __init__(self, xbrl_zip_path):
        self.xbrl_zip_path = xbrl_zip_path
        self.df = pd.DataFrame(columns=['Job Title', 'Name', 'DOB', 'Work History', 'Footnotes'])
        self.ns = {
            'xbrli': 'http://www.xbrl.org/2003/instance',
            'jpcrp_cor': 'http://www.xbrl.org/jpcrp_cor/2012-12-01',
            # Add other necessary namespaces here
        }
        self.parse(xbrl_zip_path)
        
    def parse(self, file_path):
        try:
            zip_dir = os.path.dirname(self.xbrl_zip_path)
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                zip_ref.extractall(zip_dir)
            
            new_dir = os.path.join(zip_dir, 'XBRL_TO_CSV')
            xbrl_file = None
            for file in os.listdir(new_dir):
                if file.endswith('.xbrl') or file.endswith('.xml'):
                    xbrl_file = os.path.join(new_dir, file)
                    break
            
            if not xbrl_file:
                print(f"No XBRL file found in the archive {file_path}")
                return
            
            tree = etree.parse(xbrl_file)
            root = tree.getroot()

            # Find relevant elements using XPath
            context_elements = root.xpath('.//jpcrp_cor:OfficialTitleOrPositionInformationAboutDirectorsAndCorporateAuditors', namespaces=self.ns)
            
            if not context_elements:
                print(f"Document {file_path} does not contain the required elements")
                return

            for context in context_elements:
                job_title = context.find('.//jpcrp_cor:Title', namespaces=self.ns).text if context.find('.//jpcrp_cor:Title', namespaces=self.ns) else ""
                name = context.find('.//jpcrp_cor:Name', namespaces=self.ns).text if context.find('.//jpcrp_cor:Name', namespaces=self.ns) else ""
                dob = context.find('.//jpcrp_cor:DOB', namespaces=self.ns).text if context.find('.//jpcrp_cor:DOB', namespaces=self.ns) else ""
                work_history = context.find('.//jpcrp_cor:WorkHistory', namespaces=self.ns).text if context.find('.//jpcrp_cor:WorkHistory', namespaces=self.ns) else ""
                footnotes = context.find('.//jpcrp_cor:Footnotes', namespaces=self.ns).text if context.find('.//jpcrp_cor:Footnotes', namespaces=self.ns) else ""

                self.df = self.df._append({
                    'Job Title': job_title,
                    'Name': name,
                    'DOB': dob,
                    'Work History': work_history,
                    'Footnotes': footnotes
                }, ignore_index=True)
            
            # Example of extracting footnotes separately
            footnote_elements = root.xpath('.//jpcrp_cor:FootnotesDirectorsAndCorporateAuditorsTextBlock', namespaces=self.ns)
            footnotes = " ".join([el.text for el in footnote_elements if el.text])

            self.df['Company Footnotes'] = footnotes

            # Clean up extracted files
            shutil.rmtree(new_dir)
        except Exception as e:
            print(f"Error parsing XBRL file: {file_path} ", e)
            shutil.rmtree(new_dir)

    def get_df(self):
        return self.df

if __name__ == '__main__':
    parser = XBRLParser('files/1332/S100R66L/S100R66L.zip')
    df = parser.get_df()
    print(df)