import pandas as pd
import csv
# Specify the file path
file_path = 'companyinfo2.csv'

# Read the CSV file into a pandas DataFrame
df = pd.read_csv(file_path)
def fix_names(name):
    extraneous_words = [
        '株式会社', '有限会社', 'CO., LTD.', 'CO.,LTD.', 'Co., Ltd.', 'Co.,Ltd.', 
        'Co., LTD.', 'Co.,LTD.', 'co., ltd.', 'co.,ltd.', 'co., Ltd.', 'co.,Ltd.', 
        '（株）', '㈱', 'カブシキガイシャ', 'コーポレーション', 'corporation', 
        'Corporation', 'CORPORATION', 'INC.', 'Inc.', 'inc.'
    ]
    
    for word in extraneous_words:
        name = name.replace(word, '')
    
    return name.strip()

# Create a new DataFrame with the desired columns
df2 = pd.DataFrame(columns=['TSE number', 'names'])

for SIC in df['Securities Identification Code'].unique():
    # Filter the rows for the current Securities Identification Code (SIC)
    rows = df[df['Securities Identification Code'] == SIC]
    # Get the relevant name columns
    n1 = rows['Submitter Name'].dropna().unique()
    n2 = rows['Submitter Name（alphabetic）'].dropna().unique()
    n3 = rows['Submitter Name（phonetic）'].dropna().unique()
    n1 = [fix_names(name) for name in n1]
    n2 = [fix_names(name) for name in n2]
    n3 = [fix_names(name) for name in n3]
    # Combine the names using a pipe '|' as a delimiter
    names = ' | '.join(n1 + n2 + n3)
    # Add to the new DataFrame
    df2 = df2._append({'TSE number': SIC, 'names': names}, ignore_index=True)

# Save the DataFrame to a CSV file
df2.to_csv('variantnames.csv', index=False,quoting=csv.QUOTE_ALL)