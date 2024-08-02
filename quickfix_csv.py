import pandas as pd

# Read the CSV file with the correct encoding
input_file = 'companyinfo_processed.csv'
output_file = 'companyinfo2.csv'

df = pd.read_csv(input_file)
df.head()
#drop columns that are not listed companies in the column 'Listed company / Unlisted company'
df = df[df['Listed company / Unlisted company'] == 'Listed company']
df.to_csv(output_file, index=False)
