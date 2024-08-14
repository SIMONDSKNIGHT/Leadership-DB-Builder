import csv
import pandas as pd

def read_csv(file_path):
    data = {}
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            data[row[0]] = row[1]
    return data

# Replace 'file_path' with the actual path to your CSV file
file_path = 'variantnames.csv'
variant_names = read_csv(file_path)

# Print the variant names


# Read the CSV file as a dataframe
filepath = 'TEST_CSV_UTIL_4.csv'
df = pd.read_csv(filepath)

df['Guess Company Name'] = ''
print(variant_names )
for index, row in df.iterrows():
    #match the Lst Company TSE number with the TSE number in the variantnames.csv

    if row['Last Company TSE'] in variant_names.keys():
        print('yo')
        #if the TSE number is in the variantnames.csv, then the guess company name is the name in the variantnames.csv
        df.at[index, 'Guess Company Name'] = variant_names[row['Last Company TSE']]
df.to_csv('TEST_CSV_UTIL_5.csv', index=False)


    # Perform operations on the dataframe
    # ...

    # Example: Print the first 5 rows of the dataframe
