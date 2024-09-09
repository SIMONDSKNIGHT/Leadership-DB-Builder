# Create the formatted output similar to the provided Excel image layout
import pandas as pd
output_data = []
with open ('TEST_CSV_UTIL_6.csv', 'r') as f:
    df = pd.read_csv(f)
# Iterate over each row and create the formatted structure for each individual
for index, row in df.iterrows():
    # Adding the Name in larger font style area (like row 4)
    output_data.append(['', '', '', f"{row['Name']}", '', '', f"{row['Company Name']}", '', ''])

    
    # Adding the current and previous titles and companies (rows 6 and 7)
    output_data.append(['', '', '', 'Current Title:', f"{row['Job Title']}", '', 'Company:', f"{row['Company Name']}"])
    previous_title = row['role']
    if previous_title == 'nan' or previous_title == None:
        previous_title = row['Last Company Name']
    if previous_title == 'nan':
        print('No previous title found')
        
    tse_val = str(row['Last Company TSE'])[:4]
    output_data.append(['', '', '', 'Previous Title:', previous_title, '', 'Company:', tse_val])
    
    # Placeholder for "Blurb" section (row 9)
    output_data.append(['', '', '', '', '', '', '', f"Start: {row['Year Started At Last Company']}", f"End: {row['Year Joined']}"])

    output_data.append(['', '', '', 'Blurb', '', '', '', '', '', ''])

    # Add empty row as spacing
    output_data.append([])  

# Convert the output_data to a DataFrame for easier CSV export
formatted_df = pd.DataFrame(output_data)

# Save the formatted output to a CSV file
output_csv_path = 'formatted_baseball_card_layout_fixed.csv'
formatted_df.to_csv(output_csv_path, index=False, header=False)

output_csv_path