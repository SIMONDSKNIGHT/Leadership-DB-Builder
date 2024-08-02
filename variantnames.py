import pandas as pd

# Specify the file path
file_path = 'companyinfo2.csv'

# Read the CSV file into a pandas DataFrame
df = pd.read_csv(file_path)

# Create a new DataFrame with the desired columns
df2 = pd.DataFrame(columns=['TSE number', 'names'])

for SIC in df['Securities Identification Code'].unique():
    # Filter the rows for the current Securities Identification Code (SIC)
    rows = df[df['Securities Identification Code'] == SIC]
    # Get the relevant name columns
    n1 = rows['Submitter Name'].dropna().unique()
    n2 = rows['Submitter Name（alphabetic）'].dropna().unique()
    n3 = rows['Submitter Name（phonetic）'].dropna().unique()
    # Combine the names using a pipe '|' as a delimiter
    names = ' | '.join([name for sublist in [n1, n2, n3] for name in sublist])
    # Add to the new DataFrame
    df2 = df2._append({'TSE number': SIC, 'names': names}, ignore_index=True)

# Save the DataFrame to a CSV file
df2.to_csv('variantnames.csv', index=False)