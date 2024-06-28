import pandas as pd

def process_csv(file_path):
    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(file_path)

    # Perform your desired processing on the DataFrame
    # For example, you can print the first few rows
    print(df.head())
    # Count the unique TSE values
    unique_tse_count = df['TSE:'].nunique()

    print("Unique index count:", unique_tse_count)
    file_ids= pd.read_csv('file_ids.csv')
    #compare unique tse values and file_ids tse values to find which ids are missing
    #make the first column of file_ids just all numbers past the 4th character
    file_ids['File ID'] = file_ids['File ID'].str[4:]
    #find the missing ids

   

    df['TSE:'] = df['TSE:'].astype(str)
    missing_ids = set(file_ids['File ID']).difference(set(df['TSE:']))
    print(missing_ids)


    

    


    #go through the CSV and for each row, check the value of the column company footnotes for the substring '社外取締役' and return the TSE value for those that do not
    #contain the substring

    #add this column to the df
    df['contains 社外取締役'] = df['Company Footnotes'].str.contains('社外取締役')
    #return all rows where contains 社外取締役 is False

    print(df['contains 社外取締役'].dtype)
    #turn into bool
    df['contains 社外取締役'] = df['contains 社外取締役'].astype(bool)


    #print the TSE values for the rows where the column contains 社外取締役 is False
    print(df[~df['contains 社外取締役']]['TSE:'].unique())

    


process_csv('sumdf.csv')
