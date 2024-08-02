import csv
import pandas
def get_row_values(listo):
    csv_file = 'test.csv'

    with open (csv_file, 'r') as file:
        #get the columns in listo
        # make into pandas dataframe
        df = pandas.read_csv(file)
        # get the columns with titles that match listo
        # get the values in the row
        # return the values
        #print number of unique tse values
        df.to_csv('panda.csv')
def get_unique(column):
    df = pandas.read_csv('test.csv')
    return df[column].unique()
def reorganise(column_name):
    df = pandas.read_csv('test.csv')
    #get column names
    columns = df.columns
    columns = list(columns)
    #remove column name from columns
    columns.remove(column_name)
    #reorganise the columns
    columns.insert(0,column_name)
    #reorganise the dataframe

    df = df[columns]
    return df


    

if __name__ == '__main__':
    #TSE:,Company Name,Document Title,Submission Date,period end,type,Job Title,Name,DOB,Work History,Footnotes,external,which table?,Company Footnotes,document code

    print(len(get_unique('TSE:')))
    df = reorganise('which table?')
    df.to_csv('test.csv')
        