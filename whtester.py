import csv
#open up a csv, and for each row, print out the first column, but newline on each ';' character
with open('work_histories.csv', 'r') as csvfile:
    reader = csv.reader(csvfile)
    for row in reader:
        print(row[1].replace(';', '\n'))
        #wait for input to proceed
        q=input()
        if q=='q':
            exit()

