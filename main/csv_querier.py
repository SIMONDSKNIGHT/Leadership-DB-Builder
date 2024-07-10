import csv
import pandas as pd

class CSVQuerier:
    def __init__(self, csv_file):
        self.csv_file = csv_file

    def query(self, query_string):
        with open(self.csv_file, 'r') as file:
            df = pd.read_csv(file)
        query = query_string.split(';')
        for i in query:
            split_query = i.split("$")
            if split_query[0]=="columns":
                columns = split_query[1:]
                df = df[columns]
            if split_query[0]=="sort":
                if len(split_query) == 2:
                    df = df.sort_values(by=[split_query[1]])
                else:
                    order = split_query[2]
                    if order == "asc":
                        df = df.sort_values(by=[split_query[1]], ascending=True)
                    if order == "desc":
                        df = df.sort_values(by=[split_query[1]], ascending=False)
            if split_query[0]=="condition":
                column = split_query[1]
                condition = split_query[2]
                value = int(split_query[3])
                if condition == "<":
                    df = df[df[column] < value]
                if condition == ">":
                    df = df[df[column] > value]
                if condition == "=":
                    df = df[df[column] == value]
                if condition == ">=":
                    df = df[df[column] >= value]
                if condition == "<=":
                    df = df[df[column] <= value]
                if condition == "!=":
                    df = df[df[column] != value]
            if split_query[0]=="contains":
                column = split_query[1]
                value = split_query[2]
                modifiers = split_query[3] if len(split_query) > 3 else ""
                if modifiers == "strip":
                    df['temp']    = df[column].replace(' ', '').replace('　', '')
                    df = df[df['temp'].str.contains(value)].drop(columns=['temp'])
                else:

                    df = df[df[column].str.contains(value)]
            if split_query[0]=="boolCondition":
                column = split_query[1]
                true = int(split_query[2])
                bool = true == 1
                
                df = df[df[column] == bool]

        outputdf = df



                
        with open('query.csv', 'w') as file:
            outputdf.to_csv(file, index=False)

