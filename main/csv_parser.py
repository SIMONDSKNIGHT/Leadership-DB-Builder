import csv
from file_parser import FileParser

class CSVParser(FileParser):
    def parse(self, file_path):
        positions = []
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                position = row.get('position')
                if position:
                    positions.append(position)
        return positions