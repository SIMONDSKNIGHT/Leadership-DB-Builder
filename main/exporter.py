from abc import ABC, abstractmethod
class Exporter(ABC):
    def __init__(self, database):
        self.database = database



