from abc import ABC, abstractmethod

class FileParser(ABC):
    @abstractmethod
    def parse(self):
        pass
    @abstractmethod
    def find_leadership(self):
        pass
