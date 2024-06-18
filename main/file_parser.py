from abc import ABC, abstractmethod

class file_parser(ABC):
    @abstractmethod
    def parse(self, file_path):
        pass
