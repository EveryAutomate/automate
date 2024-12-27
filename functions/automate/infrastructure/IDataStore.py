from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Union

# Interfaces
class DataStore(ABC):
    @abstractmethod
    def read(self, collection: str, identifier: Optional[str] = None, filters: Optional[List] = None) -> List[Dict]:
        pass
    
    @abstractmethod
    def write(self, collection: str, data: Union[Dict, List[Dict]]) -> None:
        pass
    
    @abstractmethod
    def update(self, collection: str, identifier: str, updates: Dict) -> None:
        pass
    
    @abstractmethod
    def delete(self, collection: str, identifier: Optional[str] = None, field: Optional[str] = None) -> None:
        pass
