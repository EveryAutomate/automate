from typing import Dict, List, Any, Optional, Iterable
from datetime import datetime
import numpy as np
from functools import reduce
from dateutil.relativedelta import relativedelta, MO, TU, WE, TH, FR, SA, SU

class DataManipulator:

    def __init__(self):
        self.cache: Dict[str, Any] = {}
       
    def process(self, action: str, params: Dict, output_key: Optional[str] = None) -> Any:
      
       action_type = params.get('type')
       values = params.get('values', [])
       
       # Route to appropriate handler based on action type
       if action_type == 'numeric':
           result = self._handle_numeric(action, values)
       elif action_type == 'string':
           result = self._handle_string(action, params)
       elif action_type == 'datetime':
           result = self._handle_datetime(params)
       elif action_type == 'collection':
           result = self._handle_collection(action, params)
       else:
           raise ValueError(f"Unsupported action type: {action_type}")
           
       # Cache result if output key provided
       if output_key:
           self.cache[output_key] = result
           
       return result

    def _handle_numeric(self, action: str, values: List) -> Any:

       try:
           return getattr(np, action)(*values)
       except AttributeError:
           raise ValueError(f"Invalid NumPy action: {action}")
           
    def _handle_string(self, action: str, params: Dict) -> str:

       values = params.get('values', [])
       if len(values) != 1 or not isinstance(values[0], str):
           raise ValueError("String actions require a single string value")
           
       s = values[0]
       # Resolve cached arguments if referenced
       args = [
           self.cache[arg] if isinstance(arg, str) and arg in self.cache else arg
           for arg in params.get('args', [])
       ]
       return getattr(s, action)(*args)

    def _handle_datetime(self, params: Dict) -> datetime:
   
       base_date = params.get('base_date', datetime.now())
       weekday_name = params.get('weekday_name')
       weekday_offset = params.get('weekday_offset', 0)
       kwargs = params.get('kwargs', {})
       
       # Map weekday names to relativedelta constants
       weekdays = {"MO": MO, "TU": TU, "WE": WE, "TH": TH, "FR": FR, "SA": SA, "SU": SU}
       
       if weekday_name and weekday_name in weekdays:
           kwargs["weekday"] = weekdays[weekday_name](weekday_offset)
           
       return base_date + relativedelta(**kwargs)

    def _handle_collection(self, action: str, params: Dict) -> Any:
  
       collection = params.get('collection', [])
       if isinstance(collection, str):
           collection = self.cache.get(collection.strip('{}'), [])
           
       if not isinstance(collection, list):
           raise ValueError("Collection must be a list")
           
       # Define available collection operations
       operations = {
           "filter": lambda: list(filter(params['predicate'], collection)),
           "map": lambda: list(map(params['transform'], collection)),
           "reduce": lambda: reduce(params['reducer'], collection),
           "sort": lambda: sorted(collection, key=params.get('args', [None])[0]),
           "find": lambda: next((item for item in collection if params.get('predicate')(item)), None),
           "index_of": lambda: collection.index(params['value']) if params.get('value') in collection else -1,
           "flatten": lambda: list(self._flatten(collection)),
           "unique": lambda: list(set(collection))
       }
       
       if action not in operations:
           raise ValueError(f"Unsupported collection operation: {action}")
           
       return operations[action]()

    def _flatten(self, lst: List) -> Iterable:
   
       for el in lst:
           if isinstance(el, Iterable) and not isinstance(el, (str, bytes)):
               yield from self._flatten(el)
           else:
               yield el


    # def handle_libs(self):
    #     pass

    # def translate(self):
    #   pass