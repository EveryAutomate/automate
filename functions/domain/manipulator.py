"""
DataManipulator Module
----------------------

This module provides the `DataManipulator` class, which enables flexible data manipulation through processes defined in configurations. It supports numeric, string, datetime, and collection operations.
"""

from typing import Dict, List, Any, Optional, Iterable
from datetime import datetime
import numpy as np
from functools import reduce
from dateutil.relativedelta import relativedelta, MO, TU, WE, TH, FR, SA, SU

class DataManipulator:
    """
    A class for performing various data manipulation tasks, including numeric, string, datetime, and collection operations.

    Attributes:
        cache (Dict[str, Any]): A dictionary used for storing intermediate results of operations.
    """

    def __init__(self):
        """
        Initializes a new instance of the `DataManipulator` class.
        """
        self.cache: Dict[str, Any] = {}
        
    def execute_process(self, process_config: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Executes a series of steps defined in a process configuration.

        Args:
            process_config (List[Dict[str, Any]]): A list of steps, each defining an action, parameters, and optionally an output key.

        Returns:
            Dict[str, Any]: A dictionary containing the cached results of the executed steps.
        """
        for step in process_config:
            result = self.process(
                action=step['action'],
                params={
                    'type': step['type'],
                    'values': self._resolve_values(step.get('params', [])),
                    **step.get('additional_params', {})
                },
                output_key=step.get('output_key')
            )
        return self.cache

    def _resolve_values(self, values: List[Any]) -> List[Any]:
        """
        Resolves values, replacing references to cached values with their actual values.

        Args:
            values (List[Any]): A list of values, some of which may reference cached values using the `$` prefix.

        Returns:
            List[Any]: A list of resolved values.

        Raises:
            ValueError: If a referenced cached value does not exist.
        """
        resolved = []
        for value in values:
            if isinstance(value, str) and value.startswith('$'):
                key = value[1:]
                if key not in self.cache:
                    raise ValueError(f"No cached value found for: {key}")
                resolved.append(self.cache[key])
            else:
                resolved.append(value)
        return resolved

    def process(self, action: str, params: Dict, output_key: Optional[str] = None) -> Any:
        """
        Performs a specific action based on the given parameters.

        Args:
            action (str): The action to perform.
            params (Dict): Parameters required for the action.
            output_key (Optional[str]): An optional key to store the result in the cache.

        Returns:
            Any: The result of the action.

        Raises:
            ValueError: If an unsupported type is specified.
        """
        action_type = params.get('type')
        values = params.get('values', [])
        if action_type == 'numeric':
            result = self._handle_numeric(action, values)
        elif action_type == 'string':
            result = self._handle_string(action, params)
        elif action_type == 'datetime':
            result = self._handle_datetime(params)
        elif action_type == 'collection':
            result = self._handle_collection(action, params)
        else:
            raise ValueError(f"Unsupported type: {action_type}")
            
        if output_key:
            self.cache[output_key] = result
            
        return result

    def _handle_numeric(self, action: str, values: List) -> Any:
        """
        Handles numeric actions using NumPy operations.

        Args:
            action (str): The numeric action to perform (e.g., 'add', 'mean').
            values (List): A list of numeric values.

        Returns:
            Any: The result of the numeric operation.

        Raises:
            ValueError: If a value is not numeric or if the action is invalid.
        """
        # Ensure all values are numeric
        numeric_values = []
        for v in values:
            if isinstance(v, (int, float)):
                numeric_values.append(v)
            else:
                try:
                    numeric_values.append(float(v))  # Attempt to convert to float
                except ValueError:
                    raise ValueError(f"Value '{v}' is not numeric and cannot be processed")
        
        # Convert to numpy array
        arr = np.array(numeric_values)
        
        # Define custom handlers for specific operations
        custom_handlers = {
            'multiply': lambda x: np.prod(x),
            'add': lambda x: np.sum(x),
            'sum': lambda x: np.sum(x),
            'mean': lambda x: np.mean(x),
            'median': lambda x: np.median(x),
            'std': lambda x: np.std(x),
            'min': lambda x: np.min(x),
            'max': lambda x: np.max(x),
            'abs': lambda x: np.abs(x),
            'round': lambda x: np.round(x),
        }
        
        if action in custom_handlers:
            return custom_handlers[action](arr)
        
        # For other NumPy functions, attempt to use them directly
        try:
            return getattr(np, action)(arr)
        except AttributeError:
            raise ValueError(f"Invalid NumPy action: {action}")
        except TypeError as e:
            raise ValueError(f"NumPy action '{action}' failed: {str(e)}")

    def _handle_string(self, action: str, params: Dict) -> str:
        """
        Handles string actions.

        Args:
            action (str): The string action to perform (e.g., 'concatenate').
            params (Dict): Parameters for the string action.

        Returns:
            str: The result of the string operation.

        Raises:
            ValueError: If invalid values are provided.
            AttributeError: If an unsupported string action is specified.
        """
        values = params.get('values', [])
        
        # Flatten a single nested list
        if isinstance(values, list) and len(values) == 1 and isinstance(values[0], list):
            values = values[0]
        
        # Determine the string to process
        if isinstance(values, list):
            if len(values) == 1 and isinstance(values[0], str):
                value_to_process = values[0]
            elif len(values) == 2 and isinstance(values[1], str):
                value_to_process = values[1]
            else:
                raise ValueError("String actions require a single string or a list containing a single string value")
        else:
            raise ValueError("`values` must be a list")

        # Resolve cached arguments if referenced
        args = [
            self.cache[arg] if isinstance(arg, str) and arg in self.cache else arg
            for arg in params.get('args', [])
        ]

        # Handle actions
        if action == "concatenate":
            return "".join(str(v) for v in values)
        
        # Safeguard for unsupported string actions
        if hasattr(value_to_process, action) and callable(getattr(value_to_process, action)):
            return getattr(value_to_process, action)(*args)
        else:
            raise AttributeError(f"Unsupported action: {action} for string")


    def _handle_datetime(self, params: Dict) -> datetime:
        """
        Handles datetime operations.

        Args:
            params (Dict): Parameters for datetime operations, including `base_date`, `weekday_name`, and `kwargs`.

        Returns:
            datetime: The resulting datetime after applying the operation.
        """
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
        """
        Handles collection operations such as filter, map, and reduce.

        Args:
            action (str): The collection operation to perform.
            params (Dict): Parameters for the collection operation.

        Returns:
            Any: The result of the collection operation.

        Raises:
            ValueError: If an unsupported operation or invalid collection is provided.
        """
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
            "unique": lambda: list(set(collection)),
            "flatten": lambda self, lst: (el for el in lst for el in (self._flatten(el) if isinstance(el, Iterable) and not isinstance(el, (str, bytes)) else [el]))

        }

        if action not in operations:
            raise ValueError(f"Unsupported collection operation: {action}")
           
        return operations[action]()
