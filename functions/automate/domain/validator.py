from typing import Dict, Any, Type, Optional
from datetime import datetime

# simplify using getattr
class Validator:
    """Handles validation logic for attributes."""
    
    def __init__(self, invariants: Dict[str, Dict[str, Any]]):

        self.invariants = invariants

    def validate(self, attribute_name: str, value: Any) -> None:
        # Check if attribute exists in invariants
        if attribute_name not in self.invariants:
            raise ValueError(f"Attribute '{attribute_name}' not defined.")
        
        # Get attribute configuration
        attr_config = self.invariants[attribute_name]
        
        # Validate type
        expected_type = self._get_expected_type(attr_config.get("dtype"))
        if expected_type and not isinstance(value, expected_type):
            raise TypeError(f"Expected '{expected_type.__name__}' for '{attribute_name}', got '{type(value).__name__}'.")
        
        # Validate other constraints
        self._validate_constraints(attr_config, value)

    def _get_expected_type(self, dtype: Optional[str]) -> Optional[Type]:
        type_map = {
            "string": str,
            "integer": int,
            "float": float,
            "boolean": bool,
            "datetime": str  # datetime strings stored as str for flexibility
        }
        return type_map.get(dtype)

    def _validate_constraints(self, config: Dict[str, Any], value: Any) -> None:
        constraints = config.get("constraints", {})
        
        # Check minimum value constraint
        if "min" in constraints and value < constraints["min"]:
            raise ValueError(f"Value '{value}' is less than the minimum allowed.")
        
        # Check maximum value constraint
        if "max" in constraints and value > constraints["max"]:
            raise ValueError(f"Value '{value}' exceeds the maximum allowed.")