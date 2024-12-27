from typing import Dict, List, Set
from ..infrastructure.IDataStore import DataStore

class ServiceConfig:
    """
    Manages service configuration and domain object loading from a data store.
    Handles service configuration retrieval and building validation invariants.
    """

    def __init__(self, data_store: DataStore):
        self.data_store = data_store
        
    def load(self, service_name: str) -> Dict:
        config = self.data_store.read('services', service_name)
        if not config:
            raise ValueError(f"Service '{service_name}' not found")
        return config[0]["data"]
    
    def get_domain_objects(self, flow: List[Dict]) -> Dict:
        # Extract unique entity references from flow steps
        domain_objects = {}
        references = {
            step.get("params", {}).get("entity_name") or step.get("params", {}).get("entity")
            for step in flow
            if step.get("params", {}).get("entity_name") or step.get("params", {}).get("entity")
        }
        
        # Load domain object configurations for each reference
        for reference in references:
            config = self.data_store.read('domain_objects', reference)
            if config:
                domain_objects[reference] = config[0]["data"]
                
        return domain_objects

    def _build_invariants(self, domain_objects: Dict) -> Dict:
        """
        Build validation invariants from domain object configurations.
        
        Args:
            domain_objects: Dictionary of domain object configurations,
                          where each configuration contains attribute definitions
                          
        Returns:
            Dictionary mapping attribute names to their validation configurations
            
        Example domain_objects structure:
            {
                "user": {
                    "attributes": {
                        "age": {
                            "dtype": "integer",
                            "constraints": {"min": 0, "max": 120}
                        }
                    }
                }
            }
        """
        invariants = {}
        # Extract attribute configurations from each domain object
        for domain, obj in domain_objects.items():
            for attr, attr_config in obj.get("attributes", {}).items():
                invariants[attr] = attr_config
        return invariants