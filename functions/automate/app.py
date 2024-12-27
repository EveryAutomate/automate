from typing import Dict, List, Optional, Union
import json
import requests
from dataclasses import dataclass
from google.cloud import firestore
from google.cloud.firestore import FieldFilter

@dataclass
class Step:
    actor: str
    action: str
    kwargs: Dict
    output_name: Optional[str] = None

class ScenarioExecutor:
    def __init__(self, db):
        self.db = db
        self.cache = {}  # Store intermediate outputs
        
    def read(self, collection: str, identifier: Optional[str] = None, filters: Optional[List] = None) -> List[Dict]:
        collection_ref = self.db.collection(collection)
        
        if identifier:
            doc = collection_ref.document(identifier).get()
            return [doc.to_dict()] if doc.exists else []
        
        if filters:
            query = collection_ref
            for field, operator, value in filters:
                query = query.where(filter=FieldFilter(field, operator, value))
            docs = query.stream()
        else:
            docs = collection_ref.stream()
            
        return [doc.to_dict() for doc in docs]

    def write(self, collection: str, data: Union[Dict, List[Dict]]) -> None:
        collection_ref = self.db.collection(collection)
        batch = self.db.batch()
        
        documents = [data] if isinstance(data, dict) else data
        document_tags_seen = set()
        
        for document in documents:
            document_tag = document.get("document_tag")
            contents = document.get("contents")
            
            if not document_tag or not contents:
                raise ValueError("Each document must have 'document_tag' and 'contents'.")
                
            if document_tag in document_tags_seen:
                raise ValueError(f"Duplicate document tag '{document_tag}'")
                
            document_tags_seen.add(document_tag)
            doc_ref = collection_ref.document(document_tag)
            
            if doc_ref.get().exists:
                raise ValueError(f"Document '{document_tag}' already exists")
                
            batch.set(doc_ref, contents)
            
        batch.commit()
        return True

    def send(self, config: Dict, payload: Dict, api_key: Optional[str] = None) -> Dict:
        if api_key:
            config = json.loads(json.dumps(config).replace("{api_key_here}", api_key))
            
        response = requests.post(
            url=config["url"],
            headers=config.get("headers"),
            json=payload
        )
        
        if response.status_code >= 400:
            raise Exception(f"External service error: {response.text}")
            
        return response.json()

    def manipulate(self, input_1: str, input_2: str, method: str) -> str:
        if method == "concatenate":
            return f"{input_1}\n{input_2}"
        # Add other manipulation methods as needed
        raise ValueError(f"Unknown manipulation method: {method}")

    def execute_scenario(self, scenario_name: str, initial_input: str) -> Dict:
        # Read scenario steps
        scenario_steps = self.read("scenarios", identifier=scenario_name)
        if not scenario_steps:
            raise ValueError(f"Scenario '{scenario_name}' not found")
            
        # Parse steps into Step objects
        steps = []
        for step_data in scenario_steps:
            steps.append(Step(**step_data))
            
        # Initialize cache with input
        self.cache = {"input": initial_input}
        
        # Execute each step
        for step in steps:
            kwargs = json.loads(step.kwargs.replace("'", '"'))
            
            if step.action == "reads":
                result = self.read(**kwargs)
            elif step.action == "manipulates":
                result = self.manipulate(
                    self.cache[kwargs["input_1"]], 
                    self.cache[kwargs["input_2"]], 
                    kwargs["method"]
                )
            elif step.action == "sends":
                config = {
                    "url": f"https://api.gemini.com/v1/generate",
                    "headers": {"Authorization": f"Bearer {kwargs['access_token']}"}
                }
                result = self.send(config, {"prompt": self.cache[kwargs["payload"]]})
            elif step.action == "write":
                result = self.write(**kwargs)
            else:
                raise ValueError(f"Unknown action: {step.action}")
                
            if step.output_name:
                self.cache[step.output_name] = result
                
        return self.cache

# Example usage
def run_content_generation(input_text: str):
    db = firestore.Client()
    executor = ScenarioExecutor(db)
    result = executor.execute_scenario("content_gen", input_text)
    return result["true"]  # Returns final output


input_text = """
Our: {app} 
helps: {business professionals} 
who want to: {improve or build a business} 
by : {avoid making stuff nobody wants} 
and : {creating clear indicators to measure progress}
"""

result = run_content_generation(input_text)
print(result)