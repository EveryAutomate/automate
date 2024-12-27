from typing import Dict, List, Optional, Union
import json
import time
import requests
from dataclasses import dataclass
from firebase_admin import firestore
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
            if not doc.exists:
                return []
            data = doc.to_dict()
            data['step_id'] = doc.id
            return [data]
        
        if filters:
            query = collection_ref
            for field, operator, value in filters:
                query = query.where(filter=FieldFilter(field, operator, value))
            docs = query.stream()
        else:
            docs = collection_ref.stream()
            
        result = []
        for doc in docs:
            data = doc.to_dict()
            data['step_id'] = doc.id
            result.append(data)
            
        result.sort(key=lambda x: int(x['step_id'].replace('s', '')))
            
        return result

    def write(self, collection: str, data: Union[Dict, List[Dict], str]) -> str:
        """
        Write data to Firestore. Handles both string and dictionary inputs.
        Returns "true" string to maintain compatibility with existing code.
        """
        collection_ref = self.db.collection(collection)
        batch = self.db.batch()
        
        if isinstance(data, str):
            data = {
                "document_tag": f"output_{int(time.time())}",
                "contents": data
            }
        
        documents = [data] if not isinstance(data, list) else data
        document_tags_seen = set()
        
        for document in documents:
            if isinstance(document, str):
                document = {
                    "document_tag": f"output_{int(time.time())}",
                    "contents": document
                }
            
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
                
            batch.set(doc_ref, {"contents": contents})
        
        batch.commit()
        return "true"  # Return string "true" instead of dictionary
        
        # batch.commit()
        # return {"success": True, "written_tags": written_tags}

    def send(self, config: Dict, payload: Dict, api_key: Optional[str] = None) -> Dict:
        """
        Send request to Gemini API with proper formatting
        """
        url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent"
        
        # Add API key to URL
        if api_key:
            url = f"{url}?key={api_key}"
            
        # Format payload for Gemini API
        formatted_payload = {
            "contents": [{
                "parts": [{
                    "text": payload["prompt"]
                }]
            }]
        }
        
        response = requests.post(
            url=url,
            headers={"Content-Type": "application/json"},
            json=formatted_payload
        )
        
        if response.status_code >= 400:
            raise Exception(f"External service error: {response.text}")
            
        return response.json()

    def extract_gemini_response(self, response: Dict) -> str:
        """
        Extract text content from Gemini API response.
        """
        try:
            candidates = response.get('candidates', [])
            if not candidates:
                return ""
            
            content = candidates[0].get('content', {})
            parts = content.get('parts', [])
            if not parts:
                return ""
            
            return parts[0].get('text', "")
        except Exception as e:
            raise ValueError(f"Failed to parse Gemini response: {str(e)}")

    def manipulate(self, input_1: str, input_2: str, method: str) -> str:
        if method == "concatenate":
            return f"{input_1}\n{input_2}"
        raise ValueError(f"Unknown manipulation method: {method}")

    def execute_scenario(self, scenario_name: str, initial_input: str) -> Dict:
        steps = self.read(collection='scenarios')
        if not steps:
            raise ValueError("No steps found in 'scenarios' collection")
            
        self.cache = {
            "input": initial_input,
            "params": initial_input,
            "true": "true"  # Add this to ensure 'true' is always in cache
        }
        
        for step_data in steps:
            try:
                # Clean and parse kwargs
                kwargs_str = step_data['kwargs'].replace("'", '"').replace('\n', '').strip()
                kwargs = json.loads(kwargs_str)
                
                step = Step(
                    actor=step_data['actor'],
                    action=step_data['action'],
                    kwargs=kwargs,
                    output_name=step_data.get('output_name')
                )
                
                if step.action == "reads":
                    result = self.read(**step.kwargs)
                    if result and len(result) > 0:
                        result = result[0].get('contents', '')
                
                elif step.action == "manipulates":
                    input_1 = self.cache.get(kwargs["input_1"])
                    if input_1 is None:
                        raise ValueError(f"Missing input_1: {kwargs['input_1']}")
                    
                    input_2 = self.cache.get(kwargs["input_2"])
                    if input_2 is None:
                        raise ValueError(f"Missing input_2: {kwargs['input_2']}")
                    
                    result = self.manipulate(input_1, input_2, kwargs["method"])
                    
                    # Store in cache if output specified
                    if "output" in kwargs:
                        self.cache[kwargs["output"]] = result
                        
                elif step.action == "sends":
                    payload_key = kwargs.get("payload")
                    if not payload_key:
                        raise ValueError("Missing payload key in kwargs")
                        
                    payload = self.cache.get(payload_key)
                    if payload is None:
                        raise ValueError(f"Missing payload in cache: {payload_key}")
                    
                    api_response = self.send(
                        config={},
                        payload={"prompt": payload},
                        api_key=kwargs.get("access_token")
                    )
                    
                    result = self.extract_gemini_response(api_response)
                    
                    # Store in cache if output specified
                    if "output" in kwargs:
                        self.cache[kwargs["output"]] = result
                    
                elif step.action == "write":
                    write_kwargs = {}
                    for key, value in kwargs.items():
                        if isinstance(value, str) and value in self.cache:
                            write_kwargs[key] = self.cache[value]
                        else:
                            write_kwargs[key] = value
                    
                    result = self.write(**write_kwargs)
                    # Store write result
                    if "output" in kwargs:
                        self.cache[kwargs["output"]] = result
                    self.cache["true"] = result  # Always store write result as "true"
                else:
                    raise ValueError(f"Unknown action: {step.action}")
                    
                # Store result based on output_name
                if step.output_name:
                    self.cache[step.output_name] = result
                    
                # Update the final output
                if "output" in step.kwargs:
                    self.cache["output"] = self.cache[step.kwargs["output"]]
                    
            except Exception as e:
                raise Exception(f"Error in step {step_data['step_id']}: {str(e)}")
        
        # Ensure "true" is in the final output
        if "true" not in self.cache:
            self.cache["true"] = "true"
                
        return self.cache