from typing import Dict, Optional
import json
import requests
from tenacity import retry, wait_exponential, stop_after_attempt
from abc import ABC, abstractmethod

class MessageBroker(ABC):

    @abstractmethod
    def publish(self, config: Dict, payload: Dict, api_key: Optional[str] = None) -> Dict:
        """Abstract method to publish messages."""
        pass

class HTTPMessageBroker(MessageBroker):
    
    @retry(wait=wait_exponential(min=1, max=10), stop=stop_after_attempt(3))
    def publish(self, config: Dict, payload: Dict, api_key: Optional[str] = None) -> Dict:
       
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