from typing import Dict, List, Optional, Union, Any
import json
import requests
from dataclasses import dataclass
from google.cloud import firestore
from google.cloud.firestore import FieldFilter
from domain.manipulator import DataManipulator

@dataclass
class Step:
    actor: str
    action: str
    kwargs: Dict
    output_name: Optional[str] = None

class ScenarioExecutor:
    """
    A class to manage and execute database operations, external service interactions,
    and scenario-based workflows.
    """

    def __init__(self, db):
        """
        Initialize the ScenarioExecutor.

        :param db: Firestore database client instance.
        """
        self.db = db
        self.data_manipulator = DataManipulator()
        self.cache = {}  # Store intermediate outputs

    def read(self, collection: str, identifier: Optional[str] = None, filters: Optional[List] = None) -> List[Dict]:
        """
        Read documents from a Firestore collection.

        :param collection: The Firestore collection name.
        :param identifier: Optional document identifier.
        :param filters: Optional list of filters as tuples (field, operator, value).
        :return: List of documents as dictionaries.
        """
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
        """
        Write new documents to a Firestore collection.

        :param collection: The Firestore collection name.
        :param data: A single document or list of documents to write.
        :raises ValueError: If data does not meet the required structure or has duplicate tags.
        """
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

    def update(self, collection: str, identifier: str, updates: Dict) -> None:
        """
        Update an existing Firestore document.

        :param collection: The Firestore collection name.
        :param identifier: Document identifier to update.
        :param updates: Dictionary of updates to apply.
        :raises ValueError: If the document does not exist.
        """
        doc_ref = self.db.collection(collection).document(identifier)
        if not doc_ref.get().exists:
            raise ValueError(f"Document '{identifier}' does not exist")
        doc_ref.update(updates)
        return True

    def delete(self, collection: str, identifier: Optional[str] = None, field: Optional[str] = None) -> None:
        """
        Delete documents or fields from a Firestore collection.

        :param collection: The Firestore collection name.
        :param identifier: Optional document identifier to delete.
        :param field: Optional specific field to delete within a document.
        """
        collection_ref = self.db.collection(collection)

        if identifier and field:
            doc_ref = collection_ref.document(identifier)
            doc_ref.update({field: firestore.DELETE_FIELD})
        elif identifier:
            collection_ref.document(identifier).delete()
        else:
            self._delete_collection(collection_ref)

    def _delete_collection(self, coll_ref, batch_size: int = 10) -> None:
        """
        Helper method to delete an entire Firestore collection in batches.

        :param coll_ref: Firestore collection reference.
        :param batch_size: Number of documents to delete per batch.
        """
        docs = coll_ref.limit(batch_size).stream()
        deleted = 0

        batch = self.db.batch()
        for doc in docs:
            batch.delete(doc.reference)
            deleted += 1

        if deleted >= batch_size:
            self._delete_collection(coll_ref, batch_size)

    def send(
        self,
        service: str,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
        api_key: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Send a request to an external API using stored configuration.

        :param service: Service name in Firestore.
        :param endpoint: Endpoint name in the service configuration.
        :param data: Optional dictionary of data to send with the request.
        :param api_key: Optional API key to include in the request.
        :return: JSON response from the external API.
        :raises ValueError: If configuration or data is invalid.
        :raises requests.exceptions.RequestException: If the API request fails.
        """
        # Retrieve configuration for the service/endpoint
        config_list = self.read(service, identifier=endpoint)
        if not config_list:
            raise ValueError(f"No configuration found for {service}/{endpoint}")

        if not isinstance(config_list, list) or not config_list[0].get('content'):
            raise ValueError(f"Invalid configuration format for {service}/{endpoint}")

        # Replace {api_key} in the configuration string if provided
        config_str = config_list[0]['content']
        if api_key:
            config_str = config_str.replace("{api_key}", api_key)
        config = json.loads(config_str)

        # Prepare parameters and payload
        params = None
        if data and config.get('query_mapping'):
            params = {k: data[v] for k, v in config['query_mapping'].items() if v in data}

        payload = None
        if data and config.get('payload_mapping'):
            payload = {k: data[v] for k, v in config['payload_mapping'].items() if v in data}

            # Gemini-specific adjustments
            if 'contents' in payload:
                payload['contents'] = [{"parts": [{"text": payload['contents']}]}]

        # Log the payload
        import logging
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)
        logger.info("Final payload: %s", json.dumps(payload, indent=2))

        # Make the API request
        response = requests.request(
            method=config['method'],
            url=config['url'],
            headers=config.get('headers', {}),
            params=params,
            json=payload
        )

        # Handle errors
        if response.status_code >= 400:
            logger.error("Error response body: %s", response.text)
            raise requests.exceptions.RequestException(
                f"External service error: {response.status_code} - {response.reason}"
            )

        # Return the JSON response
        return response.json()

    def manipulate(self, method_name: str, inputs: Dict) -> Any:
        """
        Execute a data manipulation process based on stored configuration.

        :param method_name: Name of the manipulation method.
        :param inputs: Input parameters for the process.
        :return: Result of the manipulation process.
        :raises ValueError: If configuration or inputs are invalid.
        """
        # Get process model from firestore
        process_docs = self.read(collection=method_name)
        
        if not process_docs:
            raise ValueError(f"Method {method_name} not found")
        
        # Get the process document
        process_doc = process_docs[0]
        
        # Validate process document structure
        if 'output_key' not in process_doc:
            raise ValueError(f"Process document for {method_name} is missing required 'output_key' field")
            
        # Convert firestore document to process config
        process_config = []
        
        # Check if we have a single step document (without s1, s2, etc.)
        if 'type' in process_doc and 'action' in process_doc and 'params' in process_doc:
            # Single step document - use it directly
            config = {
                'type': process_doc['type'],
                'action': process_doc['action'],
                'params': self._prepare_params(process_doc['params'], inputs),
                'output_key': process_doc.get('output_key')
            }
            if 'additional_params' in process_doc:
                config['additional_params'] = process_doc['additional_params']
            process_config.append(config)
        else:
            # Multiple steps with s1, s2, etc.
            steps = sorted(
                [(k, v) for k, v in process_doc.items() if k.startswith('s')],
                key=lambda x: int(x[0][1:])
            )
            
            if not steps:
                raise ValueError(f"No steps found in process document for {method_name}")
            
            # Build process config
            for _, step in steps:
                # Validate required step fields
                required_fields = {'type', 'action', 'params'}
                missing_fields = required_fields - set(step.keys())
                if missing_fields:
                    raise ValueError(f"Step is missing required fields: {missing_fields}")
                    
                config = {
                    'type': step['type'],
                    'action': step['action'],
                    'params': self._prepare_params(step['params'], inputs),
                    'output_key': step.get('output_key')
                }
                if 'additional_params' in step:
                    config['additional_params'] = step['additional_params']
                process_config.append(config)
        
        # Execute process
        results = self.data_manipulator.execute_process(process_config)
        
        # Return the final output using the specified output key
        output_key = process_doc['output_key']
        if output_key not in results:
            raise ValueError(f"Output key '{output_key}' not found in process results")
            
        return results[output_key]

    def _prepare_params(self, params: List, inputs: Dict) -> List:
        """
        Prepare parameters by resolving references to input values.

        :param params: List of parameter definitions.
        :param inputs: Dictionary of input values.
        :return: List of resolved parameter values.
        :raises ValueError: If required inputs are missing or invalid.
        """
        def get_nested_value(data: Dict, path: str) -> Any:
            """
            Get a value from nested dictionary using dot notation.
            Example: get_nested_value({'inputs': {'artifact': 'value'}}, 'inputs.artifact')
            """
            current = data
            parts = path.split('.')
            
            for part in parts:
                if isinstance(current, dict) and part in current:
                    current = current[part]
                else:
                    raise ValueError(f"Required input '{path}' not found in structure: {data}")
            
            return current

        prepared = []
        for param in params:
            if isinstance(param, str) and param.startswith('@'):
                input_key = param[1:]  # Remove the '@' symbol
                try:
                    # First try direct access
                    if input_key in inputs:
                        prepared.append(inputs[input_key])
                    # Then try nested access in 'inputs'
                    elif 'inputs' in inputs:
                        prepared.append(get_nested_value(inputs, f'inputs.{input_key}'))
                    else:
                        raise ValueError(f"Required input '{input_key}' not found")
                except (KeyError, TypeError):
                    raise ValueError(f"Required input '{input_key}' not found or has invalid format")
            else:
                prepared.append(param)
                
        return prepared

    def execute_scenario(self, scenario_name: str, initial_input: Dict) -> Dict:
        """
        Execute a scenario by performing a sequence of steps.

        :param scenario_name: Name of the scenario (Firestore collection).
        :param initial_input: Initial input data for the scenario.
        :return: Cache containing intermediate and final outputs.
        :raises ValueError: If the scenario or steps are invalid.
        """
        # Read all documents from the collection and sort by document ID
        scenario_docs = self.read(collection=scenario_name)
        if not scenario_docs:
            raise ValueError(f"Scenario '{scenario_name}' not found")
        
        # Sort documents by their step number (s1, s2, s3, etc.)
        sorted_docs = sorted(
            scenario_docs,
            key=lambda x: int(x.get('step_number', x.get('__name__', '0').replace('s', '')))
        )
        
        print(f"Found {len(sorted_docs)} steps")
        
        # Parse steps into Step objects
        steps = []
        for doc in sorted_docs:
            step_data = {
                'actor': doc.get('actor'),
                'action': doc.get('action'),
                'kwargs': doc.get('kwargs', {}),
                'output_name': doc.get('output_name')
            }
            steps.append(Step(**step_data))
        
        print(f"Steps to execute: {steps}")
                
        # Initialize cache with input
        self.cache = {"input": initial_input}
        
        # Execute each step
        for i, step in enumerate(steps, 1):
            try:
                # Replace any cache references in kwargs
                processed_kwargs = self._process_kwargs(step.kwargs)
                
                # Execute the appropriate action based on step type
                result = None
                if step.action == "read":
                    result = self.read(**processed_kwargs)
                elif step.action == "write":
                    result = self.write(**processed_kwargs)
                elif step.action == "update":
                    result = self.update(**processed_kwargs)
                elif step.action == "delete":
                    result = self.delete(**processed_kwargs)
                elif step.action == "manipulate":
                    result = self.manipulate(
                        method_name=processed_kwargs.pop('method_name'),
                        inputs=processed_kwargs
                    )
                elif step.action == "send":
                    result = self.send(
                        service=processed_kwargs.pop('service'),
                        endpoint=processed_kwargs.pop('endpoint'),
                        data=processed_kwargs.get('data'),
                        api_key=processed_kwargs.get('api_key')
                    )
                else:
                    raise ValueError(f"Unknown action: {step.action}")
                
                # Store result in cache if output_name is specified
                if step.output_name:
                    self.cache[step.output_name] = result

                print(f"Step {i} executed successfully: {result}")
                
                # Store step execution status
                self.cache[f"step_{i}_status"] = "success"
                
            except Exception as e:
                # Store error information in cache
                self.cache[f"step_{i}_status"] = "error"
                self.cache[f"step_{i}_error"] = str(e)
                raise RuntimeError(f"Error executing step {i}: {str(e)}")
        
        return self.cache
    
    def _process_kwargs(self, kwargs: Dict) -> Dict:
        """
        Process kwargs dictionary to replace cache references with actual values.
        Supports nested cache references using dot notation (e.g., 'input.user_message')
        
        Args:
            kwargs (Dict): Original kwargs dictionary
            
        Returns:
            Dict: Processed kwargs with cache values substituted
        """
        def get_nested_value(cache: Dict, key_path: str):
            """Helper function to get nested values using dot notation"""
            keys = key_path.split('.')
            value = cache
            for k in keys:
                if isinstance(value, dict) and k in value:
                    value = value[k]
                else:
                    raise ValueError(f"Cache reference '{key_path}' not found")
            return value

        processed = {}
        for key, value in kwargs.items():
            if isinstance(value, str) and value.startswith("$"):
                # Remove $ and get the cache key
                cache_key = value[1:]
                try:
                    processed[key] = get_nested_value(self.cache, cache_key)
                except ValueError as e:
                    raise ValueError(f"Cache reference '{cache_key}' not found")
            elif isinstance(value, dict):
                # Recursively process nested dictionaries
                processed[key] = self._process_kwargs(value)
            elif isinstance(value, list):
                # Process lists that might contain cache references
                processed[key] = [
                    self._process_kwargs({0: v})[0] if isinstance(v, (str, dict))
                    else v for v in value
                ]
            else:
                processed[key] = value
                
        return processed


if __name__ == "__main__":
    import argparse
    from google.cloud import firestore
    import logging

    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)

    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Execute scenarios using ScenarioExecutor')
    parser.add_argument('--scenario', type=str, default='content_generation',
                       help='Name of the scenario to execute')
    parser.add_argument('--input-file', type=str,
                       help='Path to JSON file containing input data')
    args = parser.parse_args()

    try:
        # Initialize Firestore client
        logger.info("Initializing Firestore client...")
        db = firestore.Client()
        executor = ScenarioExecutor(db)

        # Prepare input data
        if args.input_file:
            with open(args.input_file, 'r') as f:
                input_data = json.load(f)
        else:
            # Default test input
            input_data = {
                "user_message": "Generate a scenario for user registration flow"
            }

        # Execute scenario
        logger.info(f"Executing scenario '{args.scenario}' with input: {input_data}")
        results = executor.execute_scenario(
            scenario_name=args.scenario,
            initial_input=input_data
        )

        # Log results
        logger.info("Scenario execution completed successfully")
        logger.info("Results:")
        for key, value in results.items():
            if key.startswith('step_') and key.endswith('_status'):
                logger.info(f"{key}: {value}")
            elif not key.startswith('step_'):
                logger.info(f"{key}: {value}")

    except FileNotFoundError:
        logger.error(f"Input file not found: {args.input_file}")
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON in input file: {args.input_file}")
    except Exception as e:
        logger.error(f"Error executing scenario: {str(e)}", exc_info=True)