from flask import jsonify
from firebase_functions import https_fn
from firebase_admin import firestore, initialize_app
from app import ScenarioExecutor
import json
from typing import Dict, Tuple, Any

# Initialize Firebase App
initialize_app()

class RequestProcessor:
    def __init__(self):
        self.db = firestore.client()
        self.executor = ScenarioExecutor(self.db)
    
    def validate_request(self, request_data: Dict) -> Tuple[bool, str]:
        """Validate the incoming request data"""
        if not request_data:
            return False, "Invalid request: JSON payload required"
            
        scenario_name = request_data.get("scenario_name")
        initial_input = request_data.get("initial_input")
        
        if not scenario_name:
            return False, "Invalid request: 'scenario_name' is required"
        if not initial_input:
            return False, "Invalid request: 'initial_input' is required"
            
        return True, ""
    
    def format_response(self, data: Any, status: int = 200) -> https_fn.Response:
        """Format the response with proper headers and status"""
        return https_fn.Response(
            json.dumps({"output": data}),
            status=status,
            headers={
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"  # Add CORS headers if needed
            }
        )
    
    def format_error(self, message: str, status: int = 400) -> https_fn.Response:
        """Format error responses"""
        return https_fn.Response(
            json.dumps({
                "error": message,
                "status": status
            }),
            status=status,
            headers={
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"  # Add CORS headers if needed
            }
        )
    
    def process(self, request_data: Dict) -> https_fn.Response:
        """Process the request and execute the scenario"""
        try:
            # Validate request
            is_valid, error_message = self.validate_request(request_data)
            if not is_valid:
                return self.format_error(error_message)
            
            # Execute scenario
            result = self.executor.execute_scenario(
                request_data["scenario_name"],
                request_data["initial_input"]
            )
            
            # Check for expected output
            final_output = result.get("true")
            if final_output is None:
                return self.format_error(
                    "Output 'true' not found in scenario execution result",
                    status=500
                )
            
            return self.format_response(final_output)
            
        except ValueError as ve:
            # Handle validation errors from ScenarioExecutor
            return self.format_error(str(ve), status=400)
            
        except Exception as e:
            # Handle unexpected errors
            return self.format_error(
                f"Internal server error: {str(e)}",
                status=500
            )

@https_fn.on_request()
def process_request(req: https_fn.Request) -> https_fn.Response:
    """
    Firebase function to handle scenario execution requests
    
    Expected request format:
    {
        "scenario_name": "scenarios",
        "initial_input": "string input for the scenario"
    }
    """
    # Handle preflight requests for CORS
    if req.method == 'OPTIONS':
        return https_fn.Response(
            status=204,
            headers={
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "POST",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Max-Age": "3600"
            }
        )
    
    # Only allow POST requests
    if req.method != 'POST':
        return https_fn.Response(
            json.dumps({"error": "Method not allowed"}),
            status=405,
            headers={"Allow": "POST"}
        )
    
    try:
        request_data = req.get_json()
    except Exception:
        return https_fn.Response(
            json.dumps({"error": "Invalid JSON payload"}),
            status=400,
            headers={"Content-Type": "application/json"}
        )
    
    processor = RequestProcessor()
    return processor.process(request_data)