"""
JSON Utility Functions
Helper functions for parsing JSON from LLM responses
"""

import re
import json
import logging


def extract_json_from_response(response: str) -> dict:
    """
    Extract JSON from LLM response, handling markdown code blocks
    
    Args:
        response: Raw LLM response text
        
    Returns:
        Parsed JSON dict, or dict with raw_response if parsing fails
    """
    # Remove markdown code blocks if present
    # Pattern: ```json\n{...}\n``` or ```\n{...}\n```
    cleaned_response = response.strip()
    
    # Remove leading/trailing markdown code blocks
    if cleaned_response.startswith('```'):
        # Find the first newline after ```
        first_newline = cleaned_response.find('\n')
        if first_newline != -1:
            cleaned_response = cleaned_response[first_newline + 1:]
        
        # Remove trailing ```
        if cleaned_response.endswith('```'):
            cleaned_response = cleaned_response[:-3].strip()
    
    # Try to extract JSON object
    try:
        json_match = re.search(r'\{.*\}', cleaned_response, re.DOTALL)
        if json_match:
            json_str = json_match.group()
            return json.loads(json_str)
        else:
            logging.warning("No JSON object found in response")
            return {"raw_response": response}
    except json.JSONDecodeError as e:
        logging.error(f"JSON decode error: {e}")
        logging.error(f"Attempted to parse: {cleaned_response[:200]}...")
        return {"raw_response": response}
    except Exception as e:
        logging.error(f"Unexpected error parsing JSON: {e}")
        return {"raw_response": response}
