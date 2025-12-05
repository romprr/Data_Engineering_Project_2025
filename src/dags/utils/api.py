# Utility functions for API interactions
import requests
def query(url, params=None, headers=None):
    """
    Query a REST API and return the JSON response.

    Args:
        url (str): The API endpoint URL.
        params (dict, optional): Query parameters for the API request.
        headers (dict, optional): Headers for the API request.
    Returns:
        dict: The JSON response from the API.
    """
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()  # Raise an error for bad responses
    return response.json()