import requests

def call_external_api(endpoint : str, token : str = None) :
    """
    Function used to make an api call to an external endpoint and return the response data.
    Args:
        endpoint (str): The API endpoint to call.
    Returns:
        dict: The JSON response from the API.
    Raises:
        Exception: If the API call fails or returns a non-200 status code.
    """

    response = requests.get(endpoint, headers={"Content-Type": "application/json"}, params={f"access_key": token})
    if response.status_code == 200 :
        return response.json()
    else :
        raise Exception(f"API call failed with status code {response.status_code}")