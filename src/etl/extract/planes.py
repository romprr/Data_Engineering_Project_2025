from utils.api import call_external_api
from config import AVIATIONSTACK_ENDPOINTS, AVIATIONSTACK_API_KEY

def extract_planes_data() :
    """
    Function to extract planes data from an external API.
    Returns:
        list: A list of planes data retrieved from the API in JSON.
    """
    planes_data = call_external_api(AVIATIONSTACK_ENDPOINTS["airplanes"], AVIATIONSTACK_API_KEY)
    return planes_data
