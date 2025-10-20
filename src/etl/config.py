#       API base URLS for data extraction
AVIATIONSTACK_BASE_URL = "https://api.aviationstack.com/v1"

#       API endpoints for data extraction
AVIATIONSTACK_ENDPOINTS = {
    "all_flights" : f"{AVIATIONSTACK_BASE_URL}/flights",
    "flights_schedules" : f"{AVIATIONSTACK_BASE_URL}/flight_schedules",
    "future_flights" : f"{AVIATIONSTACK_BASE_URL}/flightsFuture",
    "airline_routes" : f"{AVIATIONSTACK_BASE_URL}/routes",
    "airports" : f"{AVIATIONSTACK_BASE_URL}/airports",
    "airlines" : f"{AVIATIONSTACK_BASE_URL}/airlines", 
    "airplanes" : f"{AVIATIONSTACK_BASE_URL}/airplanes"
}

#       API keys for authentication
AVIATIONSTACK_API_KEY = "a941e74ba55a704e182d6d9a3302911b"