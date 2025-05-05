import os
import requests
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

FC_URL = os.getenv("FARM_CALENDAR_URL2")
FC_LOGIN_URL = "https://gk.sip5.horizon-openagri.eu/api/login/"
FC_USERNAME = os.getenv("FC_USERNAME")
FC_PASSWORD = os.getenv("FC_PASSWORD")

# Function to login to Farm Calendar API and get JWT token
def login_to_fc():
    try:
        response = requests.post(FC_LOGIN_URL, json={'username': FC_USERNAME, 'password': FC_PASSWORD})
        response.raise_for_status()
        token = response.json()["access"]
        if not token:
            logging.error("Login failed: No token returned.")
            return None
        logging.info("Logged in successfully to Farm Calendar")
        return token
    except requests.exceptions.RequestException as e:
        logging.error(f"Error logging in to Farm Calendar: {e}")
        return None

# Function to fetch the compost operation ID from Farm Calendar
def get_compost_operation_details(pile_name, token):
    headers = {"Authorization": f"Bearer {token}"}
    compost_operations_url = f"{FC_URL}/CompostOperations/"
    try:
        # Get the list of compost operations
        response = requests.get(compost_operations_url, headers=headers)
        response.raise_for_status()
        compost_operations = response.json()

        # Filter compost operations based on pile_name
        for compost in compost_operations["@graph"]:
            if "isOperatedOn" in compost and compost["isOperatedOn"].get("@id") == f"urn:farmcalendar:CompostPile:{pile_name}":
                compost_id = compost["@id"].split(":")[-1]  # Extract the compost operation ID
                logging.info(f"Found compost operation ID: {compost_id}")
                start = compost.get("hasStartDatetime")
                end = compost.get("hasEndDatetime")

                if not start or not end:
                    logging.warning(f"Missing start or end date for {pile_name}")
                    return None

                return (compost_id, start, end)

        logging.warning(f"No compost operation found for pile {pile_name}")
        return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching compost operations: {e}")
        return None

# Function to post observation to the correct endpoint
def post_observation_to_fc(compost_operation_id, observation_data, token):
    if not compost_operation_id:
        logging.warning("No compost operation ID available. Skipping post.")
        return False

    url = f"{FC_URL}/CompostOperations/{compost_operation_id}/Observations/"
    headers = {"Authorization": f"Bearer {token}"}
    
    try:
        response = requests.post(url, json=observation_data, headers=headers)
        response.raise_for_status()
        logging.info(f"Successfully posted observation to {url}")
        return True
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to post observation: {e}")
        return False
