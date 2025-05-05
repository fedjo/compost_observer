import datetime
import os
from dotenv import load_dotenv
load_dotenv()

PH_ACTIVITY_TYPE_ID = os.getenv("PH_ACTIVITY_TYPE_ID")
TEMP_ACTIVITY_TYPE_ID = os.getenv("TEMP_ACTIVITY_TYPE_ID")
HUMIDITY_ACTIVITY_TYPE_ID = os.getenv("HUMIDITY_ACTIVITY_TYPE_ID")

# Function to create the observation payload
def create_observation_payload(key, minn, maxx, avg):
    # Determine the observed property and unit based on the variable (key)
    if any(e in key for e in ("TEMP", "temperature")):  # For temperature
        prop = "https://vocab.nerc.ac.uk/standard_name/air_temperature/"
        unit = "http://qudt.org/vocab/unit/DEG_C"
        act = TEMP_ACTIVITY_TYPE_ID
    elif any(e in key for e in ("water", "moisture")):  # For humidity
        prop = "http://vocab.nerc.ac.uk/standard_name/moisture_content_of_soil_layer/"
        unit = "http://qudt.org/vocab/unit/PERCENT"
        act = HUMIDITY_ACTIVITY_TYPE_ID
    elif any(e in key for e in  ("PH", "pH")):  # For pH
        prop = "http://vocab.nerc.ac.uk/standard_name/pH_of_soil_layer/"
        unit = "http://qudt.org/vocab/unit/UNITLESS"
        act = PH_ACTIVITY_TYPE_ID
    else:
        raise ValueError("Unknown telemetry key")

    # Create the payload
    return {
        "@type": "Observation",
        "observedProperty": prop,
        "activityType": f"urn:farmcalendar:FarmActivityType:{act}",
        "details": f"Values range from MIN: {minn} to MAX: {maxx}",
        "phenomenonTime": datetime.datetime.now(datetime.UTC).strftime('%Y-%m-%dT%H:%MZ'),
        "hasResult": {
            "@type": "QuantityValue",
            "hasValue": avg,
            "unit": unit
        }
    }