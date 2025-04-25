import os
import json
import logging
import sqlite3
import requests
import argparse
from dotenv import load_dotenv
from statistics import mean
from apscheduler.schedulers.blocking import BlockingScheduler
import datetime
from device_config import DEVICES

from farm_calendar import login_to_fc, get_compost_operation_id, post_observation_to_fc


# Load environment
load_dotenv()
TB_URL = os.getenv("THINGSBOARD_URL")
TB_USER = os.getenv("THINGSBOARD_USERNAME")
TB_PASS = os.getenv("THINGSBOARD_PASSWORD")
FARM_URL = os.getenv("FARM_CALENDAR_URL")
PH_ACTIVITY_TYPE_ID=os.getenv("PH_ACTIVITY_TYPE_ID")
TEMP_ACTIVITY_TYPE_ID=os.getenv("TEMP_ACTIVITY_TYPE_ID")
HUMIDITY_ACTIVITY_TYPE_ID=os.getenv("HUMIDITY_ACTIVITY_TYPE_ID")

DB_PATH = "db.sqlite"
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)


def login_tb():
    try:
        r = requests.post(f"{TB_URL}/api/auth/login", json={"username": TB_USER, "password": TB_PASS})
        r.raise_for_status()
        logging.info("Authenticated successfully!")
        return r.json()["token"]
    except Exception as e:
        logging.error(f"Login failed: {e}")
        return None


def logout_tb(token):
    try:
        requests.post(f"{TB_URL}/api/auth/logout", headers={"X-Authorization": f"Bearer {token}"})
    except:
        pass


def get_time_range():
    now = datetime.datetime.now(datetime.UTC)
    start = datetime.datetime(now.year, now.month, now.day)
    end = start + datetime.timedelta(days=1)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)


def get_telemetry(device_id, keys, token):
    headers = {"X-Authorization": f"Bearer {token}"}
    start_ts, end_ts = get_time_range()
    params = {
        "keys": ",".join(keys),
        "startTs": start_ts,
        "endTs": end_ts,
        "limit": 10000,
        "orderBy": "ASC"
    }
    url = f"{TB_URL}/api/plugins/telemetry/DEVICE/{device_id}/values/timeseries"
    r = requests.get(url, headers=headers, params=params)
    r.raise_for_status()
    return r.json()


def get_asset_info(device_id, token):
    headers = {"X-Authorization": f"Bearer {token}"}
    url = f"{TB_URL}/api/relations?toId={device_id}&toType=DEVICE"
    
    try:
        # Send the request to ThingsBoard to get the device relations
        r = requests.get(url, headers=headers)
        r.raise_for_status()  # Raise error if the request fails
        relations = r.json()

        # Look for the device-to-asset relationship
        for relation in relations:
            if relation["from"]["entityType"] == "ASSET":
                logging.info(f"Device {device_id} is linked to asset {relation['from']['id']}")
                return relation['from']  # Return asset details if found
        
        # No asset found for the device, log a warning and return None
        logging.warning(f"No asset linked to device {device_id}")
        return None

    except requests.exceptions.RequestException as e:
        # Log any error encountered during the request
        logging.error(f"Failed to fetch asset info for {device_id}: {e}")
        return None

# Function to create the observation payload
def create_observation_payload(key, avg):
    # Determine the observed property and unit based on the variable (key)
    observed_property = ""
    unit = ""

    if any(e in key for e in ("TEMP" or "temperature")):  # For temperature
        observed_property = "https://vocab.nerc.ac.uk/standard_name/air_temperature/"
        unit = "http://qudt.org/vocab/unit/DEG_C"
        activity_type_id = f'{TEMP_ACTIVITY_TYPE_ID}'
    elif any(e in key for e in ("water" or "moisture")):  # For humidity
        observed_property = "http://vocab.nerc.ac.uk/standard_name/moisture_content_of_soil_layer/"
        unit = "http://qudt.org/vocab/unit/PERCENT"
        activity_type_id = f'{HUMIDITY_ACTIVITY_TYPE_ID}'
    elif any(e in key for e in  ("PH"  "pH")):  # For pH
        observed_property = "http://vocab.nerc.ac.uk/standard_name/pH_of_soil_layer/"
        unit = "http://qudt.org/vocab/unit/UNITLESS"
        activity_type_id = f'{PH_ACTIVITY_TYPE_ID}'

    # Create the payload
    phenomenon_time = datetime.datetime.now(datetime.UTC).strftime('%Y-%m-%dT%H:%MZ')
    payload = {
        "@type": "Observation",
        "observedProperty": observed_property,
        "activityType": f"urn:farmcalendar:FarmActivityType:{activity_type_id}",
        "phenomenonTime": phenomenon_time,
        "hasResult": {
            "@type": "QuantityValue",
            "hasValue": avg,  # The calculated mean value
            "unit": unit
        }
    }
    return payload


def insert_observation(payload, device_id, device_name, pile_id, assset_name, sent):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            '''
            INSERT INTO observations (device_id, device_name, asset_id, pile_id, variable, mean_value, date, sent)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                device_id,
                device_name,
                pile_id,
                assset_name,
                payload["observedProperty"],
                payload["hasResult"]["hasValue"],
                payload["phenomenonTime"],
                int(sent)
            )
        )
        conn.commit()


def try_send(payload):
    try:
        r = requests.post(FARM_URL, json=payload)
        r.raise_for_status()
        return True
    except Exception as e:
        logging.warning(f"Send failed: {e}")
        return False


def resend_unsent():
    # Login to Farm Calendar and get the JWT token
    fc_token = login_to_fc()
    if not fc_token:
        logging.error("Farm Calendar login failed. Skipping posting observations.")
        return

    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute("SELECT * FROM observations WHERE sent = 0").fetchall()
        for row in rows:
            payload = create_observation_payload(row[5], row[6])
            payload["phenomenonTime"] = row[7]
            print(payload)

            if post_observation_to_fc(row[4], payload, fc_token):
                conn.execute("UPDATE observations SET sent = 1 WHERE id = ?", (row[0],))
                logging.info(f"Resent: {row[1]} - {row[5]}")


def process_devices():
    logging.info("📡 Starting telemetry processing")
    token = login_tb()
    if not token:
        return

     # Login to Farm Calendar and get the JWT token
    fc_token = login_to_fc()
    if not fc_token:
        logging.error("Farm Calendar login failed. Skipping posting observations.")
        return

    try:
        resend_unsent()

        for device in DEVICES:
            try:
                telemetry = get_telemetry(device["id"], device["keys"], token)
                asset = get_asset_info(device["id"], token)
                if not asset:
                    logging.warning(f"No asset for {device['id']}")
                    continue

                for key in device["keys"]:
                    datapoints = telemetry.get(key, [])
                    values = [float(dp["value"]) for dp in datapoints if "value" in dp]
                    if not values:
                        continue

                    avg = mean(values)
                    observation_payload = create_observation_payload(key, avg)
                    print(observation_payload)

                    # Fetch compost operation ID from Farm Calendar
                    compost_operation_id = get_compost_operation_id(fc_token)

                    # Post the observation to the correct endpoint on FC
                    post_success = post_observation_to_fc(compost_operation_id, observation_payload, fc_token)

                    if not post_success:
                        insert_observation(
                            observation_payload, device["id"], device["name"],
                            asset["id"], compost_operation_id, post_success
                        )
                    msg = "✅ Sent" if post_success else "❌ Stored unsent"
                    logging.info(f"{msg}: {device['name']} - {key}")

            except Exception as e:
                logging.error(f"Error processing {device['name']}: {e}")
    finally:
        logout_tb(token)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--now", action="store_true", help="Run processing immediately (in addition to scheduled)")
    args = parser.parse_args()

    if args.now:
        logging.info("⚡ Running process_devices() manually with --now")
        process_devices()
    else:
        scheduler = BlockingScheduler()
        scheduler.add_job(process_devices, "cron", hour=23, minute=59)
        logging.info("🔁 Scheduler started — job scheduled for 23:59 UTC daily")
        try:
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            logging.info("Stopping scheduler...")

if __name__ == "__main__":
    main()
