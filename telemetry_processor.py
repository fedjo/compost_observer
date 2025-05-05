import logging
import requests
from statistics import mean

from db import insert_observation, resend_unsent
from device_config import DEVICES
from observation import create_observation_payload
from tb_client import login_tb, logout_tb, get_telemetry, get_asset_info
from fc_client import login_to_fc, get_compost_operation_details, post_observation_to_fc


def process_telemetry_for_pile(compost_operation_id, fc_token):
    logging.info(f"🔁 Running telemetry process for Compost Operation: {compost_operation_id}")
    token = login_tb()
    if not token:
        return

    try:
        resend_unsent(fc_token)

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

                    stats = (min(values), max(values), mean(values))
                    observation_payload = create_observation_payload(key, *stats)
                    logging.info(observation_payload)

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


