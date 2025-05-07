import datetime
import logging
from typing import Any, Dict, List
import requests
from statistics import mean
import numpy as np

from db import insert_observation, resend_unsent
from device_config import DEVICES
from observation import create_observation_payload
from tb_client import login_tb, logout_tb, get_telemetry, get_asset_info, post_recommendation_to_tb
from fc_client import get_compost_operation_details, post_observation_to_fc


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


def create_recommendation_for_pile():
    logging.info(f"🔁 Running recommendation analysis for Compost Pile")
    token = login_tb()
    if not token:
        return

    try:
        daily_stats = {}
        for device in DEVICES:
                telemetry = get_telemetry(device["id"], device["keys"], token)
                asset = get_asset_info(device["id"], token)
                if not asset:
                    logging.warning(f"No asset for {device['id']}")
                    return

                for key in device["keys"]:
                    datapoints = telemetry.get(key, [])
                    values = [float(dp["value"]) for dp in datapoints if "value" in dp]
                    if not values:
                        continue

                    daily_stats[key] = {
                        'min': np.min(values),
                        'max': np.max(values),
                        'avg': np.mean(values),
                        'std': np.std(values)

                    }

        start_date = datetime.datetime.now() - datetime.timedelta(days = 3)
        materials = ["grass clippings", "twigs", "wood chips"]
        results = analyze_compost_status(daily_stats, start_date, materials, [15.2], [10], [4.3])

        # Post the observation to the correct endpoint on FC
        post_success = post_recommendation_to_tb(asset["id"], results, token)

        msg = "✅ Sent" if post_success else "❌ Stored unsent"
        logging.info(f"{msg}: {device['name']} - {key}")


    except Exception as e:
        logging.error(f"Error processing: {e}")


def analyze_compost_status(
    daily_stats: Dict[str, Dict[str, float]],  # Aggregated stats (min, max, avg, std for temp, moisture, etc.)
    start_date: datetime,                      # Compost start date
    compost_materials: List[str],              # List of compost materials (e.g., ['greens', 'brown'])
    forecast_temp: List[float],                # List of forecasted temperatures for the next day (24 hourly values)
    forecast_humidity: List[float],           # List of forecasted humidity for the next day (24 hourly values)
    forecast_precipitation: List[float]       # List of forecasted precipitation for the next day (optional)
) -> Dict[str, Any]:
    """
    Analyzes compost status based on daily data, compost start date,
    forecasted values for the next day, and material type.
    """

    # Compost Age in days
    compost_age_days = (datetime.datetime.utcnow() - start_date).days
    # Compost speed factor
    speed_factor = classify_materials(compost_materials)
    # Compost Phase based on average temperature and moisture
    avg_temp = daily_stats['data_TEMP_SOIL']['avg']
    avg_moisture = daily_stats['data_water_SOIL']['avg']
    phase = ""
    if avg_temp > 60 and avg_moisture > 50:
        phase = "Active"
    elif avg_temp > 40 and avg_moisture < 50:
        phase = "Maturing"
    else:
        phase = "Curing"

    # Calculate Total Duration (Speed Factor adjusted)
    compost_hot_duration = 60
    total_duration = compost_hot_duration * speed_factor

    # Estimate Remaining Days for Composting
    if phase == "Active":
        estimated_days_remaining = max(0, total_duration - compost_age_days)
    else:
        estimated_days_remaining = max(0, total_duration - compost_age_days)

    # Recommendations based on forecasted values for the next day
    recommendation = ""
    if forecast_temp[0] > 70 and forecast_humidity[0] > 60:
        recommendation = "Warning: High temperature and humidity expected. Consider actively turning the compost pile."
    elif daily_stats['data_PH1_SOIL']['avg'] < 6.0:  # Check if pH goes below 6.0
        recommendation = "Warning: pH level is low. Consider adding lime to balance pH."
    elif phase == "Active":
        recommendation = "Continue to monitor moisture and temperature. Ensure pile is turning regularly."
    else:
        recommendation = "Monitor temperature closely for optimal results."

    # 7. Compile the results into a dictionary
    compost_status = {
        "compost_age_days": compost_age_days,
        "speed_factor": speed_factor,
        "phase": phase,
        "total_duration": total_duration,
        "estimated_days_remaining": estimated_days_remaining,
        "recommendation": recommendation,
    }

    return compost_status


# Helper functions from the advanced logic
def classify_materials(material_list):
    # Define the lists of green and woody materials
    greens = ["vegetable scraps", "grass clippings", "coffee grounds", "manure"]
    woody = ["branches", "twigs", "wood chips", "sawdust", "straw"]

    # Define the speed factors for greens and browns
    speed_factors = {
        'greens': 1.5,  # Faster decomposition
        'browns': 1.0,   # Slower decomposition
        'mixed': 1.2     # Mixed materials decompose at an intermediate rate
    }

    # Initialize speed factor sum
    speed_factor = 0
    
    # Calculate speed factor based on materials in the list
    for material in material_list:
        if material in greens:
            speed_factor += speed_factors['greens']  # Green materials have a faster decomposition rate
        elif material in woody:
            speed_factor += speed_factors['browns']  # Woody materials have a slower decomposition rate
        else:
            # If the material is not in the predefined categories, assign a default speed factor (e.g., 1.0)
            speed_factor += 1.0

    # If the list is empty or no recognized materials, avoid division by zero
    if len(material_list) > 0:
        # Average the speed factor across the number of materials in the list
        speed_factor /= len(material_list)
    else:
        # Default speed factor if no materials are passed
        speed_factor = 1.0

    # Return the calculated speed factor
    return speed_factor

