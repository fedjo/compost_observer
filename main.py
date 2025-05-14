import argparse
import logging

from db import create_tables, record_composite_id
from fc_client import get_compost_operation_details, login_to_fc
from scheduler import start_scheduler
from telemetry_processor import create_recommendation_for_pile, process_telemetry_for_pile


LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--reco", action="store_true", help="Start a recommendation process for a pile immediately")
    parser.add_argument("--fc-obs", action="store_true", help="Send daily stats from pile to farm calendar immediately")
    args = parser.parse_args()

    if args.reco:
        logging.info("⚡ Start a recommendation process for a pile immediately")
        asset_id = input("Enter Thingsboard Pile ID: ").strip()
        if asset_id:
            create_recommendation_for_pile(asset_id)
    elif args.fc_obs:
        create_tables()
        logging.info("⚡ Send daily stats from pile to farm calendar immediately")
        pile_name = input("Enter Farm Calendar Pile Name: ").strip()
        # Login to Farm Calendar and get the JWT token
        fc_token = login_to_fc()
        if not fc_token:
            logging.error("Farm Calendar login failed. Skipping posting observations.")
        else:
            details = get_compost_operation_details(pile_name, fc_token)
            if not details:
                logging.warning(f"No compost operation found for {pile_name}")
            else:
                (cid, start, end) = details
                process_telemetry_for_pile(cid, fc_token)
                record_composite_id(cid, pile_name, start, end)
    else:
        start_scheduler()
