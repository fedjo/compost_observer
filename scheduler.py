import datetime
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from fc_client import get_compost_operation_details, login_to_fc
from telemetry_processor import create_recommendation_for_pile, process_telemetry_for_pile
from db import record_composite_id, create_tables

scheduler = BackgroundScheduler()

def schedule_for_pile(pile_name):
    # Login to Farm Calendar and get the JWT token
    fc_token = login_to_fc()
    if not fc_token:
        logging.error("Farm Calendar login failed. Skipping posting observations.")
        return

    res = get_compost_operation_details(pile_name, fc_token)
    if not res:
        return

    (cid, start, end) = res
    if not cid:
        logging.warning(f"No compost operation found for {pile_name}")
        return

    job_id = f"obs_{pile_name}"
    scheduler.add_job(
        func=process_telemetry_for_pile,
        trigger='cron',
        hour=23,
        minute=0,
        id=job_id,
        args=[cid, fc_token],
        start_date=start,
        end_date=end,
        replace_existing=True
    )
    record_composite_id(cid, pile_name, start, end)
    logging.info(f"📆 Scheduled job for {pile_name} from period {start} - {end} at 23:00 UTC daily.")


def schedule_for_tb_piles():
    job_id = f"obs_TB_monitor"
    scheduler.add_job(
        func=create_recommendation_for_pile,
        trigger='interval',
        minutes=5,
        id=job_id,
        start_date=datetime.datetime.now(),
        # end_date=end,
        replace_existing=True
    )
    logging.info(f"📆 Scheduled job for recommendations.")


def start_scheduler():
    create_tables()
    scheduler.start()
    logging.info("🟢 Scheduler running. Awaiting compost pile IDs.")

    schedule_for_tb_piles()
    try:
        while True:
            pile_name = input("Enter Compost Pile ID: ").strip()
            if pile_name:
                schedule_for_pile(pile_name)
    except (KeyboardInterrupt, SystemExit):
        logging.info("🛑 Stopping scheduler...")
        scheduler.shutdown()
