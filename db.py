import logging
import sqlite3

from observation import create_observation_payload
from fc_client import post_observation_to_fc

DB_PATH = "db.sqlite"

def create_tables():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id TEXT,
                device_name TEXT,
                asset_id TEXT,
                compost_pile_id INTEGER,
                variable TEXT,
                mean_value REAL,
                min_value REAL,
                max_value REAL,
                date TEXT,
                sent INTEGER DEFAULT 0,
                FOREIGN KEY (compost_pile_id) REFERENCES fc_compost_piles(id)
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS fc_compost_piles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pile_id TEXT UNIQUE,
                pile_name TEXT,
                start_date TEXT,
                end_date TEXT
            )
        ''')
        conn.commit()

def record_composite_id(fc_pile_id, fc_pile_name, start_date, end_date):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute('''
            INSERT OR IGNORE INTO fc_compost_piles (
                     pile_id, pile_name, start_date, end_date
            ) VALUES (?, ?, ?, ?)
        ''', (
            fc_pile_id,
            fc_pile_name,
            start_date,
            end_date
        ))
        conn.commit()

def insert_observation(payload, device_id, device_name, fc_pile_id, asset_id, sent):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute('''
            INSERT INTO observations (
                device_id, device_name, asset_id, pile_id,
                variable, mean_value, min_value, max_value, date, sent
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            device_id, device_name, asset_id, fc_pile_id,
            payload["observedProperty"],
            payload["hasResult"]["hasValue"],
            float(payload["details"].split("MIN:")[1].split("to")[0].strip()),
            float(payload["details"].split("MAX:")[1].strip()),
            payload["phenomenonTime"],
            int(sent)
        ))
        conn.commit()

def resend_unsent(fc_token):
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute("SELECT * FROM observations WHERE sent = 0").fetchall()
        for row in rows:
            payload = create_observation_payload(row[5], row[7], row[8], row[6])
            payload["phenomenonTime"] = row[9]
            if post_observation_to_fc(row[4], payload, fc_token):
                conn.execute("UPDATE observations SET sent = 1 WHERE id = ?", (row[0],))
                logging.info(f"Resent: {row[1]} - {row[5]}")
