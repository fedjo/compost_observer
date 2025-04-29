import sqlite3

DB_PATH = "db.sqlite"

def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id TEXT,
                device_name TEXT,
                asset_id TEXT,
                pile_id TEXT,
                variable TEXT,
                mean_value REAL,
                min_value REAL,
                max_value REAL,
                date TEXT,
                sent INTEGER DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        print("✅ Database initialized at db.sqlite")

if __name__ == "__main__":
    init_db()
