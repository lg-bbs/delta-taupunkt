import sqlite3
import os
from typing import List, Dict, Any
from .model.Measurement import Measurement


DB_PATH = os.path.join(os.path.dirname(__file__), "measure.db")


class MeasureDB:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self._ensure_db()

    def _ensure_db(self):
        """Erstellt die Datenbank inkl. Tabelle, falls sie nicht existiert."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS measurement (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                time INTEGER NOT NULL,
                temp_inside REAL,
                hum_inside REAL,
                temp_outside REAL,
                hum_outside REAL
            );
        """)

        conn.commit()
        conn.close()

    def insert_measurement(self, m: Measurement):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO measurement
            (time, temp_inside, hum_inside, temp_outside, hum_outside)
            VALUES (?, ?, ?, ?, ?)
        """, (
            m.time,
            m.inside.temp,
            m.inside.hum,
            m.outside.temp,
            m.outside.hum
        ))

        conn.commit()
        conn.close()

    # -----------------------------------------------------------
    # LESEN
    # -----------------------------------------------------------
    def get_all(self) -> List[Dict[str, Any]]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        rows = cursor.execute(
            "SELECT * FROM measurement ORDER BY time ASC"
        ).fetchall()

        conn.close()
        return [dict(row) for row in rows]

    def get_last(self, limit: int = 10) -> List[Dict[str, Any]]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        rows = cursor.execute(
            "SELECT * FROM measurement ORDER BY time DESC LIMIT ?",
            (limit,)
        ).fetchall()

        conn.close()
        return [dict(row) for row in rows]