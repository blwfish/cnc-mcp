"""
CNC Journal — MariaDB-backed log of jobs, events, and notes.

Schema (tables live in the shared mqtt_log database with cnc_ prefix):
  cnc_jobs   — one row per run_file() call
  cnc_events — timestamped log of significant operations (home, zero, probe, alarm, connect)

Notes can be appended to any job at any time. All timestamps are ISO-8601 UTC.

Credentials are fetched from macOS Keychain:
  keyring.get_password("mariadb-mqtt", user)
"""

import json
import threading
from datetime import datetime, timezone
from typing import Optional

import keyring
import pymysql
import pymysql.cursors


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


_RECONNECT_ERRNOS = {2006, 2013, 2055}  # MySQL server gone away / lost connection


# ---------------------------------------------------------------------------
# Journal
# ---------------------------------------------------------------------------

class Journal:
    def __init__(self, host: str = "localhost", port: int = 3306,
                 db: str = "mqtt_log", user: str = "dashboard"):
        self._host = host
        self._port = port
        self._db   = db
        self._user = user
        self._lock = threading.Lock()
        self._con  = None
        self._connect()
        self._migrate()

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def _connect(self):
        pw = keyring.get_password("mariadb-mqtt", self._user)
        if not pw:
            raise RuntimeError(
                f"No Keychain password for mariadb-mqtt/{self._user}. "
                f"Run: python3 -c \"import keyring; keyring.set_password('mariadb-mqtt', '{self._user}', 'YOUR_PASSWORD')\""
            )
        self._con = pymysql.connect(
            host=self._host, port=self._port,
            user=self._user, password=pw,
            database=self._db, charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,
        )

    def _cursor(self):
        """Return a cursor, reconnecting if the connection was lost."""
        try:
            self._con.ping(reconnect=True)
        except Exception as e:
            code = getattr(e, "args", [None])[0]
            if code in _RECONNECT_ERRNOS or self._con is None:
                self._connect()
            else:
                raise
        return self._con.cursor()

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def _migrate(self):
        with self._lock:
            cur = self._cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS cnc_jobs (
                    id           INT AUTO_INCREMENT PRIMARY KEY,
                    started_at   VARCHAR(32) NOT NULL,
                    completed_at VARCHAR(32),
                    filepath     TEXT NOT NULL,
                    filename     VARCHAR(255) NOT NULL,
                    total_lines  INT,
                    sent_lines   INT,
                    success      TINYINT,   -- 1=ok, 0=error, NULL=incomplete
                    error        TEXT,
                    duration_s   FLOAT,
                    wpos_start   VARCHAR(128),   -- JSON {x,y,z} at job start
                    notes        TEXT
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS cnc_events (
                    id    INT AUTO_INCREMENT PRIMARY KEY,
                    ts    VARCHAR(32) NOT NULL,
                    type  VARCHAR(32) NOT NULL,  -- connect|disconnect|home|zero|probe|alarm|error
                    data  TEXT                   -- JSON, optional detail
                )
            """)
            self._con.commit()

    # ------------------------------------------------------------------
    # Job lifecycle
    # ------------------------------------------------------------------

    def job_start(self, filepath: str, wpos: Optional[dict] = None) -> int:
        """Record job start. Returns job id."""
        from pathlib import Path
        with self._lock:
            cur = self._cursor()
            cur.execute(
                """INSERT INTO cnc_jobs (started_at, filepath, filename, wpos_start)
                   VALUES (%s, %s, %s, %s)""",
                (_now(), filepath, Path(filepath).name,
                 json.dumps(wpos) if wpos else None)
            )
            self._con.commit()
            return cur.lastrowid

    def job_complete(self, job_id: int, total_lines: int, sent_lines: int,
                     success: bool, error: Optional[str] = None):
        """Record job completion."""
        now = _now()
        with self._lock:
            cur = self._cursor()
            cur.execute("SELECT started_at FROM cnc_jobs WHERE id=%s", (job_id,))
            row = cur.fetchone()
            duration = None
            if row:
                try:
                    started = datetime.fromisoformat(row["started_at"])
                    ended   = datetime.fromisoformat(now)
                    duration = (ended - started).total_seconds()
                except Exception:
                    pass

            cur.execute(
                """UPDATE cnc_jobs SET completed_at=%s, total_lines=%s, sent_lines=%s,
                   success=%s, error=%s, duration_s=%s WHERE id=%s""",
                (now, total_lines, sent_lines, 1 if success else 0,
                 error, duration, job_id)
            )
            self._con.commit()

    # ------------------------------------------------------------------
    # Events
    # ------------------------------------------------------------------

    def log_event(self, event_type: str, data: Optional[dict] = None):
        with self._lock:
            cur = self._cursor()
            cur.execute(
                "INSERT INTO cnc_events (ts, type, data) VALUES (%s, %s, %s)",
                (_now(), event_type, json.dumps(data) if data else None)
            )
            self._con.commit()

    # ------------------------------------------------------------------
    # Notes
    # ------------------------------------------------------------------

    def add_note(self, job_id: int, note: str) -> bool:
        """Append a note to a job. Returns False if job_id not found."""
        with self._lock:
            cur = self._cursor()
            cur.execute("SELECT notes FROM cnc_jobs WHERE id=%s", (job_id,))
            row = cur.fetchone()
            if row is None:
                return False
            existing = row["notes"] or ""
            separator = "\n\n" if existing else ""
            timestamp = _now()
            combined = f"{existing}{separator}[{timestamp}] {note.strip()}"
            cur.execute(
                "UPDATE cnc_jobs SET notes=%s WHERE id=%s", (combined, job_id)
            )
            self._con.commit()
            return True

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def list_jobs(self, limit: int = 20) -> list[dict]:
        with self._lock:
            cur = self._cursor()
            cur.execute(
                """SELECT id, started_at, completed_at, filename, total_lines,
                          sent_lines, success, error, duration_s, notes
                   FROM cnc_jobs ORDER BY id DESC LIMIT %s""",
                (limit,)
            )
            return cur.fetchall()

    def get_job(self, job_id: int) -> Optional[dict]:
        with self._lock:
            cur = self._cursor()
            cur.execute("SELECT * FROM cnc_jobs WHERE id=%s", (job_id,))
            row = cur.fetchone()
            if row is None:
                return None
            result = dict(row)
            # Attach nearby events (±5 min of job window)
            started   = result.get("started_at")
            completed = result.get("completed_at") or _now()
            if started:
                cur.execute(
                    """SELECT ts, type, data FROM cnc_events
                       WHERE ts >= DATE_SUB(%s, INTERVAL 5 MINUTE)
                         AND ts <= DATE_ADD(%s, INTERVAL 5 MINUTE)
                       ORDER BY ts""",
                    (started, completed)
                )
                result["events"] = cur.fetchall()
            return result

    def list_events(self, limit: int = 50,
                    event_type: Optional[str] = None) -> list[dict]:
        with self._lock:
            cur = self._cursor()
            if event_type:
                cur.execute(
                    "SELECT * FROM cnc_events WHERE type=%s ORDER BY id DESC LIMIT %s",
                    (event_type, limit)
                )
            else:
                cur.execute(
                    "SELECT * FROM cnc_events ORDER BY id DESC LIMIT %s", (limit,)
                )
            return cur.fetchall()

    def stats(self) -> dict:
        with self._lock:
            cur = self._cursor()
            cur.execute(
                """SELECT COUNT(*) as total,
                          SUM(CASE WHEN success=1 THEN 1 ELSE 0 END) as succeeded,
                          SUM(CASE WHEN success=0 THEN 1 ELSE 0 END) as failed,
                          SUM(duration_s) as total_s,
                          MIN(started_at) as first_job,
                          MAX(started_at) as last_job
                   FROM cnc_jobs WHERE completed_at IS NOT NULL"""
            )
            row = cur.fetchone()
            return dict(row) if row else {}


# Module-level singleton — initialised by cnc_mcp_server on startup
_journal: Optional[Journal] = None


def init(host: str = "localhost", port: int = 3306,
         db: str = "mqtt_log", user: str = "dashboard") -> Journal:
    global _journal
    _journal = Journal(host=host, port=port, db=db, user=user)
    return _journal


def get() -> Optional[Journal]:
    return _journal
