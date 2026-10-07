"""FORTLO lead-pipeline schema on the existing outreach SQLite.

Extends outreach_log.sqlite (DB_PATH) with accounts, contacts, and runs.
Does not alter the existing `sent` outreach table.

Connection order:
  1. --db / CLI override
  2. DATABASE_URL (postgres:// or postgresql://)
  3. DB_PATH (same env as app2.py)
  4. outreach_log.sqlite next to this file
"""

from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence
from urllib.parse import urlparse

from dotenv import load_dotenv

load_dotenv()

SCHEMA_VERSION = 1

ACCOUNTS_DDL = """
CREATE TABLE IF NOT EXISTS accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    legal_name TEXT,
    domain TEXT NOT NULL UNIQUE,
    website TEXT,
    linkedin_url TEXT,
    city TEXT,
    country TEXT NOT NULL DEFAULT 'Germany',
    revenue_eur REAL,
    revenue_year INTEGER,
    employees INTEGER,
    industry TEXT,
    size_band TEXT,
    status TEXT NOT NULL,
    exclusion_reason TEXT,
    score_notes TEXT,
    suggested_titles_json TEXT,
    source_urls_json TEXT,
    apollo_org_id TEXT,
    first_seen_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
)
"""

CONTACTS_DDL = """
CREATE TABLE IF NOT EXISTS contacts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER NOT NULL,
    apollo_person_id TEXT NOT NULL UNIQUE,
    first_name TEXT,
    last_name TEXT,
    title TEXT,
    seniority TEXT,
    email TEXT,
    email_status TEXT,
    linkedin_url TEXT,
    city TEXT,
    country TEXT,
    lawful_basis TEXT NOT NULL DEFAULT 'legitimate_interest_b2b',
    raw_json TEXT,
    first_seen_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (account_id) REFERENCES accounts(id)
)
"""

RUNS_DDL = """
CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stage TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    rows_written INTEGER DEFAULT 0,
    notes TEXT,
    error TEXT
)
"""

INDEXES = [
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_accounts_domain ON accounts(domain)",
    "CREATE INDEX IF NOT EXISTS idx_accounts_status ON accounts(status)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_contacts_apollo_person ON contacts(apollo_person_id)",
    "CREATE INDEX IF NOT EXISTS idx_contacts_account ON contacts(account_id)",
    "CREATE INDEX IF NOT EXISTS idx_runs_stage ON runs(stage)",
]


def utcnow() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def normalize_domain(value: Optional[str]) -> str:
    if not value:
        return ""
    text = value.strip().lower()
    text = text.replace("https://", "").replace("http://", "")
    text = text.split("/")[0]
    if text.startswith("www."):
        text = text[4:]
    return text.strip(".")


def default_db_path() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    return os.getenv("DB_PATH", os.path.join(here, "outreach_log.sqlite"))


def _is_postgres(url: Optional[str]) -> bool:
    if not url:
        return False
    return urlparse(url).scheme in {"postgres", "postgresql"}


class Database:
    """SQLite (default) or Postgres (DATABASE_URL) adapter."""

    def __init__(self, path: Optional[str] = None, url: Optional[str] = None):
        self.url = url or os.getenv("DATABASE_URL")
        self.path = path or default_db_path()
        self.postgres = _is_postgres(self.url)
        self._pg = None

    def connect(self):
        if self.postgres:
            try:
                import psycopg2  # type: ignore
            except ImportError as exc:
                raise RuntimeError(
                    "DATABASE_URL is Postgres but psycopg2 is not installed"
                ) from exc
            conn = psycopg2.connect(self.url)
            conn.autocommit = False
            return conn
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        return conn

    def _ph(self, sql: str) -> str:
        if self.postgres:
            return sql.replace("?", "%s")
        return sql

    def init_schema(self, conn=None) -> None:
        own = conn is None
        if own:
            conn = self.connect()
        cur = conn.cursor()
        if self.postgres:
            cur.execute(ACCOUNTS_DDL.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY"))
            cur.execute(CONTACTS_DDL.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY"))
            cur.execute(RUNS_DDL.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY"))
        else:
            cur.execute(ACCOUNTS_DDL)
            cur.execute(CONTACTS_DDL)
            cur.execute(RUNS_DDL)
        for ddl in INDEXES:
            cur.execute(ddl)
        conn.commit()
        if own:
            conn.close()

    def execute(self, conn, sql: str, params: Sequence[Any] = ()):
        cur = conn.cursor()
        cur.execute(self._ph(sql), params)
        return cur

    def fetchone(self, conn, sql: str, params: Sequence[Any] = ()):
        cur = self.execute(conn, sql, params)
        row = cur.fetchone()
        return dict(row) if row is not None and not isinstance(row, dict) else row

    def fetchall(self, conn, sql: str, params: Sequence[Any] = ()):
        cur = self.execute(conn, sql, params)
        rows = cur.fetchall()
        out = []
        for row in rows:
            out.append(dict(row) if not isinstance(row, dict) else row)
        return out

    def upsert_account(self, conn, rec: Dict[str, Any]) -> int:
        now = utcnow()
        domain = normalize_domain(rec.get("domain") or rec.get("website"))
        if not domain:
            raise ValueError(f"account {rec.get('name')!r} has no domain")
        existing = self.fetchone(
            conn, "SELECT id, first_seen_at FROM accounts WHERE domain = ?", (domain,)
        )
        payload = {
            "name": rec["name"],
            "legal_name": rec.get("legal_name") or rec["name"],
            "domain": domain,
            "website": rec.get("website") or f"https://{domain}",
            "linkedin_url": rec.get("linkedin_url"),
            "city": rec.get("city"),
            "country": rec.get("country") or "Germany",
            "revenue_eur": rec.get("revenue_eur"),
            "revenue_year": rec.get("revenue_year"),
            "employees": rec.get("employees"),
            "industry": rec.get("industry"),
            "size_band": rec.get("size_band"),
            "status": rec["status"],
            "exclusion_reason": rec.get("exclusion_reason"),
            "score_notes": rec.get("score_notes"),
            "suggested_titles_json": rec.get("suggested_titles_json"),
            "source_urls_json": rec.get("source_urls_json"),
            "updated_at": now,
        }
        if rec.get("apollo_org_id"):
            payload["apollo_org_id"] = rec["apollo_org_id"]
        if existing:
            cols = [k for k in payload if k != "domain"]
            sets = ", ".join(f"{c} = ?" for c in cols)
            values = [payload[c] for c in cols] + [existing["id"]]
            self.execute(conn, f"UPDATE accounts SET {sets} WHERE id = ?", values)
            return int(existing["id"])
        payload["first_seen_at"] = now
        cols = list(payload.keys())
        placeholders = ", ".join("?" for _ in cols)
        self.execute(
            conn,
            f"INSERT INTO accounts ({', '.join(cols)}) VALUES ({placeholders})",
            [payload[c] for c in cols],
        )
        row = self.fetchone(conn, "SELECT id FROM accounts WHERE domain = ?", (domain,))
        return int(row["id"])

    def upsert_contact(self, conn, rec: Dict[str, Any]) -> int:
        now = utcnow()
        pid = rec["apollo_person_id"]
        existing = self.fetchone(
            conn, "SELECT id FROM contacts WHERE apollo_person_id = ?", (pid,)
        )
        payload = {
            "account_id": rec["account_id"],
            "apollo_person_id": pid,
            "first_name": rec.get("first_name"),
            "last_name": rec.get("last_name"),
            "title": rec.get("title"),
            "seniority": rec.get("seniority"),
            "email": rec.get("email"),
            "email_status": rec.get("email_status"),
            "linkedin_url": rec.get("linkedin_url"),
            "city": rec.get("city"),
            "country": rec.get("country"),
            "lawful_basis": rec.get("lawful_basis") or "legitimate_interest_b2b",
            "raw_json": rec.get("raw_json"),
            "updated_at": now,
        }
        if existing:
            cols = [k for k in payload]
            sets = ", ".join(f"{c} = ?" for c in cols)
            self.execute(
                conn,
                f"UPDATE contacts SET {sets} WHERE id = ?",
                [payload[c] for c in cols] + [existing["id"]],
            )
            return int(existing["id"])
        payload["first_seen_at"] = now
        cols = list(payload.keys())
        self.execute(
            conn,
            f"INSERT INTO contacts ({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)})",
            [payload[c] for c in cols],
        )
        row = self.fetchone(
            conn, "SELECT id FROM contacts WHERE apollo_person_id = ?", (pid,)
        )
        return int(row["id"])

    def start_run(self, conn, stage: str, notes: str = "") -> int:
        cur = self.execute(
            conn,
            "INSERT INTO runs (stage, started_at, notes) VALUES (?, ?, ?)",
            (stage, utcnow(), notes),
        )
        conn.commit()
        if self.postgres:
            row = self.fetchone(conn, "SELECT id FROM runs ORDER BY id DESC LIMIT 1")
            return int(row["id"])
        return int(cur.lastrowid)

    def finish_run(
        self,
        conn,
        run_id: int,
        rows_written: int = 0,
        notes: Optional[str] = None,
        error: Optional[str] = None,
    ) -> None:
        self.execute(
            conn,
            """
            UPDATE runs
               SET finished_at = ?, rows_written = ?, notes = COALESCE(?, notes), error = ?
             WHERE id = ?
            """,
            (utcnow(), rows_written, notes, error, run_id),
        )
        conn.commit()

    def qualified_accounts(self, conn, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM accounts WHERE status = 'qualified' ORDER BY name"
        if limit:
            sql += f" LIMIT {int(limit)}"
        return self.fetchall(conn, sql)

    def counts_by_status(self, conn) -> Dict[str, int]:
        rows = self.fetchall(
            conn, "SELECT status, COUNT(*) AS n FROM accounts GROUP BY status"
        )
        return {r["status"]: int(r["n"]) for r in rows}

    def account_count(self, conn) -> int:
        row = self.fetchone(conn, "SELECT COUNT(*) AS n FROM accounts")
        return int(row["n"])

    def contact_count(self, conn) -> int:
        row = self.fetchone(conn, "SELECT COUNT(*) AS n FROM contacts")
        return int(row["n"])


@contextmanager
def open_db(path: Optional[str] = None) -> Iterator[tuple[Database, Any]]:
    db = Database(path=path)
    db.init_schema()
    conn = db.connect()
    try:
        yield db, conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
