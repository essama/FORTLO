"""Upsert Stage 1/2 rows into the existing FORTLO CRM (customers / contacts).

Uses the same sqlite file and hyphenless UUID style as the live EC2 CRM
so /customers and /contacts show ICP leads. Campaign flag stays off.
"""

from __future__ import annotations

import json
import os
import sqlite3
import uuid
from datetime import datetime
from typing import Any, Dict, Optional

from db import normalize_domain

EXISTING_TENANT = "92e521fdc9ed4667a27973396b38f385"


def crm_db_path() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    url = os.getenv("DATABASE_URL") or os.getenv("CRM_DATABASE_URL") or ""
    if url.startswith("sqlite:////"):
        return "/" + url[len("sqlite:////") :]
    if url.startswith("sqlite:///"):
        rest = url[len("sqlite:///") :]
        if rest.startswith("./"):
            rest = rest[2:]
        if not os.path.isabs(rest):
            return os.path.join(here, rest)
        return rest
    return os.path.join(here, "fortlo.db")


def _now() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def _new_id() -> str:
    return uuid.uuid4().hex


def _connect() -> sqlite3.Connection:
    path = crm_db_path()
    if not os.path.exists(path):
        raise FileNotFoundError(f"CRM db not found: {path}")
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    _ensure_columns(con)
    return con


def _cols(con, table: str) -> set:
    return {r[1] for r in con.execute(f"PRAGMA table_info({table})")}


def _ensure_columns(con: sqlite3.Connection) -> None:
    cust = _cols(con, "customers")
    for col, ddl in {
        "domain": "VARCHAR(255)",
        "legal_name": "VARCHAR(255)",
        "revenue_year": "INTEGER",
        "size_band": "VARCHAR(50)",
        "icp_status": "VARCHAR(50)",
        "exclusion_reason": "TEXT",
        "score_notes": "TEXT",
        "apollo_org_id": "VARCHAR(64)",
        "linkedin_url": "VARCHAR(255)",
        "source_urls_json": "TEXT",
        "suggested_titles_json": "TEXT",
        "leadgen_score": "INTEGER",
        "leadgen_decision": "VARCHAR(20)",
        "leadgen_reason": "TEXT",
        "leadgen_scored_at": "VARCHAR(30)",
    }.items():
        if col not in cust:
            con.execute(f"ALTER TABLE customers ADD COLUMN {col} {ddl}")
    cont = _cols(con, "contacts")
    for col, ddl in {
        "apollo_person_id": "VARCHAR(64)",
        "seniority": "VARCHAR(80)",
        "email_status": "VARCHAR(80)",
        "linkedin_url": "VARCHAR(255)",
        "city": "VARCHAR(100)",
        "country": "VARCHAR(100)",
        "lawful_basis": "VARCHAR(80)",
    }.items():
        if col not in cont:
            con.execute(f"ALTER TABLE contacts ADD COLUMN {col} {ddl}")
    con.commit()


def _tenant_id(con: sqlite3.Connection) -> str:
    env = os.getenv("CRM_TENANT_ID")
    if env:
        return uuid.UUID(env).hex
    row = con.execute("SELECT tenant_id FROM customers WHERE tenant_id IS NOT NULL LIMIT 1").fetchone()
    return row[0] if row else EXISTING_TENANT


def _find_customer(con, tenant_id: str, name: str, domain: str) -> Optional[sqlite3.Row]:
    if domain:
        row = con.execute(
            "SELECT * FROM customers WHERE tenant_id=? AND domain=?",
            (tenant_id, domain),
        ).fetchone()
        if row:
            return row
        row = con.execute(
            "SELECT * FROM customers WHERE tenant_id=? AND website LIKE ?",
            (tenant_id, f"%{domain}%"),
        ).fetchone()
        if row:
            return row
    return con.execute(
        "SELECT * FROM customers WHERE tenant_id=? AND name=?",
        (tenant_id, name),
    ).fetchone()


def upsert_customer(rec: Dict[str, Any]) -> str:
    domain = normalize_domain(rec.get("domain") or rec.get("website"))
    with _connect() as con:
        tenant_id = _tenant_id(con)
        row = _find_customer(con, tenant_id, rec["name"], domain)
        now = _now()
        fields = {
            "name": rec["name"],
            "legal_name": rec.get("legal_name") or rec["name"],
            "domain": domain,
            "website": rec.get("website") or (f"https://{domain}" if domain else None),
            "industry": rec.get("industry"),
            "country": rec.get("country") or "Germany",
            "city": rec.get("city"),
            "employee_count": rec.get("employees"),
            "annual_revenue": rec.get("revenue_eur"),
            "revenue_year": rec.get("revenue_year"),
            "size_band": rec.get("size_band"),
            "icp_status": rec.get("status"),
            "exclusion_reason": rec.get("exclusion_reason"),
            "score_notes": rec.get("score_notes"),
            "linkedin_url": rec.get("linkedin_url"),
            "source_urls_json": rec.get("source_urls_json"),
            "suggested_titles_json": rec.get("suggested_titles_json"),
            "updated_at": now,
        }
        if rec.get("apollo_org_id"):
            fields["apollo_org_id"] = rec["apollo_org_id"]
        for col in ("leadgen_score", "leadgen_decision", "leadgen_reason", "leadgen_scored_at"):
            if rec.get(col) is not None:
                fields[col] = rec[col]
        if row:
            sets = ", ".join(f"{k}=?" for k in fields)
            con.execute(
                f"UPDATE customers SET {sets} WHERE id=?",
                list(fields.values()) + [row["id"]],
            )
            cid = row["id"]
        else:
            cid = _new_id()
            fields["id"] = cid
            fields["tenant_id"] = tenant_id
            fields["created_at"] = now
            fields["is_regulated_industry"] = 0
            fields["has_multiple_erps"] = 0
            fields["in_email_campaign"] = 1 if rec.get("in_email_campaign") else 0
            cols = list(fields.keys())
            con.execute(
                f"INSERT INTO customers ({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)})",
                [fields[c] for c in cols],
            )
        con.commit()
        return cid


def upsert_contact(account_domain: str, rec: Dict[str, Any], return_new: bool = False):
    """Insert or update a contact. Returns the id, or (id, is_new) with return_new=True."""
    result = _upsert_contact(account_domain, rec)
    if return_new:
        return result
    return result[0]


def _upsert_contact(account_domain: str, rec: Dict[str, Any]):
    email = (rec.get("email") or "").strip()
    if not email:
        return None, False
    domain = normalize_domain(account_domain)
    with _connect() as con:
        tenant_id = _tenant_id(con)
        cust = _find_customer(con, tenant_id, rec.get("account_name") or domain, domain)
        if cust is None:
            return None, False
        now = _now()
        pid = rec.get("apollo_person_id")
        row = None
        if pid:
            row = con.execute(
                "SELECT * FROM contacts WHERE apollo_person_id=?", (pid,)
            ).fetchone()
        if row is None:
            row = con.execute(
                "SELECT * FROM contacts WHERE tenant_id=? AND email=?",
                (tenant_id, email),
            ).fetchone()
        first = (rec.get("first_name") or "Unknown").strip() or "Unknown"
        last = (rec.get("last_name") or "Unknown").strip() or "Unknown"
        fields = {
            "customer_id": cust["id"],
            "first_name": first,
            "last_name": last,
            "email": email,
            "job_title": rec.get("title"),
            "apollo_person_id": pid,
            "seniority": rec.get("seniority"),
            "email_status": rec.get("email_status"),
            "linkedin_url": rec.get("linkedin_url"),
            "city": rec.get("city"),
            "country": rec.get("country") or "Germany",
            "lawful_basis": rec.get("lawful_basis") or "legitimate_interest_b2b",
            "metadata_json": rec.get("raw_json"),
            "updated_at": now,
        }
        is_new = row is None
        if row:
            sets = ", ".join(f"{k}=?" for k in fields)
            con.execute(
                f"UPDATE contacts SET {sets} WHERE id=?",
                list(fields.values()) + [row["id"]],
            )
            cid = row["id"]
        else:
            cid = _new_id()
            fields["id"] = cid
            fields["tenant_id"] = tenant_id
            fields["created_at"] = now
            cols = list(fields.keys())
            con.execute(
                f"INSERT INTO contacts ({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)})",
                [fields[c] for c in cols],
            )
        con.commit()
        return cid, is_new


def account_has_contacts(name: str, domain: str) -> bool:
    """True if the CRM already holds this account with at least one contact."""
    domain = normalize_domain(domain)
    with _connect() as con:
        cust = _find_customer(con, _tenant_id(con), name, domain)
        if cust is None:
            return False
        row = con.execute(
            "SELECT 1 FROM contacts WHERE customer_id=? LIMIT 1", (cust["id"],)
        ).fetchone()
        return row is not None
