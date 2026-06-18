"""
Export sent/processed emails to CSV.
Scans:
 - recruiter_responses.sqlite (table: processed_emails)
 - outreach_log.sqlite (table: sent) -- optional, produced by other scripts

Outputs: sent_emails_export.csv (in current directory by default)

Usage:
  python export_sent_emails.py [output.csv]
"""
import csv
import os
import sqlite3
import sys
from typing import List, Dict

OUT_PATH_DEFAULT = "sent_emails_export.csv"

DB1 = os.getenv("DB_PATH", "recruiter_responses.sqlite")
DB2 = os.getenv("OUTREACH_DB", "outreach_log.sqlite")


def fetch_from_recruiter_db(path: str) -> List[Dict]:
    rows = []
    if not os.path.exists(path):
        return rows
    con = sqlite3.connect(path)
    cur = con.cursor()
    try:
        cur.execute("SELECT sender_name, sender_email, subject, received_date, processed_date FROM processed_emails")
    except sqlite3.OperationalError:
        con.close()
        return rows
    for r in cur.fetchall():
        name, email, subject, received_date, processed_date = r
        rows.append({
            "name": name or "",
            "company": "",
            "email": email or "",
            "subject": subject or "",
            "received_date": received_date or "",
            "processed_date": processed_date or "",
            "source": os.path.basename(path)
        })
    con.close()
    return rows


def fetch_from_outreach_db(path: str) -> List[Dict]:
    rows = []
    if not os.path.exists(path):
        return rows
    con = sqlite3.connect(path)
    cur = con.cursor()
    try:
        cur.execute("SELECT sent_at, email, person_id, company, subject, status FROM sent")
    except sqlite3.OperationalError:
        con.close()
        return rows
    for r in cur.fetchall():
        sent_at, email, person_id, company, subject, status = r
        # person_id may be null; name not available in this table
        rows.append({
            "name": person_id or "",
            "company": company or "",
            "email": email or "",
            "subject": subject or "",
            "received_date": "",
            "processed_date": sent_at or "",
            "source": os.path.basename(path)
        })
    con.close()
    return rows


def dedupe_rows(rows: List[Dict]) -> List[Dict]:
    seen = set()
    out = []
    for r in rows:
        key = (r.get("email",""), r.get("company",""), r.get("subject",""), r.get("processed_date",""))
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def main(argv):
    out_path = argv[1] if len(argv) > 1 else OUT_PATH_DEFAULT

    rows = []
    rows.extend(fetch_from_recruiter_db(DB1))
    rows.extend(fetch_from_outreach_db(DB2))

    rows = dedupe_rows(rows)

    # Ensure directory exists
    out_dir = os.path.dirname(os.path.abspath(out_path))
    os.makedirs(out_dir, exist_ok=True)

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["name", "company", "email", "subject", "received_date", "processed_date", "source"])
        writer.writeheader()
        for r in rows:
            writer.writerow(r)

    print(f"Wrote {len(rows)} rows to {out_path}")


if __name__ == "__main__":
    main(sys.argv)
