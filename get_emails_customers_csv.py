#!/usr/bin/env python3
"""
Enrich contacts CSV with Apollo.io People Enrichment (bulk_match).

Usage:
  export APOLLO_API_KEY="YOUR_KEY"
  python apollo_enrich_emails.py input.csv output.csv

Input CSV expected columns (best effort):
  - Person
  - Company (optional)
  - LinkedIn URL (optional)
  - Official Company Email (optional; used to infer domain)
  - Role (optional)

Output:
  Adds columns:
    apollo_match_status, apollo_person_id, apollo_email, apollo_phone,
    apollo_title, apollo_company, apollo_linkedin_url
"""

import os
import sys
import time
import re
import math
from typing import Dict, Any, List, Tuple, Optional

import pandas as pd
import requests
import uuid

# database imports for saving results
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import uuid

# models defined in databases.py
from databases import Base, Customer, Contact

APOLLO_BULK_MATCH_URL = "https://api.apollo.io/api/v1/people/bulk_match"

# ---- helpers ----

def split_name(full_name: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Very simple name split. Returns (first, last) or (None, None) if not usable.
    Skips obvious placeholders like "Head of ..." or "Company page".
    """
    if not isinstance(full_name, str):
        return (None, None)
    n = full_name.strip()

    # Skip placeholders / non-person entries commonly present in your sheet
    bad_prefixes = (
        "head of", "senior", "managing director", "director", "company page",
        "linkedin", "official", "general contact", "office", "showcase",
        "alliance manager", "program director", "practice lead"
    )
    if n.lower().startswith(bad_prefixes) or "via linkedin" in n.lower():
        return (None, None)

    # Remove titles
    n = re.sub(r"^(dr\.|prof\.|mr\.|mrs\.|ms\.)\s+", "", n, flags=re.IGNORECASE).strip()

    parts = [p for p in re.split(r"\s+", n) if p]
    if len(parts) < 2:
        return (None, None)

    first = parts[0]
    last = parts[-1]
    return (first, last)

def extract_domain(company_email: str) -> Optional[str]:
    if not isinstance(company_email, str):
        return None
    company_email = company_email.strip()
    m = re.search(r"@([A-Za-z0-9\.-]+\.[A-Za-z]{2,})$", company_email)
    return m.group(1).lower() if m else None

def apollo_request(payload: Dict[str, Any], api_key: str, timeout: int = 45) -> Dict[str, Any]:
    """
    POST to Apollo with retry/backoff on 429/5xx.
    """
    headers = {
        "Content-Type": "application/json",
        "Cache-Control": "no-cache",
        "X-Api-Key": api_key,
    }

    backoff = 2.0
    for attempt in range(1, 8):  # up to ~2+4+8+... seconds
        resp = requests.post(APOLLO_BULK_MATCH_URL, headers=headers, json=payload, timeout=timeout)

        if resp.status_code == 200:
            return resp.json()

        if resp.status_code in (429, 500, 502, 503, 504):
            # respect Retry-After if present
            retry_after = resp.headers.get("Retry-After")
            if retry_after:
                sleep_s = float(retry_after)
            else:
                sleep_s = backoff
                backoff = min(backoff * 2, 60)
            time.sleep(sleep_s)
            continue

        # other errors: raise
        raise RuntimeError(f"Apollo API error {resp.status_code}: {resp.text[:500]}")

    raise RuntimeError("Apollo API: too many retries (rate limit or server errors).")

def parse_bulk_match_response(resp: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Apollo response shape can vary by plan/features; we try common keys.
    You may need to adjust based on your exact payload/response.
    """
    # Often responses include something like:
    # { "people": [ { "person": {...}, "status": "success" }, ... ] }
    # or { "matches": [...] } etc.
    for key in ("people", "matches", "results", "data"):
        if key in resp and isinstance(resp[key], list):
            return resp[key]
    # fallback: if it returns list at top-level
    if isinstance(resp, list):
        return resp
    return []


def save_to_db(df: pd.DataFrame) -> None:
    """Persist customers and contacts from the DataFrame to the SQL database.

    - Expects `Company` or enriched `apollo_company` to map to Customer.name
    - Contacts are created with available person/email data.
    - `TENANT_ID` env var is used for tenant scoping; falls back to None.
    """
    db_url = os.getenv("DATABASE_URL", "sqlite:///fortlo.db")
    engine = create_engine(db_url, echo=False, future=True)
    # create tables if they don't exist yet
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    tenant_id = None
    tenant_str = os.getenv("TENANT_ID")
    if tenant_str:
        try:
            tenant_id = uuid.UUID(tenant_str)
        except ValueError:
            pass

    for _, row in df.iterrows():
        company = row.get("Company") or row.get("apollo_company")
        if not company or not isinstance(company, str) or not company.strip():
            continue
        company = company.strip()

        # look up existing customer
        query = session.query(Customer).filter(Customer.name == company)
        if tenant_id is not None:
            query = query.filter(Customer.tenant_id == tenant_id)
        cust = query.first()
        if not cust:
            cust = Customer(name=company)
            if tenant_id is not None:
                cust.tenant_id = tenant_id
            session.add(cust)
            session.flush()  # to assign id

        # create contact if we have a person name and email
        first, last = split_name(str(row.get("Person", "")))
        email = (row.get("apollo_email") or row.get("Official Company Email")) or ""
        if first and last and email:
            # avoid duplicate contacts by email
            q2 = session.query(Contact).filter(Contact.email == email)
            if tenant_id is not None:
                q2 = q2.filter(Contact.tenant_id == tenant_id)
            existing = q2.first()
            if not existing:
                contact = Contact(
                    customer_id=cust.id,
                    first_name=first,
                    last_name=last,
                    email=email,
                )
                if tenant_id is not None:
                    contact.tenant_id = tenant_id
                session.add(contact)

    session.commit()
    session.close()

# ---- main ----
from dotenv import load_dotenv
def main():
    if len(sys.argv) < 3:
        print("Usage: python apollo_enrich_emails.py input.csv output.csv", file=sys.stderr)
        sys.exit(2)
    load_dotenv()
    api_key = os.getenv("APOLLO_API_KEY")
    if not api_key:
        print("Missing APOLLO_API_KEY env var.", file=sys.stderr)
        sys.exit(2)

    in_path = sys.argv[1]
    out_path = sys.argv[2]

    # tolerate malformed rows by skipping
    df = pd.read_csv(in_path, on_bad_lines='skip')
    # normalize column names
    df.columns = [c.strip() for c in df.columns]

    # Ensure columns exist
    for col in ["Company", "Person", "Role", "LinkedIn URL", "Official Company Email"]:
        if col not in df.columns:
            df[col] = ""

    # Prepare output columns
    out_cols = [
        "apollo_match_status",
        "apollo_person_id",
        "apollo_email",
        "apollo_phone",
        "apollo_title",
        "apollo_company",
        "apollo_linkedin_url",
    ]
    for c in out_cols:
        if c not in df.columns:
            df[c] = ""

    # Build list of enrichable rows
    enrich_rows = []

    for idx, row in df.iterrows():
        print(row)
        first, last = split_name(str(row.get("Person", " ")))
        print(f"row {row['Person']}")
        if not first or not last:
            continue

        domain = extract_domain(str(row.get("Official Company Email", "")))
        linkedin = str(row.get("LinkedIn URL", "")).strip() or None
        company = str(row.get("Company", "")).strip() or None

        details = {
            "first_name": first,
            "last_name": last,
        }
        # Provide more info increases match probability :contentReference[oaicite:6]{index=6}
        if domain:
            details["domain"] = domain
        if company:
            details["organization_name"] = company
        if linkedin:
            details["linkedin_url"] = linkedin
        print(idx, details)
        enrich_rows.append((idx, details))

    # Batch into chunks of 10 (Apollo bulk limit) :contentReference[oaicite:7]{index=7}
    BATCH_SIZE = 10
    total = len(enrich_rows)
    print(f"Enrichable rows: {total}")

    for start in range(0, total, BATCH_SIZE):
        chunk = enrich_rows[start:start+BATCH_SIZE]
        payload = {
            "details": [d for _, d in chunk],
            # Not returned by default; enable reveal flags :contentReference[oaicite:8]{index=8}
            "reveal_personal_emails": True,
            #"reveal_phone_number": "true",
            # Keep waterfall off here (waterfall can be async via webhook) :contentReference[oaicite:9]{index=9}
            "run_waterfall_email": False,
            "run_waterfall_phone": False,
        }

        resp = apollo_request(payload, api_key)
        
        results = parse_bulk_match_response(resp)

        # Map results back to rows (best-effort: same order)
        for (idx, _), item in zip(chunk, results):
            # try to extract a "person" object
            person_obj = None
            if isinstance(item, dict):
                if "person" in item and isinstance(item["person"], dict):
                    person_obj = item["person"]
                    status = item.get("status") or item.get("match_status") or ""
                else:
                    person_obj = item
                    status = item.get("status") or item.get("match_status") or ""
            else:
                continue

            df.at[idx, "apollo_match_status"] = status

            # common fields
            df.at[idx, "apollo_person_id"] = person_obj.get("id", "") if person_obj else ""
            df.at[idx, "apollo_title"] = person_obj.get("title", "") if person_obj else ""
            df.at[idx, "apollo_company"] = (person_obj.get("organization", {}) or {}).get("name", "") if person_obj else ""

            # emails/phones: keys vary; try common possibilities
            email = person_obj.get("email") if person_obj else None
            if not email and person_obj:
                email = person_obj.get("personal_email") or person_obj.get("email_address")
            df.at[idx, "apollo_email"] = email or ""

            phone = person_obj.get("phone_number") if person_obj else None
            if not phone and person_obj:
                phone = person_obj.get("mobile_phone") or person_obj.get("phone")
            df.at[idx, "apollo_phone"] = phone or ""

            li = person_obj.get("linkedin_url") if person_obj else None
            df.at[idx, "apollo_linkedin_url"] = li or ""

        # gentle pacing (avoid bursts)
        time.sleep(1.0)

        print(f"Processed {min(start+BATCH_SIZE, total)}/{total}")

    df.to_csv(out_path, index=False)
    print(f"Saved: {out_path}")

    # also persist customers/contacts to database
    save_to_db(df)

if __name__ == "__main__":
    main()