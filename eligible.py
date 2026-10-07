"""Shared eligible-contact counting for sender, briefing, and UI."""
from __future__ import annotations

import math
import os
import re
from typing import Any, Dict, List, Set

from dotenv import load_dotenv
from sqlalchemy.orm import joinedload

from databases import Contact, EmailLog

load_dotenv()

ALLOWED_EMAIL_STATUS = {"verified", "likely to engage"}
DAILY_LIMIT = int(os.getenv("DAILY_LIMIT", "50"))
MAX_PER_COMPANY_PER_DAY = int(os.getenv("MAX_PER_COMPANY_PER_DAY", "2"))


def is_valid_email(email: str) -> bool:
    if not isinstance(email, str) or "@" not in email:
        return False
    return re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email.strip()) is not None


def load_do_not_email(path: str = "do_not_email.txt") -> Set[str]:
    if not os.path.exists(path):
        return set()
    with open(path, "r", encoding="utf-8") as handle:
        return {
            line.strip().lower()
            for line in handle
            if line.strip() and not line.strip().startswith("#")
        }


def _outreach_blocked() -> tuple:
    """People who replied, bounced, opted out or left, and companies where someone replied (outreach.py)."""
    try:
        import outreach

        con = outreach.connect()
        try:
            return outreach.blocked(con)
        finally:
            con.close()
    except Exception:
        return set(), set()


def load_eligible_rows(session, do_not_email: Set[str] | None = None) -> List[Dict[str, Any]]:
    blocked = do_not_email if do_not_email is not None else load_do_not_email()
    stopped_emails, stopped_domains = _outreach_blocked()
    emailed_ids = {
        contact_id
        for (contact_id,) in session.query(EmailLog.contact_id).all()
        if contact_id is not None
    }
    rows: List[Dict[str, Any]] = []
    contacts = session.query(Contact).options(joinedload(Contact.customer)).all()
    for contact in contacts:
        if not contact.email or not is_valid_email(contact.email):
            continue
        if contact.email.lower() in blocked:
            continue
        if contact.email.lower() in stopped_emails or contact.email.lower().split("@")[-1] in stopped_domains:
            continue
        if contact.id in emailed_ids:
            continue
        customer = contact.customer
        if customer and not customer.in_email_campaign:
            continue
        icp = getattr(customer, "icp_status", None) if customer else None
        if icp != "qualified":
            continue
        status = (getattr(contact, "email_status", None) or "").strip().lower()
        if status not in ALLOWED_EMAIL_STATUS:
            continue
        rows.append(
            {
                "contact_id": contact.id,
                "person_id": str(contact.id),
                "email": contact.email,
                "first_name": getattr(contact, "first_name", "") or "",
                "last_name": getattr(contact, "last_name", "") or "",
                "organization_name": customer.name if customer else "",
                "title": contact.job_title or "",
            }
        )
    return rows


def collect_stats(session, sent_today: int = 0) -> Dict[str, Any]:
    rows = load_eligible_rows(session)
    companies = sorted({row["organization_name"] for row in rows if row.get("organization_name")})
    daily_capacity = min(DAILY_LIMIT, MAX_PER_COMPANY_PER_DAY * max(len(companies), 1)) if rows else 0
    remaining_today = max(0, DAILY_LIMIT - int(sent_today or 0))
    if daily_capacity:
        daily_capacity = min(daily_capacity, remaining_today + MAX_PER_COMPANY_PER_DAY * max(len(companies), 1))
        daily_capacity = min(DAILY_LIMIT, MAX_PER_COMPANY_PER_DAY * max(len(companies), 1))
    days_until_refuel = math.ceil(len(rows) / daily_capacity) if daily_capacity else 0
    return {
        "eligible_contacts": len(rows),
        "eligible_companies": len(companies),
        "companies": companies,
        "daily_limit": DAILY_LIMIT,
        "per_company_per_day": MAX_PER_COMPANY_PER_DAY,
        "daily_capacity": daily_capacity,
        "days_until_refuel": days_until_refuel,
        "sent_today": int(sent_today or 0),
        "needs_refuel": len(rows) == 0,
        "rows": rows,
    }
