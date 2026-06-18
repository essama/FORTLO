#!/usr/bin/env python3
"""Count contacts and companies still eligible for outbound sending.

This mirrors the current selection logic in app3.py:
- contact must have a valid email
- contact must not be in do_not_email.txt
- contact must not already have an EmailLog entry
- contact's customer must be included in the email campaign

It loops over the database records and prints a short summary.
"""

import os
import re
from collections import Counter

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import joinedload, sessionmaker

from databases import Contact, EmailLog


load_dotenv()


def is_valid_email(email: str) -> bool:
    if not isinstance(email, str) or "@" not in email:
        return False
    return re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email.strip()) is not None


def load_do_not_email() -> set[str]:
    path = "do_not_email.txt"
    if not os.path.exists(path):
        return set()

    with open(path, "r", encoding="utf-8") as handle:
        return {line.strip().lower() for line in handle if line.strip()}


def main() -> None:
    database_url = os.getenv("DATABASE_URL", "sqlite:///fortlo.db")
    engine = create_engine(database_url, echo=False, future=True)
    session_local = sessionmaker(bind=engine)

    do_not_email = load_do_not_email()

    with session_local() as session:
        emailed_contact_ids = {
            contact_id
            for (contact_id,) in session.query(EmailLog.contact_id).all()
            if contact_id is not None
        }

        contacts = (
            session.query(Contact)
            .options(joinedload(Contact.customer))
            .all()
        )

    reason_counts = Counter()
    eligible_contacts = []
    eligible_companies = set()

    for contact in contacts:
        if not contact.email:
            reason_counts["missing_email"] += 1
            continue

        if not is_valid_email(contact.email):
            reason_counts["invalid_email"] += 1
            continue

        if contact.email.lower() in do_not_email:
            reason_counts["do_not_email"] += 1
            continue

        if contact.id in emailed_contact_ids:
            reason_counts["already_emailed"] += 1
            continue

        if contact.customer and not contact.customer.in_email_campaign:
            reason_counts["customer_out_of_campaign"] += 1
            continue

        eligible_contacts.append(contact)
        if contact.customer and contact.customer.name:
            eligible_companies.add(contact.customer.name)

    print(f"Eligible contacts: {len(eligible_contacts)}")
    print(f"Eligible companies: {len(eligible_companies)}")
    print(f"Total contacts checked: {len(contacts)}")

    if reason_counts:
        print("\nSkipped contacts by reason:")
        for reason, count in sorted(reason_counts.items()):
            print(f"- {reason}: {count}")

    if eligible_contacts:
        print("\nFirst 20 eligible contacts:")
        for contact in eligible_contacts[:20]:
            company_name = contact.customer.name if contact.customer else ""
            full_name = " ".join(
                part for part in [contact.first_name, contact.last_name] if part
            ).strip()
            print(f"- {full_name} | {contact.email} | {company_name}")


if __name__ == "__main__":
    main()