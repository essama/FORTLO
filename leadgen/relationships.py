"""Link recruiting companies (CRM clients) to end customers (profile)."""

from __future__ import annotations

import json
import re
from pathlib import Path

LINKS_FILE = Path(__file__).resolve().parent / "data" / "relationships.json"


def load_links(path: Path = LINKS_FILE) -> dict:
    """Contract and suggested links, plus delivery partners. Edit the JSON, not this module."""
    if not path.exists():
        return {"contract": [], "suggested": [], "not_staffing": [], "delivery_partners": []}
    data = json.loads(path.read_text(encoding="utf-8"))
    for key in ("contract", "suggested", "not_staffing", "delivery_partners"):
        data.setdefault(key, [])
    return data


_LINKS = load_links()
CONTRACT_LINKS = _LINKS["contract"]
SUGGESTED_LINKS = _LINKS["suggested"]
NOT_STAFFING = set(_LINKS["not_staffing"])
DELIVERY_PARTNERS = _LINKS["delivery_partners"]


def normalize(name: str) -> str:
    text = (name or "").lower()
    text = text.replace("&", " ")
    text = re.sub(r"\b(gmbh|ag|ug|ltd|limited|inc|llc|d\.o\.o|doo|se|sa|bv)\b", " ", text)
    text = re.sub(r"[^a-z0-9]+", "", text)
    return text


def _same(left: str, right: str) -> bool:
    a, b = normalize(left), normalize(right)
    if not a or not b:
        return False
    return a == b or a in b or b in a


def _find_client(companies: list[dict], name: str) -> dict | None:
    for company in companies:
        if _same(company["company_name"], name):
            return company
    return None


def _invoice_total(invoices: list[dict], client_id: str) -> dict:
    rows = [row for row in invoices if row["client_id"] == client_id]
    net = round(sum(row["net_price"] for row in rows), 2)
    return {
        "invoice_count": len(rows),
        "net_eur": net,
        "first_date": min((row["date"] for row in rows), default=""),
        "last_date": max((row["date"] for row in rows), default=""),
    }


def build_relationships(engagements: list[dict], companies: list[dict], invoices: list[dict]) -> dict:
    confirmed = []
    seen = set()

    for engagement in engagements:
        if not engagement.get("via"):
            continue
        client = _find_client(companies, engagement["via"])
        key = (normalize(engagement["via"]), normalize(engagement["company"]))
        seen.add(key)
        billing = _invoice_total(invoices, client["source_id"]) if client else {
            "invoice_count": 0, "net_eur": 0, "first_date": "", "last_date": ""
        }
        contract = next(
            (
                item for item in CONTRACT_LINKS
                if _same(item["recruiting_company"], engagement["via"])
                and _same(item["end_customer"], engagement["company"])
            ),
            None,
        )
        evidence = f"Profile line names the recruiting company: {engagement['line']}"
        confidence = 0.9
        if contract:
            evidence = contract["evidence"] + " " + evidence
            confidence = contract["confidence"]
        elif client is None:
            evidence += " This recruiting company is on the profile and is not a row in the EC2 clients table."
        confirmed.append(
            {
                "recruiting_company": client["company_name"] if client else engagement["via"],
                "recruiting_contact": client["contact_name"] if client else "",
                "end_customer": engagement["company"],
                "end_customer_title": engagement["title"],
                "end_customer_dates": engagement["dates"],
                "confidence": confidence,
                "confirmed": True,
                "in_ec2_clients": client is not None,
                "billing": billing,
                "evidence": evidence,
            }
        )

    for contract in CONTRACT_LINKS:
        key = (normalize(contract["recruiting_company"]), normalize(contract["end_customer"]))
        if key in seen:
            continue
        client = _find_client(companies, contract["recruiting_company"])
        engagement = next(
            (item for item in engagements if _same(item["company"], contract["end_customer"])),
            None,
        )
        billing = _invoice_total(invoices, client["source_id"]) if client else {
            "invoice_count": 0, "net_eur": 0, "first_date": "", "last_date": ""
        }
        confirmed.append(
            {
                "recruiting_company": client["company_name"] if client else contract["recruiting_company"],
                "recruiting_contact": client["contact_name"] if client else "",
                "end_customer": engagement["company"] if engagement else contract["end_customer"],
                "end_customer_title": engagement["title"] if engagement else "",
                "end_customer_dates": engagement["dates"] if engagement else "",
                "confidence": contract["confidence"],
                "confirmed": True,
                "in_ec2_clients": client is not None,
                "billing": billing,
                "evidence": contract["evidence"],
            }
        )
        seen.add(key)

    suggested = []
    for item in SUGGESTED_LINKS:
        if any(_same(item["end_customer"], row["end_customer"]) and row["confirmed"] for row in confirmed):
            if _same(item["end_customer"], "ebm-papst"):
                continue
        if any(
            _same(item["recruiting_company"], row["recruiting_company"])
            and _same(item["end_customer"], row["end_customer"])
            for row in confirmed
        ):
            continue
        client = _find_client(companies, item["recruiting_company"])
        billing = _invoice_total(invoices, client["source_id"]) if client else {
            "invoice_count": 0, "net_eur": 0, "first_date": "", "last_date": ""
        }
        suggested.append(
            {
                "recruiting_company": client["company_name"] if client else item["recruiting_company"],
                "recruiting_contact": client["contact_name"] if client else "",
                "end_customer": item["end_customer"],
                "confidence": item["confidence"],
                "confirmed": False,
                "in_ec2_clients": client is not None,
                "billing": billing,
                "evidence": item["evidence"],
            }
        )

    linked_clients = {normalize(row["recruiting_company"]) for row in confirmed + suggested}
    linked_customers = {normalize(row["end_customer"]) for row in confirmed}
    unresolved_clients = []
    for company in companies:
        token = normalize(company["company_name"])
        if token in linked_clients:
            continue
        if any(token.startswith(skip) or skip in token for skip in NOT_STAFFING):
            unresolved_clients.append(
                {
                    **company,
                    "reason": "Invoiced client, but not a recruiting company and not an end customer on the profile. Left out of the ICP.",
                }
            )
            continue
        unresolved_clients.append(
            {
                **company,
                "reason": "Recruiting company is in the EC2 clients table. No end customer is confirmed.",
            }
        )

    unresolved_customers = []
    for engagement in engagements:
        if normalize(engagement["company"]) in linked_customers:
            continue
        if any(_same(engagement["company"], row["end_customer"]) for row in suggested):
            continue
        unresolved_customers.append(
            {
                "end_customer": engagement["company"],
                "dates": engagement["dates"],
                "reason": "End customer is on the profile. No recruiting company is confirmed.",
            }
        )

    return {
        "confirmed": confirmed,
        "suggested": suggested,
        "unresolved_clients": unresolved_clients,
        "unresolved_customers": unresolved_customers,
    }


def cv_client_check(profile_engagements: list[dict], cv_text: str) -> dict:
    """Clients named on the profile must also appear on the CV."""
    names = []
    for engagement in profile_engagements:
        if engagement.get("via") and engagement["via"] not in names:
            names.append(engagement["via"])
    present = [name for name in names if name.lower() in cv_text.lower()]
    missing = [name for name in names if name.lower() not in cv_text.lower()]
    return {"profile_clients": names, "present_on_cv": present, "missing_on_cv": missing}
