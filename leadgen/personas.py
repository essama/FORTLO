"""Buyer personas tied to won accounts. Copy lives in data/personas.json; people come from the data.

Unnamed seats stay unnamed. No email address is ever attached to a persona.
"""

from __future__ import annotations

import json
from pathlib import Path

from leadgen.relationships import DELIVERY_PARTNERS, normalize

PERSONAS_FILE = Path(__file__).resolve().parent / "data" / "personas.json"

NO_OBJECTIONS = (
    "No objection text and no lost-reason field on these wins. Do not invent an objection."
)


def load_persona_copy(path: Path = PERSONAS_FILE) -> list[dict]:
    if not path.exists():
        return []
    return list(json.loads(path.read_text(encoding="utf-8")).get("personas") or [])


def _tied(wins: list[dict], words: list[str]) -> list[str]:
    return [
        item["company"]
        for item in wins
        if any(word in (item.get("title") or "").lower() for word in words)
    ]


def build_personas(
    engagements: list[dict],
    relationships: dict,
    historical_contacts: list[dict] | None = None,
    channel: list[dict] | None = None,
) -> list[dict]:
    wins = [item for item in engagements if item.get("outcome") == "won_retained"]
    historical_contacts = historical_contacts or []
    channel = channel or []
    copy = {row["type"]: row for row in load_persona_copy()}

    def base(kind: str) -> dict:
        row = dict(copy.get(kind) or {"type": kind, "match_titles": []})
        tied = _tied(wins, row.pop("match_titles", []) or [])
        row.setdefault("title_patterns", "")
        row.setdefault("department", "")
        row.setdefault("cares_about", "")
        row.setdefault("message_angle", "")
        row["type"] = kind
        row["tied_accounts"] = tied
        row["objections"] = NO_OBJECTIONS
        return row

    # Decision-makers: the historical contacts on won end customers.
    decision = base("Decision-maker")
    named_accounts = {row["end_customer"] for row in historical_contacts if row.get("name") and row.get("end_customer")}
    decision["named_people"] = [
        {
            "name": row["name"],
            "title": row.get("role") or "Decision-maker",
            "org": row.get("company") or row.get("end_customer") or "",
            "end_customer": row.get("end_customer") or "",
            "source": row.get("source") or "",
        }
        for row in historical_contacts
        if row.get("name")
    ]
    decision["tied_accounts"] = decision["tied_accounts"] or sorted(named_accounts)
    still_unnamed = [item["company"] for item in wins if item["company"] not in named_accounts]
    decision["named_gap"] = (
        f"Decision-makers are stored for {len(named_accounts)} of {len(wins)} won accounts. "
        f"Still unnamed: {', '.join(still_unnamed) or 'none'}. No email is stored; these people are not on the send list."
    )

    # Champions: no one inside a won customer is named yet. Delivery partners are listed separately.
    champion = base("Internal champion")
    champion["named_people"] = []
    champion["named_gap"] = "No champion inside a won customer is named. The seat is described from the engagement bullets."

    # Agency account managers: the people who sign the invoices.
    agency = base("Agency account manager")
    agency["tied_accounts"] = [row["agency"] for row in channel]
    agency["named_people"] = [
        {
            "name": row["contact"],
            "title": "Client contact / invoice addressee",
            "org": row["agency"],
            "end_customer": ", ".join(row["end_customers"]) or "not linked yet",
            "source": f"{row['invoices']} invoices, {row['first']} to {row['last']}",
        }
        for row in channel
        if row.get("contact")
    ] + [
        {**partner, "title": partner["title"] + " (delivery partner, not an agency)"}
        for partner in DELIVERY_PARTNERS
    ]
    linked = {normalize(row["recruiting_company"]) for row in relationships.get("confirmed") or []}
    unlinked = [row["agency"] for row in channel if normalize(row["agency"]) not in linked]
    agency["named_gap"] = (
        "Agencies with invoices but no confirmed end customer: " + (", ".join(unlinked) or "none") + "."
    )
    return [decision, champion, agency]
