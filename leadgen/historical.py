"""Historical end-customer contacts for profile analysis. Not an email store."""

from __future__ import annotations

import json
from pathlib import Path


def load_historical_contacts(path: Path) -> list[dict]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("contacts") if isinstance(payload, dict) else payload
    if not isinstance(rows, list):
        return []
    out = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        out.append(
            {
                "company": (row.get("company") or "").strip(),
                "end_customer": (row.get("end_customer") or row.get("company") or "").strip(),
                "name": (row.get("name") or "").strip(),
                "role": (row.get("role") or "Decision-maker").strip(),
                "source": (row.get("source") or "").strip(),
            }
        )
    return out
