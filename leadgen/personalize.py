"""Personalised email openers (Phase C) from sourced buying signals.

Only contacts at companies with stored signals (leadgen/signals.py) get a draft: without a real,
dated, sourced fact there is nothing honest to personalise with, and the standard email is used.
Claude writes one or two formal German sentences that state the public fact and link it to SAP MDG.
Drafts are stored in fortlo.db `outreach_drafts` and, by default, need approval on /outreach.
"""

from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timezone

from leadgen import signals as signal_store

SYSTEM = (
    "You write the first one or two sentences of a cold email in German from Essam Azzam, an SAP Master "
    "Data Governance (MDG) architect, to a manager at a DACH company. For each item in the user JSON, use "
    "exactly one of its `facts` (public, dated, sourced). State the fact neutrally, e.g. 'ich habe gesehen, "
    "dass ...' or 'laut ...', and connect it in a few words to master data or SAP MDG. Formal 'Sie'. "
    "At most 2 sentences and 45 words. No flattery, no exclamation marks, no questions (the email asks one "
    "next), no claims about results, no other client names, no URLs, nothing that is not in the fact. "
    "Do not start with a greeting (the greeting is already there). "
    "Return one JSON object and no markdown: {\"drafts\": [{\"key\", \"opener\", \"fact_index\"}]}."
)
BATCH = 8
FORMAL = re.compile(r"\b(Sie|Ihr|Ihre|Ihrem|Ihren|Ihrer|Ihnen)\b")
BAD = re.compile(r"(https?://|www\.|!|\?)")


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def candidates(con: sqlite3.Connection, limit: int = 30) -> list[dict]:
    """Unsent campaign contacts without a draft whose company has stored signals."""
    rows = con.execute(
        """
        SELECT c.id AS contact_id, c.email, c.first_name, c.job_title, cu.name AS company, cu.domain
        FROM contacts c JOIN customers cu ON cu.id = c.customer_id
        WHERE cu.in_email_campaign = 1 AND cu.icp_status = 'qualified'
          AND c.id NOT IN (SELECT contact_id FROM email_logs WHERE contact_id IS NOT NULL)
          AND CAST(c.id AS TEXT) NOT IN (SELECT contact_id FROM outreach_drafts)
        ORDER BY c.created_at DESC
        """
    ).fetchall()
    out = []
    for row in rows:
        facts = [
            {"type": s["type"], "fact": s.get("summary", ""), "date": s.get("date", ""), "source_url": s.get("source_url", "")}
            for s in signal_store.cached((row["domain"] or "").lower())
            if s.get("type") in signal_store.POINTS and s.get("type") != "hiring_md_ops"
        ][:4]
        if facts:
            out.append({**dict(row), "contact_id": str(row["contact_id"]), "facts": facts})
        if len(out) >= limit:
            break
    return out


def clean_opener(text: str) -> str | None:
    text = re.sub(r"\s+", " ", str(text or "")).strip()
    if not (30 <= len(text) <= 320) or BAD.search(text) or not FORMAL.search(text):
        return None
    if len(re.findall(r"[.!?](\s|$)", text)) > 2:
        return None
    return text


def draft(con: sqlite3.Connection, client, items: list[dict]) -> dict:
    """Ask Claude for openers in batches; store valid ones as status 'draft'. Returns counts."""
    from leadgen.model_review import _parse_json

    summary = {"requested": len(items), "drafted": 0, "rejected": 0, "errors": []}
    if client is None:
        summary["errors"].append("ANTHROPIC_API_KEY not set")
        return summary
    for start in range(0, len(items), BATCH):
        batch = items[start:start + BATCH]
        payload = {"items": [
            {"key": item["contact_id"], "company": item["company"], "contact_title": item.get("job_title") or "",
             "facts": [{"type": f["type"], "fact": f["fact"], "date": f["date"]} for f in item["facts"]]}
            for item in batch
        ]}
        try:
            parsed = _parse_json(client.complete(payload, system=SYSTEM))
        except Exception as exc:
            summary["errors"].append(f"{exc.__class__.__name__}: {str(exc)[:120]}")
            continue
        by_key = {item["contact_id"]: item for item in batch}
        for row in parsed.get("drafts") or []:
            item = by_key.get(str(row.get("key")))
            if not item:
                continue
            opener = clean_opener(row.get("opener"))
            try:
                fact = item["facts"][int(row.get("fact_index", 0))]
            except (ValueError, IndexError, TypeError):
                fact = item["facts"][0]
            if not opener:
                summary["rejected"] += 1
                continue
            con.execute(
                "INSERT OR REPLACE INTO outreach_drafts VALUES (?,?,?,?,?,?,?,?,?,?)",
                (item["contact_id"], item["email"], item["company"], item.get("job_title") or "", opener,
                 json.dumps(fact, ensure_ascii=False), "draft", getattr(client, "model", ""), _now(), None),
            )
            summary["drafted"] += 1
        con.commit()
    return summary


def decide(con: sqlite3.Connection, contact_id: str, action: str, opener: str | None = None) -> str:
    """approve / reject / save (edited text, stays a draft). Returns the new status or an error."""
    row = con.execute("SELECT status FROM outreach_drafts WHERE contact_id=?", (contact_id,)).fetchone()
    if not row:
        return "missing"
    if row[0] == "sent":
        return "already sent"
    if opener is not None:
        text = clean_opener(opener)
        if not text:
            return "invalid text (30-320 characters, formal Sie, no URL, no ! or ?)"
        con.execute("UPDATE outreach_drafts SET opener=? WHERE contact_id=?", (text, contact_id))
    status = {"approve": "approved", "reject": "rejected", "save": "draft"}.get(action)
    if not status:
        return "unknown action"
    con.execute("UPDATE outreach_drafts SET status=?, decided_at=? WHERE contact_id=?", (status, _now(), contact_id))
    con.commit()
    return status
