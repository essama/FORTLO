"""Phase C outreach: reply tracking, follow-up sequence, personalised openers.

Config: `outreach` in config.yaml. Everything here is called from app3.py inside try/except, so a
failure never stops the sender.

  replies     Reads the inbox (Graph Mail.Read). Classifies each message from a contacted address or
              domain as reply / auto_reply / bounce / unsubscribe. A human reply, bounce or opt-out
              stops every further email to that person; a human reply also stops the whole company.
  sequence    Follow-ups as "Re: <first subject>" after N days, only for contacts whose first email
              went out on or after `start_date`, stopped by any reply.
  openers     Grok-written first sentences from sourced buying signals (leadgen/personalize.py),
              used only once approved on the leadgen /outreach page (unless require_approval: false).

Tables live in fortlo.db: outreach_events, outreach_sequence, outreach_drafts.
"""
from __future__ import annotations

import html
import os
import re
import sqlite3
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
import yaml

HERE = os.path.dirname(os.path.abspath(__file__))
GRAPH = "https://graph.microsoft.com/v1.0"

AUTO_SUBJECT = re.compile(
    r"^(automatische antwort|automatic reply|auto(matic)?[- ]?reply|autoreply|out of (the )?office|abwesenheit|"
    r"abwesend|r[ée]ponse automatique|risposta automatica|respuesta autom[aá]tica|ooo\b)", re.I)
AUTO_BODY = re.compile(
    r"(nicht im b[üu]ro|abwesend|im urlaub|nicht erreichbar|nicht verf[üu]gbar|wieder (im haus|im b[üu]ro|erreichbar|"
    r"verf[üu]gbar)|ausgeflogen|not in the office|out of (the )?office|out of office|on (vacation|holiday|leave)|"
    r"currently away|not available|limited access to (my )?e-?mail|will not be forwarded|wird nicht weitergeleitet|"
    r"ihre nachricht ist eingegangen)", re.I)
BOUNCE_SUBJECT = re.compile(
    r"(undeliverable|unzustellbar|delivery (status notification|has failed|failure)|mail delivery failed|"
    r"returned mail|nicht zugestellt|zustellung fehlgeschlagen|couldn't be delivered|message notification)", re.I)
TEMPORARY = re.compile(r"(\(delay\)|delayed|delivery incomplete|temporary problem|verz[öo]gert)", re.I)
BOUNCE_SENDER = re.compile(r"^(mailer-daemon|postmaster|microsoftexchange|no-?reply-?dsn)", re.I)
UNSUB = re.compile(
    r"(abmelden|abbestellen|austragen|bitte (keine|nicht mehr)|kein(e)? (weiteren )?(e-?mails|nachrichten|interesse)|"
    r"unsubscribe|remove me|do not (contact|email)|stop (emailing|contacting)|not interested|no interest)", re.I)
LEFT = re.compile(
    r"(nicht mehr (für|bei|im unternehmen|t[äa]tig)|no longer (with|at|employed|working)|has left|"
    r"(habe|hat) .{0,40} verlassen|i am retired|im ruhestand|in rente|successor|nachfolger)", re.I)
PREFIX = re.compile(
    r"^\s*((re|aw|wg|fw|fwd|antw|antwort|sv|vs)\s*:|automatische antwort\s*:|automatic reply\s*:|"
    r"out of office\s*(re\s*:)?|\[extern(al)?\]\s*-?)\s*", re.I)
EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+(\.[\w-]+)+")


# --------------------------------------------------------------------- config / db

def load_cfg() -> dict:
    try:
        with open(os.path.join(HERE, "config.yaml"), "r", encoding="utf-8") as handle:
            return (yaml.safe_load(handle) or {}).get("outreach") or {}
    except OSError:
        return {}


def db_path() -> str:
    url = os.getenv("DATABASE_URL", "sqlite:///fortlo.db")
    path = url.split("sqlite:///", 1)[-1]
    return path if os.path.isabs(path) else os.path.join(HERE, path)


def connect(path: Optional[str] = None) -> sqlite3.Connection:
    con = sqlite3.connect(path or db_path(), timeout=30)
    con.row_factory = sqlite3.Row
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS outreach_events (
            message_id TEXT PRIMARY KEY,
            email TEXT, domain TEXT, kind TEXT, received_at TEXT, subject TEXT, preview TEXT,
            created_at TEXT
        );
        CREATE INDEX IF NOT EXISTS ix_outreach_events_email ON outreach_events(email);
        CREATE INDEX IF NOT EXISTS ix_outreach_events_domain ON outreach_events(domain);
        CREATE TABLE IF NOT EXISTS outreach_sequence (
            email TEXT PRIMARY KEY,
            contact_id TEXT, customer_id TEXT, first_name TEXT, company TEXT,
            first_subject TEXT, first_sent_at TEXT, step INTEGER, last_sent_at TEXT,
            status TEXT, stopped_reason TEXT, updated_at TEXT
        );
        CREATE TABLE IF NOT EXISTS outreach_drafts (
            contact_id TEXT PRIMARY KEY,
            email TEXT, company TEXT, title TEXT, opener TEXT, facts TEXT, status TEXT,
            model TEXT, created_at TEXT, decided_at TEXT
        );
        """
    )
    return con


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def uid(value: Any) -> str:
    """SQLite stores the CRM UUIDs as 32 hex characters without dashes."""
    return str(value or "").replace("-", "").lower()


def _domain(email: str) -> str:
    return (email or "").split("@")[-1].strip().lower()


# --------------------------------------------------------------------- replies

def contacted(con: sqlite3.Connection) -> Tuple[set, set]:
    """(emails, domains) FORTLO has ever emailed."""
    rows = con.execute("SELECT DISTINCT lower(email_address) FROM email_logs").fetchall()
    emails = {r[0] for r in rows if r[0]}
    return emails, {_domain(e) for e in emails}


def base_subject(subject: str) -> str:
    text = subject or ""
    for _ in range(4):
        new = PREFIX.sub("", text)
        if new == text:
            break
        text = new
    return re.sub(r"\s+", " ", text).strip().lower()


def sent_subjects(con: sqlite3.Connection) -> set:
    rows = con.execute("SELECT DISTINCT subject FROM email_logs").fetchall()
    return {base_subject(r[0]) for r in rows if r[0]}


def about_outreach(subject: str, subjects: Optional[set]) -> bool:
    """True when the message answers one of our outreach emails (not some other thread with that company)."""
    if subjects is None:
        return True
    base = base_subject(subject)
    return bool(base) and (base in subjects or any(s and s in base for s in subjects))


def classify(msg: dict, emails: set, domains: set, subjects: Optional[set] = None) -> Optional[Tuple[str, str]]:
    """(kind, email) for an inbox message that answers our outreach, else None."""
    sender = ((msg.get("from") or {}).get("emailAddress") or {}).get("address") or ""
    sender = sender.lower()
    subject = msg.get("subject") or ""
    preview = msg.get("bodyPreview") or ""
    if BOUNCE_SENDER.match(sender) or BOUNCE_SUBJECT.search(subject):
        if TEMPORARY.search(subject) or TEMPORARY.search(preview[:200]):
            return None  # delayed, not failed: the server keeps retrying
        hit = next((m.group(0).lower() for m in EMAIL_RE.finditer(preview) if m.group(0).lower() in emails), None)
        return ("bounce", hit) if hit else None
    if sender not in emails and _domain(sender) not in domains:
        return None
    if not about_outreach(subject, subjects):
        return None
    headers = {h.get("name", "").lower(): str(h.get("value", "")).lower()
               for h in msg.get("internetMessageHeaders") or []}
    auto = (headers.get("auto-submitted", "no") not in ("", "no") or "x-autoreply" in headers
            or "x-autorespond" in headers or AUTO_SUBJECT.search(subject))
    if auto or AUTO_BODY.search(preview[:300]):
        return ("left" if LEFT.search(preview) else "auto_reply", sender)
    if LEFT.search(preview[:300]):
        return "left", sender
    if UNSUB.search(subject) or UNSUB.search(preview[:400]):
        return "unsubscribe", sender
    return "reply", sender


def fetch_inbox(token: str, upn: str, since: datetime, limit: int = 200) -> List[dict]:
    url = f"{GRAPH}/users/{upn}/mailFolders/inbox/messages"
    params = {
        "$filter": f"receivedDateTime ge {since.strftime('%Y-%m-%dT%H:%M:%SZ')}",
        "$select": "id,subject,from,receivedDateTime,bodyPreview,internetMessageHeaders",
        "$orderby": "receivedDateTime desc",
        "$top": "50",
    }
    out: List[dict] = []
    headers = {"Authorization": f"Bearer {token}"}
    while url and len(out) < limit:
        resp = requests.get(url, headers=headers, params=params, timeout=60)
        resp.raise_for_status()
        body = resp.json()
        out.extend(body.get("value") or [])
        url, params = body.get("@odata.nextLink"), None
    return out[:limit]


def scan_replies(con: sqlite3.Connection, messages: Iterable[dict]) -> List[dict]:
    """Store new events; apply stops. Returns the new events (dicts)."""
    emails, domains = contacted(con)
    subjects = sent_subjects(con)
    new = []
    for msg in messages:
        mid = msg.get("id")
        if not mid or con.execute("SELECT 1 FROM outreach_events WHERE message_id=?", (mid,)).fetchone():
            continue
        hit = classify(msg, emails, domains, subjects)
        if not hit:
            continue
        kind, email = hit
        event = {
            "message_id": mid, "email": email, "domain": _domain(email), "kind": kind,
            "received_at": msg.get("receivedDateTime") or "", "subject": (msg.get("subject") or "")[:300],
            "preview": (msg.get("bodyPreview") or "")[:400], "created_at": _now(),
        }
        con.execute(
            "INSERT INTO outreach_events VALUES (:message_id,:email,:domain,:kind,:received_at,:subject,:preview,:created_at)",
            event,
        )
        _apply_stop(con, event)
        new.append(event)
    con.commit()
    return new


def _apply_stop(con: sqlite3.Connection, event: dict) -> None:
    kind = event["kind"]
    if kind == "auto_reply":
        return
    reason = {"reply": "replied", "bounce": "bounced", "unsubscribe": "unsubscribed", "left": "left_company"}[kind]
    con.execute(
        "UPDATE outreach_sequence SET status=?, stopped_reason=?, updated_at=? WHERE email=? AND status='active'",
        (reason, f"{kind} {event['received_at'][:10]}", _now(), event["email"]),
    )
    if kind in ("reply", "unsubscribe"):
        # nobody else at that company gets a follow-up while a conversation (or an opt-out) is open
        con.execute(
            "UPDATE outreach_sequence SET status='stopped', stopped_reason=?, updated_at=? "
            "WHERE status='active' AND email LIKE ?",
            (f"company {kind} {event['received_at'][:10]}", _now(), "%@" + event["domain"]),
        )


def blocked(con: sqlite3.Connection) -> Tuple[set, set]:
    """(emails, domains) that must not get any further email: replied, bounced, opted out, left;
    domains where someone replied or opted out."""
    rows = con.execute("SELECT kind, email, domain FROM outreach_events WHERE kind != 'auto_reply'").fetchall()
    emails = {r["email"] for r in rows if r["email"]}
    domains = {r["domain"] for r in rows if r["kind"] in ("reply", "unsubscribe") and r["domain"]}
    return emails, domains


def is_blocked(con: sqlite3.Connection, email: str) -> Optional[str]:
    emails, domains = blocked(con)
    email = (email or "").lower()
    if email in emails:
        return "person replied, bounced or opted out"
    if _domain(email) in domains:
        return "someone at this company replied or opted out"
    return None


def apply_crm_status(con: sqlite3.Connection, events: List[dict]) -> None:
    """Mark the customer in the CRM when a person replies (shown in the FORTLO UI)."""
    for event in events:
        if event["kind"] not in ("reply", "unsubscribe"):
            continue
        row = con.execute(
            "SELECT customer_id FROM email_logs WHERE lower(email_address)=? ORDER BY sent_at DESC LIMIT 1",
            (event["email"],),
        ).fetchone()
        if not row:
            continue
        status = ("Replied" if event["kind"] == "reply" else "Opted out") + f" on {event['received_at'][:10]}: {event['subject'][:120]}"
        try:
            con.execute(
                "UPDATE customers SET last_client_status=?, client_phase=? WHERE id=?",
                (status, "replied" if event["kind"] == "reply" else "do_not_contact", row["customer_id"]),
            )
        except sqlite3.Error:
            pass
    con.commit()


# --------------------------------------------------------------------- sequence

def enroll(con: sqlite3.Connection, cfg: dict, row: dict, subject: str, when: Optional[datetime] = None) -> bool:
    seq = cfg.get("sequence") or {}
    if not seq.get("enabled"):
        return False
    when = when or datetime.utcnow()
    start = str(seq.get("start_date") or "")
    if start and when.date().isoformat() < start:
        return False
    con.execute(
        "INSERT OR IGNORE INTO outreach_sequence VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            (row.get("email") or "").lower(), uid(row.get("contact_id")), uid(row.get("customer_id")),
            row.get("first_name") or "", row.get("organization_name") or "", subject,
            when.strftime("%Y-%m-%d %H:%M:%S"), 1, when.strftime("%Y-%m-%d %H:%M:%S"), "active", None, _now(),
        ),
    )
    con.commit()
    return True


def due_followups(con: sqlite3.Connection, cfg: dict, today: Optional[date] = None) -> List[dict]:
    seq = cfg.get("sequence") or {}
    if not seq.get("enabled"):
        return []
    steps = seq.get("steps") or []
    today = today or datetime.utcnow().date()
    out = []
    emails, domains = blocked(con)
    for row in con.execute("SELECT * FROM outreach_sequence WHERE status='active' ORDER BY first_sent_at").fetchall():
        step = int(row["step"])
        if step - 1 >= len(steps):
            continue
        after = int(steps[step - 1].get("after_days", 5))
        first = datetime.strptime(row["first_sent_at"][:10], "%Y-%m-%d").date()
        if today < first + timedelta(days=after):
            continue
        if row["email"] in emails or _domain(row["email"]) in domains:
            continue
        out.append({**dict(row), "next_step": step + 1, "template": steps[step - 1]})
    return out[: int(seq.get("max_per_day", 20))]


def followup_payload(item: dict, signature_html: str = "") -> dict:
    tpl = item["template"]
    first = (item.get("first_name") or "").strip()
    greeting = f"Hallo {html.escape(first)}," if first else "Guten Tag,"
    paragraphs = "".join(f"<p>{html.escape(p.strip())}</p>" for p in str(tpl.get("body") or "").split("\n\n") if p.strip())
    optout = tpl.get("optout") or ""
    body = (
        '<html><body style="font-family: Arial, sans-serif; font-size:14px; color:#222;">'
        f"<p>{greeting}</p>{paragraphs}<p>Beste Grüße<br>Essam Azzam</p>{signature_html}"
        + (f'<p style="font-size:11px; color:#777; margin-top:18px;">{html.escape(optout)}</p>' if optout else "")
        + "</body></html>"
    )
    subject = item.get("first_subject") or "SAP MDG"
    if not subject.lower().startswith("re:"):
        subject = "Re: " + subject
    return {
        "message": {
            "subject": subject,
            "body": {"contentType": "HTML", "content": body},
            "toRecipients": [{"emailAddress": {"address": item["email"]}}],
        },
        "saveToSentItems": True,
    }


def mark_followup(con: sqlite3.Connection, item: dict, steps_total: int) -> None:
    step = int(item["next_step"])
    status = "done" if step - 1 >= steps_total else "active"
    con.execute(
        "UPDATE outreach_sequence SET step=?, last_sent_at=?, status=?, updated_at=? WHERE email=?",
        (step, _now(), status, _now(), item["email"]),
    )
    con.commit()


# --------------------------------------------------------------------- openers

def approved_opener(con: sqlite3.Connection, cfg: dict, contact_id: Any) -> Optional[str]:
    op = cfg.get("openers") or {}
    if not op.get("enabled") or not contact_id:
        return None
    row = con.execute("SELECT opener, status FROM outreach_drafts WHERE contact_id=?", (uid(contact_id),)).fetchone()
    if not row or not row["opener"]:
        return None
    if row["status"] == "approved" or (row["status"] == "draft" and not op.get("require_approval", True)):
        return row["opener"]
    return None


def mark_opener_sent(con: sqlite3.Connection, contact_id: Any) -> None:
    con.execute("UPDATE outreach_drafts SET status='sent', decided_at=? WHERE contact_id=? AND status IN ('approved','draft')",
                (_now(), uid(contact_id)))
    con.commit()


def summary(con: sqlite3.Connection) -> Dict[str, Any]:
    events = {r[0]: r[1] for r in con.execute("SELECT kind, count(*) FROM outreach_events GROUP BY kind")}
    seq = {r[0]: r[1] for r in con.execute("SELECT status, count(*) FROM outreach_sequence GROUP BY status")}
    drafts = {r[0]: r[1] for r in con.execute("SELECT status, count(*) FROM outreach_drafts GROUP BY status")}
    return {"events": events, "sequence": seq, "drafts": drafts}
