"""
Outbound lead email sender (database-driven).

What this app does:
- Reads unsent contacts from the main application database.
- Authenticates with Microsoft Graph using MSAL.
- Sends personalized outreach emails with inline branding and attachments.
- Logs delivery attempts in SQLite and in the main `EmailLog` table.
- Applies campaign guardrails such as per-day and per-company send limits.
"""
import os
import re
import time
import json
import sqlite3
from datetime import date, datetime, time as dt_time, timedelta, timezone
from typing import Dict, Any, List, Set
try:
    from zoneinfo import ZoneInfo
    BERLIN = ZoneInfo("Europe/Berlin")
except Exception:
    BERLIN = timezone.utc

import pandas as pd
from dotenv import load_dotenv
from helpers import log, send_notification_custom
from eligible import ALLOWED_EMAIL_STATUS, collect_stats, load_do_not_email, load_eligible_rows
from graph_auth import SENDER_UPN, get_access_token, graph_post_sendmail
from outlook_mail import send_outlook_email

try:  # Phase C: replies, follow-ups, openers. The sender keeps working without it.
    import outreach
except Exception as _exc:  # pragma: no cover
    outreach = None
    log(f"[!] outreach module unavailable: {_exc}")
load_dotenv()

TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
DAILY_LIMIT = int(os.getenv("DAILY_LIMIT", "50"))
DB_PATH = os.getenv("DB_PATH", "outreach_log.sqlite")
STATE_PATH = os.getenv("OPS_STATE_PATH", "logger/ops_state.json")
EMPTY_RECHECK_SEC = int(os.getenv("EMPTY_RECHECK_SECONDS", "1800"))
REFUEL_CONTACTS_PER_DAY = int(os.getenv("REFUEL_CONTACTS_PER_DAY", "100"))
MORNING_HOUR = int(os.getenv("MORNING_HOUR", "7"))
MORNING_MINUTE = int(os.getenv("MORNING_MINUTE", "30"))

# main application database (contacts, email logs)
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker
from databases import Contact, Customer, EmailLog, Base
from email_log_helpers import log_email_send

ENGINE = create_engine(os.getenv("DATABASE_URL", "sqlite:///fortlo.db"), echo=False, future=True)


def ensure_customer_schema() -> None:
    """Apply additive customer column updates for existing databases."""
    inspector = inspect(ENGINE)
    if "customers" not in inspector.get_table_names():
        return

    existing_columns = {column["name"] for column in inspector.get_columns("customers")}
    alter_statements = []

    if "last_client_status" not in existing_columns:
        alter_statements.append("ALTER TABLE customers ADD COLUMN last_client_status TEXT")
    if "client_phase" not in existing_columns:
        alter_statements.append("ALTER TABLE customers ADD COLUMN client_phase VARCHAR(50)")

    if not alter_statements:
        return

    with ENGINE.begin() as connection:
        for statement in alter_statements:
            connection.execute(text(statement))


ensure_customer_schema()
SessionLocal = sessionmaker(bind=ENGINE)
MAX_PER_COMPANY_PER_DAY = 2
SLEEP_BETWEEN_SENDS_SEC = 180  # be conservative

# Allowed local send windows.
SEND_WINDOWS = [
    (dt_time(8, 0), dt_time(10, 0)),
    (dt_time(10, 30), dt_time(11, 30)),
    (dt_time(13, 30), dt_time(15, 0)),
]


def _now_berlin() -> datetime:
    return datetime.now(BERLIN).replace(tzinfo=None)


def _is_within_send_window(now_local: datetime) -> bool:
    now_t = now_local.time()
    for start_t, end_t in SEND_WINDOWS:
        if start_t <= now_t < end_t:
            return True
    return False


def _next_window_start(now_local: datetime) -> datetime:
    today = now_local.date()
    for start_t, _ in SEND_WINDOWS:
        candidate = datetime.combine(today, start_t)
        if candidate > now_local:
            return candidate

    # No more windows left today -> first window tomorrow.
    tomorrow = today + timedelta(days=1)
    return datetime.combine(tomorrow, SEND_WINDOWS[0][0])


def _remaining_window_seconds_today(now_local: datetime) -> int:
    total_seconds = 0
    today = now_local.date()

    for start_t, end_t in SEND_WINDOWS:
        start_dt = datetime.combine(today, start_t)
        end_dt = datetime.combine(today, end_t)
        if now_local >= end_dt:
            continue
        window_start = max(now_local, start_dt)
        total_seconds += max(0, int((end_dt - window_start).total_seconds()))

    return total_seconds


def _seconds_until_next_send(con) -> int:
    """Return how long to wait before the next send attempt.

    The delay enforces:
    - sends only inside SEND_WINDOWS,
    - pacing across the remaining window time so DAILY_LIMIT is spread out.
    """
    now_local = _now_berlin()
    sent_today = sent_count_today(con)

    if sent_today >= DAILY_LIMIT:
        next_start = _next_window_start(now_local)
        return max(1, int((next_start - now_local).total_seconds()))

    if not _is_within_send_window(now_local):
        next_start = _next_window_start(now_local)
        return max(1, int((next_start - now_local).total_seconds()))

    remaining_quota = DAILY_LIMIT - sent_today
    remaining_seconds = _remaining_window_seconds_today(now_local)

    # Keep a minimum spacing to avoid back-to-back bursts.
    if remaining_quota <= 0:
        return SLEEP_BETWEEN_SENDS_SEC
    if remaining_seconds <= 0:
        return SLEEP_BETWEEN_SENDS_SEC

    even_gap = int(remaining_seconds / remaining_quota)
    return max(60, min(even_gap, 60 * 30))


def _wait_until_allowed_to_send(con):
    """Block until we're inside an allowed send window and below DAILY_LIMIT."""
    while True:
        wait_s = _seconds_until_next_send(con)
        now_local = _now_berlin()
        if wait_s <= 0:
            return

        in_window = _is_within_send_window(now_local)
        below_limit = sent_count_today(con) < DAILY_LIMIT
        if in_window and below_limit:
            return

        wake_at = now_local + timedelta(seconds=wait_s)
        log(f"Waiting {wait_s}s until next send slot at {wake_at.strftime('%Y-%m-%d %H:%M:%S')}")
        time.sleep(wait_s)

def is_valid_email(e: str) -> bool:
    if not isinstance(e, str) or "@" not in e:
        return False
    return re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", e.strip()) is not None

def init_db():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sent (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            send_date TEXT NOT NULL,
            sent_at TEXT NOT NULL,
            email TEXT NOT NULL,
            person_id TEXT,
            company TEXT,
            subject TEXT,
            status TEXT
        )
    """)
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_sent_email_date ON sent(email, send_date)")
    con.commit()
    return con

def sent_count_today(con) -> int:
    cur = con.cursor()
    cur.execute("SELECT COUNT(*) FROM sent WHERE send_date=?", (str(date.today()),))
    return int(cur.fetchone()[0])

def already_sent_today(con, email: str) -> bool:
    cur = con.cursor()
    cur.execute("SELECT 1 FROM sent WHERE send_date=? AND email=? LIMIT 1", (str(date.today()), email))
    return cur.fetchone() is not None

def already_sent(con, email: str) -> bool:
    """Check if email has ever been sent (all time, not just today)"""
    cur = con.cursor()
    cur.execute("SELECT 1 FROM sent WHERE email=? LIMIT 1", (email,))
    return cur.fetchone() is not None

def company_count_today(con, company: str) -> int:
    cur = con.cursor()
    cur.execute("SELECT COUNT(*) FROM sent WHERE send_date=? AND company=?", (str(date.today()), company))
    return int(cur.fetchone()[0])

def customer_count_today(con, company: str) -> int:
    cur = con.cursor()
    cur.execute("SELECT COUNT(*) FROM sent WHERE send_date=? AND email=?", (str(date.today()), company))
    return int(cur.fetchone()[0])


def mark_sent(con, email, person_id, company, subject, status):
    cur = con.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO sent(send_date, sent_at, email, person_id, company, subject, status)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (str(date.today()), datetime.utcnow().isoformat(), email, person_id, company, subject, status))
    con.commit()

import base64
import uuid
from html import escape as html_escape

def file_to_base64(path: str) -> str:
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")

def build_message(row,language='DE', opener=None):
    first = (row.get("first_name") or "").strip()
    opener_html = (
        f'<div style="margin-bottom:16px;">{html_escape(opener)}</div>' if opener else ""
    )
    company = (row.get("organization_name") or "").strip()

    subject = f"SAP MDG Support for {company}" if company else "SAP MDG Support"

    # Put this file next to your script (or use an absolute path)
    logo_file = "forte4_logo.png"
    signature_file = os.getenv("SIGNATURE_PIC", "sender_photo.png")

    # CID best practice: use the filename (works well in Outlook)
    logo_cid = logo_file
    sig_cid = signature_file
    if language == 'EN':
        html_body = f"""
<html>
  <body style="margin:0; padding:0; font-family: Arial, sans-serif; font-size:14px; color:#222;">

    <p>Hello {first or company or 'there'},</p>

    <p>
            My name is Essam, and I am a subject matter expert in SAP Master Data Governance (MDG).
            If you need support for your S/4HANA rollout or MDG initiatives, I would be happy to help.
    </p>

    <p>
            I would appreciate the opportunity to briefly discuss how FORTE4 could support your current or upcoming projects.
        </p>

        <p>Best regards,</p>

    <!-- Signature -->
    <table cellpadding="0" cellspacing="0" border="0" style="margin-top:20px; border-collapse:collapse;">
      <tr>
        <!-- Profile Picture -->
        <td style="vertical-align:middle; padding-right:14px;">
          <img src="cid:{sig_cid}"
               alt="Essam Azzam"
               width="85"

               style="display:block; border-radius:50%;" />
        </td>

        <!-- Text Block -->
        <td style="vertical-align:middle; font-family: Arial, sans-serif; line-height:1.4;">
          
          <div style="font-weight:bold; font-size:15px; color:#111;">
            Essam Azzam
          </div>

          <div style="font-size:14px; color:#444;">
            Chief Architect – FORTE4
          </div>

          <div style="font-size:12px; color:#666; margin-top:4px;">
            SAP Master Data Governance | S/4HANA Transformation
          </div>

          <!-- Contact Line (Premium Compact Style) -->
          <div style="font-size:12px; color:#444; margin-top:8px;">
            M: 
            <a href="tel:+491623389146"
               style="color:#222; text-decoration:none;">
               +491623389146
            </a>
            &nbsp;&nbsp;|&nbsp;&nbsp;
            <a href="https://www.linkedin.com/in/essam-azzam-4201b6106/"
               style="color:#0a66c2; text-decoration:none;"
               target="_blank">
               LinkedIn
            </a>
          </div>

        </td>
      </tr>
    </table>

    <!-- Logo -->
    <div style="margin-top:16px;">
      <img src="cid:{logo_cid}"
           alt="FORTE4"
           width="220"
           style="display:block;" />
    </div>

  </body>
</html>
"""

    elif language == 'DE':
        html_body = f"""
<html>
  <body style="margin:0; padding:0; font-family: Arial, sans-serif; font-size:14px; color:#222;">

    <p>Hallo {first or company or 'there'},</p>
{opener_html}
<div style="margin-bottom:16px;">
Arbeitet Ihr Team aktuell an SAP MDG oder Data Governance im S/4HANA Umfeld?
</div>

<div style="margin-bottom:16px;">
Wir unterstützen mehrere Unternehmen bei der Konzeption, Implementierung, Optimierung und beim Governance-Setup von SAP MDG.
</div>

<div style="margin-bottom:16px;">
Falls das für Sie relevant ist, können wir uns gern 15 Minuten austauschen.
</div>
        <p>Beste Grüße,</p>
    <!-- Signature -->
    <table cellpadding="0" cellspacing="0" border="0" style="margin-top:20px; border-collapse:collapse;">
      <tr>
        <!-- Profile Picture -->
        <td style="vertical-align:middle; padding-right:14px;">
          <img src="cid:{sig_cid}"
               alt="Essam Azzam"
               width="85"

               style="display:block; border-radius:50%;" />
        </td>

        <!-- Text Block -->
        <td style="vertical-align:middle; font-family: Arial, sans-serif; line-height:1.4;">
          
          <div style="font-weight:bold; font-size:15px; color:#111;">
            Essam Azzam
          </div>

          <div style="font-size:14px; color:#444;">
            Chief Architect – FORTE4
          </div>

          <div style="font-size:12px; color:#666; margin-top:4px;">
            SAP Master Data Governance | S/4HANA Transformation
          </div>

          <!-- Contact Line (Premium Compact Style) -->
          <div style="font-size:12px; color:#444; margin-top:8px;">
            M: 
            <a href="tel:+491623389146"
               style="color:#222; text-decoration:none;">
               +491623389146
            </a>
            &nbsp;&nbsp;|&nbsp;&nbsp;
            <a href="https://www.linkedin.com/in/essam-azzam-4201b6106/"
               style="color:#0a66c2; text-decoration:none;"
               target="_blank">
               LinkedIn
            </a>
          </div>

        </td>
      </tr>
    </table>

    <!-- Logo -->
    <div style="margin-top:16px;">
      <img src="cid:{logo_cid}"
           alt="FORTE4"
           width="220"
           style="display:block;" />
    </div>

  </body>
</html>
"""



    attachments = []
    if os.path.exists(logo_file):
        attachments.append(
            {
                "@odata.type": "#microsoft.graph.fileAttachment",
                "name": logo_file,
                "contentType": "image/png",
                "contentBytes": file_to_base64(logo_file),
                "isInline": True,
                "contentId": logo_cid,
            }
        )
    if os.path.exists(signature_file):
        attachments.append(
            {
                "@odata.type": "#microsoft.graph.fileAttachment",
                "name": signature_file,
                "contentType": "image/png",
                "contentBytes": file_to_base64(signature_file),
                "isInline": True,
                "contentId": sig_cid,
            }
        )

    return {
        "message": {
            "subject": subject,
            "body": {"contentType": "HTML", "content": html_body},
            "toRecipients": [{"emailAddress": {"address": row["email"]}}],
            "attachments": attachments,
        },
        "saveToSentItems": True
    }


def _load_state() -> dict:
    if not os.path.exists(STATE_PATH):
        return {}
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as handle:
            return json.load(handle) or {}
    except Exception:
        return {}


def _save_state(state: dict) -> None:
    os.makedirs(os.path.dirname(STATE_PATH) or ".", exist_ok=True)
    with open(STATE_PATH, "w", encoding="utf-8") as handle:
        json.dump(state, handle)


def _today_key() -> str:
    return str(_now_berlin().date())


def _current_stats(con) -> dict:
    with SessionLocal() as session:
        return collect_stats(session, sent_today=sent_count_today(con))


def _briefing_html(stats: dict, title: str, extra: str = "") -> str:
    companies = stats.get("companies") or []
    company_preview = ", ".join(companies[:12])
    if len(companies) > 12:
        company_preview += f" (+{len(companies) - 12} more)"
    extra_html = f"<p>{extra}</p>" if extra else ""
    return f"""
    <html><body style="font-family:Arial,sans-serif;font-size:14px;color:#222">
      <h2>{title}</h2>
      {extra_html}
      <p>The FORTLO outreach container is running.</p>
      <ul>
        <li><b>{stats['eligible_contacts']}</b> contacts still available before the next list refuel</li>
        <li><b>{stats['eligible_companies']}</b> companies still have unsent contacts</li>
        <li>Daily send capacity: {stats['daily_capacity']} (limit {stats['daily_limit']}, max {stats['per_company_per_day']} per company)</li>
        <li>Estimated days until refuel: <b>{stats['days_until_refuel']}</b></li>
        <li>Already sent today: {stats['sent_today']}</li>
        <li>Refuel needed now: {'yes' if stats['needs_refuel'] else 'no'}</li>
      </ul>
      <p>Companies still in the queue: {company_preview or 'none'}</p>
    </body></html>
    """


def send_startup_or_morning_briefing(con, state: dict, startup: bool = False) -> None:
    now = _now_berlin()
    today = _today_key()
    morning = dt_time(MORNING_HOUR, MORNING_MINUTE)
    already = state.get("briefing_date") == today
    if already:
        return
    if not startup and now.time() < morning:
        return
    stats = _current_stats(con)
    if startup:
        subject = "FORTLO started — outreach analytics"
        title = "FORTLO has started"
        extra = "This is the container start notification."
    else:
        subject = "FORTLO morning analytics"
        title = "FORTLO daily morning update"
        extra = "Scheduled Berlin morning briefing."
    send_outlook_email(subject, _briefing_html(stats, title, extra))
    if stats["needs_refuel"]:
        send_refuel_required_email(stats, state)
    state["briefing_date"] = today
    _save_state(state)


def send_refuel_required_email(stats: dict, state: dict) -> None:
    today = _today_key()
    if state.get("refuel_email_date") == today:
        return
    html = _briefing_html(
        stats,
        "List refuel required",
        "The sendable contact list is empty. Auto-refuel will fetch up to "
        f"{REFUEL_CONTACTS_PER_DAY} Head of ERP contacts from Apollo in the Bechtle size band. "
        "If the list is still empty afterwards, the app will check again later today.",
    )
    send_outlook_email("FORTLO list refuel required", html)
    send_notification_custom("FORTLO list refuel required")
    state["refuel_email_date"] = today
    _save_state(state)


def maybe_auto_refuel(con, state: dict) -> None:
    today = _today_key()
    stats = _current_stats(con)
    if not stats["needs_refuel"]:
        return
    send_refuel_required_email(stats, state)
    if state.get("refuel_run_date") == today:
        log("List empty; Apollo refuel already ran today. Checking again later.")
        return
    log(f"List empty; auto-refueling up to {REFUEL_CONTACTS_PER_DAY} Head of ERP contacts")
    try:
        from refuel_apollo import refuel_head_of_erp

        summary = refuel_head_of_erp(REFUEL_CONTACTS_PER_DAY)
    except Exception as exc:
        summary = {"new_contacts": 0, "errors": [str(exc)]}
        log(f"[!] Auto-refuel failed: {exc}")
    state["refuel_run_date"] = today
    state["last_refuel"] = summary
    _save_state(state)
    after = _current_stats(con)
    extra = (
        f"Apollo refuel stored {summary.get('new_contacts', 0)} new contacts "
        f"across {summary.get('new_accounts', 0)} accounts. "
        f"Eligible now: {after['eligible_contacts']}."
    )
    if not summary.get("new_contacts"):
        extra += (
            " WARNING: 0 genuinely new contacts. The ICP pool looks exhausted; "
            "widen refuel_small_de in config.yaml or review skipped accounts."
        )
    if summary.get("errors"):
        extra += " Errors: " + "; ".join(str(e) for e in summary["errors"][:5])
    send_outlook_email(
        "FORTLO auto-refuel result",
        _briefing_html(after, "Auto-refuel finished", extra),
    )


def _introductory_status_text(row: dict) -> str:
    first = (row.get("first_name") or "").strip()
    last = (row.get("last_name") or "").strip()
    email = (row.get("email") or "").strip()
    who = " ".join(part for part in [first, last] if part).strip() or email
    sent_on = _now_berlin().strftime("%d %b %Y")
    if who:
        return f"Sent introductory email on {sent_on} to {who}"
    return f"Sent introductory email on {sent_on}"


def record_outreach_success(row: dict, subject: str) -> None:
    contact_id = row.get("contact_id")
    session = SessionLocal()
    try:
        contact_obj = None
        if contact_id:
            contact_obj = session.query(Contact).filter(Contact.id == contact_id).first()
        cust_id = contact_obj.customer_id if contact_obj else None
        log_email_send(
            contact_id=contact_id,
            customer_id=cust_id,
            email_address=row.get("email"),
            subject=subject,
            campaign_name="forte4_icp_de",
            tenant_id=getattr(contact_obj, "tenant_id", None) if contact_obj else None,
        )
        if cust_id:
            customer = session.query(Customer).filter(Customer.id == cust_id).first()
            if customer:
                customer.last_client_status = _introductory_status_text(row)
                if not customer.client_phase or customer.client_phase == "new":
                    customer.client_phase = "contacted"
                session.commit()
    except Exception as exc:
        session.rollback()
        log(f"[!] Warning: failed to update CRM after send: {exc}")
    finally:
        session.close()


def _outreach_call(fn, *args, default=None):
    """Run an outreach step; never let it stop the sender."""
    if outreach is None:
        return default
    try:
        ocon = outreach.connect()
        try:
            return fn(ocon, *args)
        finally:
            ocon.close()
    except Exception as exc:
        log(f"[!] outreach {getattr(fn, '__name__', 'step')} failed: {exc}")
        return default


def send_one(con, token: str, row: dict):
    company = str(row.get("organization_name", "")).strip()
    why = _outreach_call(outreach.is_blocked, row.get("email"), default=None) if outreach else None
    if why:
        log(f"Skip {row.get('email')} ({company}): {why}")
        return token, False
    cfg = outreach.load_cfg() if outreach else {}
    opener = _outreach_call(outreach.approved_opener, cfg, row.get("contact_id")) if outreach else None
    payload = build_message(row, opener=opener)
    subject = payload["message"]["subject"]
    email = row["email"]
    person_id = str(row.get("person_id", "")).strip()
    try:
        resp = graph_post_sendmail(token, payload)
        if resp.status_code == 401:
            log(f"[!] Authentication failed (401) for {email}, refreshing token and retrying...")
            token = get_access_token()
            resp = graph_post_sendmail(token, payload)
        if resp.status_code in (202, 200):
            mark_sent(con, email, person_id, company, subject, "sent")
            record_outreach_success(row, subject)
            _after_first_send(row, subject, cfg, opener)
            log(f"[✓] Sent to {email} ({company})" + (" with personalised opener" if opener else ""))
            return token, True
        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            sleep_s = int(retry_after) if retry_after and retry_after.isdigit() else 10
            log(f"[!] Throttled (429). Sleeping {sleep_s}s then retrying once...")
            time.sleep(sleep_s)
            resp2 = graph_post_sendmail(token, payload)
            if resp2.status_code in (202, 200):
                mark_sent(con, email, person_id, company, subject, "sent")
                record_outreach_success(row, subject)
                _after_first_send(row, subject, cfg, opener)
                log(f"[✓] Sent after retry to {email} ({company})")
                return token, True
            mark_sent(con, email, person_id, company, subject, f"error:{resp2.status_code}:{resp2.text[:200]}")
            log(f"[!] Failed {email}: {resp2.status_code} {resp2.text[:200]}")
            return token, False
        mark_sent(con, email, person_id, company, subject, f"error:{resp.status_code}:{resp.text[:200]}")
        log(f"[!] Failed {email}: {resp.status_code} {resp.text[:200]}")
        return token, False
    except Exception as exc:
        mark_sent(con, email, person_id, company, subject, f"exception:{exc}")
        log(f"[!] Exception sending to {email}: {exc}")
        return token, False


def _after_first_send(row: dict, subject: str, cfg: dict, opener) -> None:
    if outreach is None:
        return
    customer_id = row.get("customer_id")
    if not customer_id and row.get("contact_id"):
        try:
            with SessionLocal() as session:
                contact_obj = session.query(Contact).filter(Contact.id == row["contact_id"]).first()
                customer_id = contact_obj.customer_id if contact_obj else None
        except Exception:
            customer_id = None
    _outreach_call(outreach.enroll, cfg, {**row, "customer_id": customer_id}, subject)
    if opener:
        _outreach_call(outreach.mark_opener_sent, row.get("contact_id"))


_LAST_REPLY_SCAN = {"at": None}
REPLY_SCAN_EVERY_SEC = int(os.getenv("REPLY_SCAN_EVERY_SECONDS", "900"))


def scan_replies() -> None:
    """Read the inbox (every 15 min), record replies / bounces / opt-outs, stop follow-ups, notify."""
    if outreach is None:
        return
    now = datetime.utcnow()
    last = _LAST_REPLY_SCAN["at"]
    if last and (now - last).total_seconds() < REPLY_SCAN_EVERY_SEC:
        return
    _LAST_REPLY_SCAN["at"] = now
    since = (last or now - timedelta(days=int((outreach.load_cfg().get("replies") or {}).get("first_scan_days", 60)))) - timedelta(hours=6)
    try:
        messages = outreach.fetch_inbox(get_access_token(), SENDER_UPN, since, limit=1000)
    except Exception as exc:
        log(f"[!] Reply scan failed: {exc}")
        return

    def _scan(ocon):
        events = outreach.scan_replies(ocon, messages)
        outreach.apply_crm_status(ocon, events)
        return events

    events = _outreach_call(_scan, default=[]) or []
    for event in events:
        if event["kind"] == "auto_reply":
            continue
        log(f"Outreach {event['kind']}: {event['email']} | {event['subject'][:80]}")
        if event["kind"] in ("reply", "unsubscribe") and last is not None:  # no alerts for the first back-scan
            send_notification_custom(f"FORTLO {event['kind']}: {event['email']} - {event['subject'][:80]}")


_LAST_DRAFTS = {"day": None}


def prepare_openers() -> None:
    """Once a day: Claude drafts openers for unsent contacts at companies with buying signals."""
    if outreach is None:
        return
    cfg = (outreach.load_cfg().get("openers") or {})
    today = _now_berlin().date().isoformat()
    if not cfg.get("enabled") or _LAST_DRAFTS["day"] == today:
        return
    _LAST_DRAFTS["day"] = today
    try:
        from leadgen import personalize
        from leadgen.model_review import client_from_env

        def _draft(ocon):
            items = personalize.candidates(ocon, limit=int(cfg.get("per_day", 20)))
            return personalize.draft(ocon, client_from_env(), items)

        result = _outreach_call(_draft, default=None)
        if result:
            log(f"Outreach openers: {result}")
    except Exception as exc:
        log(f"[!] Opener drafting failed: {exc}")


def send_followups(con) -> int:
    """Send due follow-ups now, inside the send window and under DAILY_LIMIT. Never waits."""
    if outreach is None:
        return 0
    cfg = outreach.load_cfg()
    seq = cfg.get("sequence") or {}
    if not seq.get("enabled"):
        return 0
    due = _outreach_call(outreach.due_followups, cfg, default=[]) or []
    sent = 0
    token = None
    for item in due:
        if not _is_within_send_window(_now_berlin()) or sent_count_today(con) >= DAILY_LIMIT:
            break
        if already_sent_today(con, item["email"]):
            continue
        why = _outreach_call(outreach.is_blocked, item["email"], default="unknown")
        if why:
            continue
        try:
            token = token or get_access_token()
            payload = outreach.followup_payload(item)
            resp = graph_post_sendmail(token, payload)
            if resp.status_code == 401:
                token = get_access_token()
                resp = graph_post_sendmail(token, payload)
        except Exception as exc:
            log(f"[!] Follow-up to {item['email']} failed: {exc}")
            continue
        subject = payload["message"]["subject"]
        if resp.status_code not in (200, 202):
            mark_sent(con, item["email"], "", item.get("company") or "", subject, f"error:{resp.status_code}:{resp.text[:200]}")
            log(f"[!] Follow-up to {item['email']} failed: {resp.status_code} {resp.text[:200]}")
            continue
        mark_sent(con, item["email"], "", item.get("company") or "", subject, f"followup:{item['next_step']}")
        _outreach_call(outreach.mark_followup, item, len(seq.get("steps") or []))
        try:
            log_email_send(
                contact_id=uuid.UUID(item["contact_id"]), customer_id=uuid.UUID(item["customer_id"]),
                email_address=item["email"], subject=subject, campaign_name=f"forte4_followup_{item['next_step']}",
            )
        except Exception as exc:
            log(f"[!] Follow-up CRM log failed for {item['email']}: {exc}")
        sent += 1
        log(f"[✓] Follow-up {item['next_step']} sent to {item['email']} ({item.get('company')})")
        pause_s = _seconds_until_next_send(con)
        if pause_s > 0 and pause_s <= 60 * 30:
            time.sleep(pause_s)
    return sent


def send_pass(con) -> int:
    do_not_email = load_do_not_email()
    with SessionLocal() as session:
        rows = load_eligible_rows(session, do_not_email)
    df = pd.DataFrame(rows)
    if df.empty:
        log("No eligible contacts in this pass")
        return 0

    def seniority_score(title: str) -> int:
        t = str(title).lower()
        for kw, score in [("chief", 5), ("cdo", 5), ("cio", 5), ("vp", 4), ("director", 3), ("head", 3), ("manager", 2), ("lead", 2)]:
            if kw in t:
                return score
        return 1

    df["seniority_score"] = df.get("title", "").apply(seniority_score)
    df = df.sort_values(by=["seniority_score"], ascending=False)
    token = get_access_token()
    sent_now = 0
    log(f"Loaded {len(df)} unsent contacts from DB")
    skipped_company_cap = 0
    for _, r in df.iterrows():
        send_startup_or_morning_briefing(con, _load_state())
        _wait_until_allowed_to_send(con)
        scan_replies()
        prepare_openers()
        send_followups(con)  # due follow-ups go before new first emails
        _wait_until_allowed_to_send(con)
        company = str(r.get("organization_name", "")).strip()
        if company and company_count_today(con, company) >= MAX_PER_COMPANY_PER_DAY:
            skipped_company_cap += 1
            log(f"customer from {company} already contacted today")
            continue
        token, ok = send_one(con, token, r.to_dict())
        if ok:
            sent_now += 1
        pause_s = _seconds_until_next_send(con)
        if pause_s > 0:
            log(f"Pacing next send in {pause_s}s")
            time.sleep(pause_s)
    log(f"Pass complete: sent={sent_now} skipped_company_cap={skipped_company_cap}")
    return sent_now


def main():
    if not all([TENANT_ID, CLIENT_ID, SENDER_UPN]):
        raise SystemExit("Missing TENANT_ID / CLIENT_ID / SENDER_UPN in .env")

    con = init_db()
    state = _load_state()
    send_startup_or_morning_briefing(con, state, startup=True)

    while True:
        state = _load_state()
        send_startup_or_morning_briefing(con, state, startup=False)
        scan_replies()
        prepare_openers()
        send_followups(con)
        stats = _current_stats(con)
        if stats["needs_refuel"]:
            maybe_auto_refuel(con, state)
            after = _current_stats(con)
            if after["needs_refuel"]:
                log(f"List still empty. Checking again in {EMPTY_RECHECK_SEC}s")
                time.sleep(EMPTY_RECHECK_SEC)
                continue
        sent_now = send_pass(con)
        leftover = _current_stats(con)
        if leftover["needs_refuel"]:
            continue
        log(
            f"Remaining eligible={leftover['eligible_contacts']}; "
            f"waiting for the next send window (sent this pass={sent_now})"
        )
        _wait_until_allowed_to_send(con)

if __name__ == "__main__":
    main()
