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
from datetime import date, datetime, time as dt_time, timedelta
from typing import Dict, Any, List, Set

import pandas as pd
import requests
import msal
from dotenv import load_dotenv
import time
from helpers import log,send_notification_custom,get_logo_encoding
load_dotenv()

TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
SENDER_UPN = os.getenv("SENDER_UPN")  # the mailbox that sends
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
DAILY_LIMIT = int(os.getenv("DAILY_LIMIT", "50"))

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
# Don't include reserved scopes like 'offline_access'/'openid'/'profile' here.
# MSAL will add the required OpenID/OAuth reserved scopes automatically.
SCOPES = ["Mail.Send"]  # delegated scopes
TOKEN_CACHE_FILE = os.getenv("TOKEN_CACHE_FILE", "msal_token_cache.json")
DB_PATH = os.getenv("DB_PATH", "outreach_log.sqlite")

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
GRAPH_SENDMAIL = f"https://graph.microsoft.com/v1.0/users/{SENDER_UPN}/sendMail"

ALLOWED_EMAIL_STATUS = {"verified", "likely to engage"}  # align with your CSV
MAX_PER_COMPANY_PER_DAY = 2
SLEEP_BETWEEN_SENDS_SEC = 180  # be conservative

# Allowed local send windows.
SEND_WINDOWS = [
    (dt_time(8, 0), dt_time(10, 0)),
    (dt_time(10, 30), dt_time(11, 30)),
    (dt_time(13, 30), dt_time(15, 0)),
]


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
    now_local = datetime.now()
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
        now_local = datetime.now()
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

def load_do_not_email() -> Set[str]:
    # Optional: put unsubscribes here, one email per line
    path = "do_not_email.txt"
    if not os.path.exists(path):
        return set()
    with open(path, "r", encoding="utf-8") as f:
        return {line.strip().lower() for line in f if line.strip()}

import base64

def file_to_base64(path: str) -> str:
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")

def build_message(row,language='DE'):
    first = (row.get("first_name") or "").strip()
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



    logo_b64 = file_to_base64(logo_file)

    return {
        "message": {
            "subject": subject,
            "body": {"contentType": "HTML", "content": html_body},
            "toRecipients": [{"emailAddress": {"address": row["email"]}}],
            "attachments": [
                {
                    "@odata.type": "#microsoft.graph.fileAttachment",
                    "name": logo_file,
                    "contentType": "image/png",
                    "contentBytes": logo_b64,
                    "isInline": True,
                    "contentId": logo_cid
                },
                {
                    "@odata.type": "#microsoft.graph.fileAttachment",
                    "name": signature_file,
                    "contentType": "image/png",
                    "contentBytes": file_to_base64(signature_file),
                    "isInline": True,
                    "contentId": sig_cid
                },
                # {
                #     "@odata.type": "#microsoft.graph.fileAttachment",
                #     "name": "Essam_Azzam_Profile.docx",
                #     "contentType": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                #     "contentBytes": file_to_base64("Essam_Azzam_Profile.docx"),
                #     "isInline": False
                # }
            ],
        },
        "saveToSentItems": True
    }


def load_cache():
    cache = msal.SerializableTokenCache()
    if os.path.exists(TOKEN_CACHE_FILE):
        cache.deserialize(open(TOKEN_CACHE_FILE, "r", encoding="utf-8").read())
    return cache

def save_cache(cache):
    if cache.has_state_changed:
        with open(TOKEN_CACHE_FILE, "w", encoding="utf-8") as f:
            f.write(cache.serialize())

def get_access_token() -> str:
    cache = load_cache()

    # If a client secret is provided use the confidential client + client credentials
    # (application permission) flow. Otherwise fall back to device code flow
    # using a public client application (delegated permissions).
    result = None
    if CLIENT_SECRET and str(CLIENT_SECRET).strip():
        # Use confidential client flow with client secret (application permissions).
        app = msal.ConfidentialClientApplication(
            client_id=CLIENT_ID,
            client_credential=str(CLIENT_SECRET).strip(),
            authority=AUTHORITY,
            token_cache=cache,
        )

        scopes = ["https://graph.microsoft.com/.default"]
        try:
            result = app.acquire_token_silent(scopes, account=None)
            if not result:
                result = app.acquire_token_for_client(scopes=scopes)
        except Exception as e:
            raise RuntimeError(
                "Confidential client token acquisition failed. Check CLIENT_SECRET, "
                "CLIENT_ID, and that the app registration has Application permissions and admin consent. "
                f"Original error: {e}"
            )

    else:
        app = msal.PublicClientApplication(
            client_id=CLIENT_ID,
            authority=AUTHORITY,
            token_cache=cache,
        )

        accounts = app.get_accounts()
        if accounts:
            result = app.acquire_token_silent(SCOPES, account=accounts[0])

        if not result:
            # Device code flow = best for scripts / headless, and avoids deprecated username+password flow
            flow = app.initiate_device_flow(scopes=SCOPES)
            if "user_code" not in flow:
                raise RuntimeError(f"Failed to create device flow: {flow}")
            log(flow["message"])
            result = app.acquire_token_by_device_flow(flow)

    save_cache(cache)

    if not result or "access_token" not in result:
        # If MSAL returned an error dict, include guidance for common misconfigurations.
        if isinstance(result, dict) and result.get("error"):
            err = result.get("error")
            desc = result.get("error_description", "")
            guidance = (
                "If using client credentials, ensure `CLIENT_SECRET` is correct and the app has "
                "Application permissions (e.g. Mail.Send) with admin consent. If using device code, "
                "ensure public client flows are enabled and SCOPES are delegated permissions."
            )
            raise RuntimeError(f"Token error: {err} - {desc}. {guidance}")
        raise RuntimeError(f"Token error: {result}")
    return result["access_token"]

def graph_post_sendmail(access_token: str, payload: Dict[str, Any]) -> requests.Response:
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    return requests.post(GRAPH_SENDMAIL, headers=headers, data=json.dumps(payload), timeout=60)

def main():
    if not all([TENANT_ID, CLIENT_ID, SENDER_UPN]):
        raise SystemExit("Missing TENANT_ID / CLIENT_ID / SENDER_UPN in .env")

    con = init_db()
    do_not_email = load_do_not_email()

    if sent_count_today(con) >= DAILY_LIMIT:
        send_notification_custom('Daily send limit reached. Waiting for next allowed slot.')
        _wait_until_allowed_to_send(con)


    # Load contacts from main DB that have not been emailed yet
    session = SessionLocal()
    # collect contact_ids that already have an EmailLog
    emailed_contact_ids = {r.contact_id for r in session.query(EmailLog.contact_id).all()}

    contacts = session.query(Contact).all()
    rows = []
    for c in contacts:
        # validate email
        if not c.email or not is_valid_email(c.email):
            continue
        # skip do not email
        if c.email.lower() in do_not_email:
            continue
        # skip if already emailed (in main EmailLog)
        if c.id in emailed_contact_ids:
            continue
        # skip if customer is not in campaign
        if c.customer and not c.customer.in_email_campaign:
            continue

        cust_name = ""
        try:
            cust_name = c.customer.name
        except Exception:
            cust_name = ""

        rows.append({
            "contact_id": c.id,
            "person_id": str(c.id),
            "email": c.email,
            "first_name": getattr(c, "first_name", ""),
            "last_name": getattr(c, "last_name", ""),
            "organization_name": cust_name,
            "title": c.job_title or "",
        })

    session.close()
    df = pd.DataFrame(rows)
    print(f"Loaded {len(df)} unsent contacts from DB")

    # Simple seniority scoring (optional)
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
    log({len(df)})
    for _, r in df.iterrows():
        _wait_until_allowed_to_send(con)

        company = str(r.get("organization_name", "")).strip()
        if company and customer_count_today(con, company) >= MAX_PER_COMPANY_PER_DAY:
            log(f'customer from {company} already contacted today')
            continue

        row = r.to_dict()
        payload = build_message(row)
        subject = payload["message"]["subject"]
        email = row["email"]
        person_id = str(row.get("person_id", "")).strip()

        try:
            resp = graph_post_sendmail(token, payload)

            # If token expired/invalid (401), refresh token and retry once
            if resp.status_code == 401:
                log(f"[!] Authentication failed (401) for {email}, refreshing token and retrying...")
                token = get_access_token()
                resp = graph_post_sendmail(token, payload)

            # Graph sendMail returns 202 Accepted on success
            if resp.status_code in (202, 200):
                mark_sent(con, email, person_id, company, subject, "sent")
                # also log in main DB EmailLog
                try:
                    contact_id = row.get("contact_id")
                    cust_id = None
                    session = SessionLocal()
                    if contact_id:
                        contact_obj = session.query(Contact).filter(Contact.id == contact_id).first()
                        if contact_obj:
                            cust_id = contact_obj.customer_id
                    log_email_send(
                        contact_id=contact_id,
                        customer_id=cust_id,
                        email_address=email,
                        subject=subject,
                    )
                    session.close()
                except Exception as e:
                    log(f"[!] Warning: failed to write EmailLog: {e}")

                sent_now += 1
                log(f"[✓] Sent to {email} ({company})")
            elif resp.status_code == 429:
                # throttled: respect Retry-After if present
                retry_after = resp.headers.get("Retry-After")
                sleep_s = int(retry_after) if retry_after and retry_after.isdigit() else 10
                log(f"[!] Throttled (429). Sleeping {sleep_s}s then retrying once...")
                time.sleep(sleep_s)
                resp2 = graph_post_sendmail(token, payload)
                if resp2.status_code in (202, 200):
                    mark_sent(con, email, person_id, company, subject, "sent")
                    # log into main EmailLog as well
                    try:
                        contact_id = row.get("contact_id")
                        cust_id = None
                        session = SessionLocal()
                        if contact_id:
                            contact_obj = session.query(Contact).filter(Contact.id == contact_id).first()
                            if contact_obj:
                                cust_id = contact_obj.customer_id
                        log_email_send(
                            contact_id=contact_id,
                            customer_id=cust_id,
                            email_address=email,
                            subject=subject,
                        )
                        session.close()
                    except Exception as e:
                        log(f"[!] Warning: failed to write EmailLog: {e}")

                    sent_now += 1
                    log(f"[✓] Sent after retry to {email} ({company})")
                else:
                    mark_sent(con, email, person_id, company, subject, f"error:{resp2.status_code}:{resp2.text[:200]}")
                    log(f"[!] Failed {email}: {resp2.status_code} {resp2.text[:200]}")
            else:
                mark_sent(con, email, person_id, company, subject, f"error:{resp.status_code}:{resp.text[:200]}")
                log(f"[!] Failed {email}: {resp.status_code} {resp.text[:200]}")

        except Exception as e:
            mark_sent(con, email, person_id, company, subject, f"exception:{e}")
            log(f"[!] Exception sending to {email}: {e}")

        # Pace send volume across today's remaining allowed windows.
        pause_s = _seconds_until_next_send(con)
        if pause_s > 0:
            log(f"Pacing next send in {pause_s}s")
            time.sleep(pause_s)

    send_notification_custom(f"[done] Sent {sent_now} emails, time to refuel the list boss")

if __name__ == "__main__":
    main()
