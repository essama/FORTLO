import os
import re
import time
import json
import sqlite3
from datetime import date, datetime
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
CSV_PATH = os.getenv("CSV_PATH", "mdg_high_intent.csv")
DAILY_LIMIT = int(os.getenv("DAILY_LIMIT", "50"))

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
# Don't include reserved scopes like 'offline_access'/'openid'/'profile' here.
# MSAL will add the required OpenID/OAuth reserved scopes automatically.
SCOPES = ["Mail.Send"]  # delegated scopes
TOKEN_CACHE_FILE = "msal_token_cache.json"
DB_PATH = "outreach_log.sqlite"

GRAPH_SENDMAIL = f"https://graph.microsoft.com/v1.0/users/{SENDER_UPN}/sendMail"

ALLOWED_EMAIL_STATUS = {"verified", "likely to engage"}  # align with your CSV
MAX_PER_COMPANY_PER_DAY = 2
SLEEP_BETWEEN_SENDS_SEC = 180  # be conservative
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

def build_message(row):
    first = (row.get("first_name") or "").strip()
    company = (row.get("organization_name") or "").strip()

    subject = f"AVC at {company} re: master data governance for SAP S/4HANA programs"

    # Put this file next to your script (or use an absolute path)
    logo_file = "forte4_logo.png"

    # CID best practice: use the filename (works well in Outlook)
    logo_cid = logo_file

    html_body = f"""
    <html>
      <body style="font-family: Arial, sans-serif; font-size: 14px; color: #222;">
        
        <p>Hi {first or 'there'},</p>

<p>
  We support SAP-centric companies like <strong>{company}</strong> with one very specific topic:
  <strong>product master data governance in SAP MDG</strong>.
</p>

<p>
  Using our <strong>Rapid Product Master SAP MDG Template</strong>, teams typically go live in <strong>6–12 months</strong>.
  Reference: <strong>Jungheinrich</strong> went live globally in <strong>12 months</strong>.
</p>

<p>
  Can we schedule a <strong>15-minute Teams call</strong> to check if this is relevant for you?
</p>



        <p>
          Best wishes,<br>
          <strong>Essam Azzam</strong><br>
          Chief Architect – FORTE4
        </p>

        <img src="cid:{logo_cid}" alt="FORTE4" width="350"
             style="display:block; margin-top:6px;" />
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
                }
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
        log(f"[i] Daily limit already reached ({DAILY_LIMIT}).")
        return

    df = pd.read_csv(CSV_PATH)

    # Normalize + filter
    df["email"] = df["email"].astype(str).str.strip()
    df["email_status_norm"] = df.get("email_status", "").astype(str).str.lower().str.strip()
    df = df[df["email"].apply(is_valid_email)]
    df = df[df["email_status_norm"].isin(ALLOWED_EMAIL_STATUS)]
    df = df[~df["email"].str.lower().isin(do_not_email)]
    #df = df[~df["email"].apply(lambda e: already_sent_today(con, e))]

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
        
        if sent_count_today(con) >= DAILY_LIMIT:
            log(f'limit reached for today, see you tomorrow isA boss')
            time.sleep(86500)
            #break

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

        time.sleep(SLEEP_BETWEEN_SENDS_SEC)

    send_notification_custom(f"[done] Sent {sent_now} emails, time to refuel the list boss")

if __name__ == "__main__":
    main()
