"""Send operator notifications through the FORTE4 Outlook mailbox."""
from __future__ import annotations

import os
from typing import Optional

from dotenv import load_dotenv

from graph_auth import SENDER_UPN, get_access_token, graph_post_sendmail
from helpers import log

load_dotenv()

NOTIFY_EMAIL = (os.getenv("NOTIFY_EMAIL") or SENDER_UPN or "").strip()


def send_outlook_email(subject: str, html_body: str, to_address: Optional[str] = None) -> bool:
    recipient = (to_address or NOTIFY_EMAIL or "").strip()
    if not recipient:
        log("[!] No NOTIFY_EMAIL / SENDER_UPN; skipped Outlook operator mail")
        return False
    payload = {
        "message": {
            "subject": subject,
            "body": {"contentType": "HTML", "content": html_body},
            "toRecipients": [{"emailAddress": {"address": recipient}}],
        },
        "saveToSentItems": True,
    }
    try:
        token = get_access_token()
        resp = graph_post_sendmail(token, payload)
        if resp.status_code == 401:
            token = get_access_token()
            resp = graph_post_sendmail(token, payload)
        if resp.status_code in (200, 202):
            log(f"[✓] Outlook operator mail sent: {subject}")
            return True
        log(f"[!] Outlook operator mail failed {resp.status_code}: {resp.text[:300]}")
        return False
    except Exception as exc:
        log(f"[!] Outlook operator mail exception: {exc}")
        return False
