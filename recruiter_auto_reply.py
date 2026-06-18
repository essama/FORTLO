"""
Automatic recruiter email response script
Monitors incoming emails and sends automatic replies with CV to recruiters
"""

import os
import json
import time
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import base64
import re

import requests
import msal
from dotenv import load_dotenv

load_dotenv()

# Configuration
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
SENDER_UPN = os.getenv("SENDER_UPN")
TOKEN_CACHE_FILE = os.getenv("TOKEN_CACHE_FILE", "msal_token_cache.json")
DB_PATH = os.getenv("DB_PATH", "recruiter_responses.sqlite")
CV_FILE_PATH = "Eessam_Azzam_Profile.docx"

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPES = ["Mail.Read", "Mail.Send"]
GRAPH_API = "https://graph.microsoft.com/v1.0"

# Recruiter keywords to identify recruiting emails
RECRUITER_KEYWORDS = [
    "recruiter", "recruitment", "hiring", "candidate", "position", "opportunity",
    "job", "role", "career", "talent acquisition", "headhunter", "talent scout",
    "staffing", "placement", "interview", "interested in your profile", "cv review",
    "open position", "exciting opportunity", "would be a great fit"
]

class RecruiterAutoResponder:
    def __init__(self):
        self.app = None
        self.access_token = None
        self.db_path = DB_PATH
        self._init_db()

    def _init_app(self):
        """Initialize MSAL app (either confidential or public based on CLIENT_SECRET)"""
        if CLIENT_SECRET and str(CLIENT_SECRET).strip():
            # Use confidential client flow with client secret (application permissions)
            return msal.ConfidentialClientApplication(
                client_id=CLIENT_ID,
                client_credential=str(CLIENT_SECRET).strip(),
                authority=AUTHORITY,
                token_cache=self._load_token_cache()
            )
        else:
            # Use public client flow (delegated permissions)
            return msal.PublicClientApplication(
                client_id=CLIENT_ID,
                authority=AUTHORITY,
                token_cache=self._load_token_cache()
            )

    def _load_token_cache(self) -> msal.SerializableTokenCache:
        """Load existing token cache or create new one"""
        cache = msal.SerializableTokenCache()
        if os.path.exists(TOKEN_CACHE_FILE):
            try:
                with open(TOKEN_CACHE_FILE, 'r') as f:
                    cache.deserialize(f.read())
            except (IOError, PermissionError) as e:
                print(f"Warning: Could not read token cache ({e}), starting fresh")
        return cache

    def _save_token_cache(self):
        """Save token cache to file"""
        if self.app and self.app.token_cache.has_state_changed:
            try:
                with open(TOKEN_CACHE_FILE, 'w') as f:
                    f.write(self.app.token_cache.serialize())
            except (IOError, PermissionError) as e:
                print(f"Warning: Could not save token cache ({e}), continuing without cache")

    def _get_access_token(self) -> str:
        """Get valid access token"""
        if self.app is None:
            self.app = self._init_app()
            
        result = None
        
        # If using confidential client (with CLIENT_SECRET)
        if isinstance(self.app, msal.ConfidentialClientApplication):
            # Try silent first
            result = self.app.acquire_token_silent(["https://graph.microsoft.com/.default"], account=None)
            if not result or "access_token" not in result:
                # Fall back to client credentials
                result = self.app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
        
        # If using public client (without CLIENT_SECRET) or if confidential failed
        else:
            accounts = self.app.get_accounts()
            if accounts:
                result = self.app.acquire_token_silent(SCOPES, account=accounts[0])
            
            if not result or "access_token" not in result:
                # Use device flow for headless environment
                try:
                    flow = self.app.initiate_device_flow(scopes=SCOPES)
                    if "user_code" not in flow:
                        raise RuntimeError(f"Failed to create device flow: {flow}")
                    print(f"\n🔐 Device Code Authentication Required:")
                    print(flow["message"])
                    print("\nWaiting for authentication (this may take a minute)...")
                    result = self.app.acquire_token_by_device_flow(flow)
                except Exception as e:
                    raise Exception(f"Device flow failed: {e}")
        
        if not result or "access_token" not in result:
            error_msg = result.get("error_description", result.get("error", "Unknown error")) if result else "No response"
            raise Exception(f"Failed to acquire access token: {error_msg}")
        
        self.access_token = result["access_token"]
        self._save_token_cache()
        return self.access_token

    def _init_db(self):
        """Initialize SQLite database to track processed emails"""
        con = sqlite3.connect(self.db_path)
        cur = con.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS processed_emails (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id TEXT UNIQUE NOT NULL,
                sender_email TEXT NOT NULL,
                sender_name TEXT,
                subject TEXT,
                received_date TEXT,
                processed_date TEXT,
                cv_sent BOOLEAN DEFAULT 1
            )
        """)
        con.commit()
        con.close()

    def _is_recruiter_email(self, subject: str, body: str, sender: str) -> bool:
        """Determine if email is from a recruiter based on keywords"""
        combined_text = f"{subject} {body} {sender}".lower()
        
        keyword_count = sum(1 for keyword in RECRUITER_KEYWORDS if keyword in combined_text)
        
        # Consider it a recruiter email if it contains multiple recruiter keywords
        # or if sender has "recruiter" in email/name
        return keyword_count >= 2 or any(kw in sender.lower() for kw in ["recruiter", "talent", "hiring","sap","sap mdg","s/4 hana","business partner","bp","abap","btp"])

    def _is_already_processed(self, message_id: str) -> bool:
        """Check if email has already been processed"""
        con = sqlite3.connect(self.db_path)
        cur = con.cursor()
        cur.execute("SELECT id FROM processed_emails WHERE message_id = ?", (message_id,))
        result = cur.fetchone()
        con.close()
        return result is not None

    def _mark_as_processed(self, message_id: str, sender_email: str, sender_name: str, subject: str, received_date: str):
        """Mark email as processed in database"""
        con = sqlite3.connect(self.db_path)
        cur = con.cursor()
        cur.execute("""
            INSERT INTO processed_emails (message_id, sender_email, sender_name, subject, received_date, processed_date)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (message_id, sender_email, sender_name, subject, received_date, datetime.now().isoformat()))
        con.commit()
        con.close()

    def _get_email_body(self, message_id: str) -> str:
        """Retrieve full email body"""
        try:
            token = self._get_access_token()
            headers = {"Authorization": f"Bearer {token}"}
            
            url = f"{GRAPH_API}/me/messages/{message_id}"
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            return data.get("bodyPreview", "") or data.get("body", {}).get("content", "")
        except Exception as e:
            print(f"Error fetching email body: {e}")
            return ""

    def _send_reply_with_cv(self, message_id: str, to_email: str, sender_name: str) -> bool:
        """Send reply email with CV attached"""
        try:
            if not os.path.exists(CV_FILE_PATH):
                print(f"Error: CV file not found at {CV_FILE_PATH}")
                return False

            # Read and encode CV
            with open(CV_FILE_PATH, 'rb') as f:
                cv_content = base64.b64encode(f.read()).decode()

            token = self._get_access_token()
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }

            # Compose email
            email_body = f"""
Hi {sender_name},

Thank you for reaching out! I'm very interested in hearing about this opportunity.

I've attached my CV for your review. I'd be happy to discuss how my experience and skills align with the position.

Looking forward to connecting with you!

Best regards,
Eessam Azzam
            """

            # Prepare attachment
            attachment = {
                "@odata.type": "#microsoft.graph.fileAttachment",
                "name": "Eessam_Azzam_Profile.docx",
                "contentBytes": cv_content
            }

            # Build message
            message = {
                "message": {
                    "subject": f"Re: {sender_name}",
                    "body": {
                        "contentType": "text/plain",
                        "content": email_body
                    },
                    "toRecipients": [
                        {
                            "emailAddress": {
                                "address": to_email
                            }
                        }
                    ],
                    "attachments": [attachment]
                },
                "saveToSentItems": True
            }

            # Send via reply endpoint (maintains conversation thread)
            url = f"{GRAPH_API}/me/messages/{message_id}/reply"
            response = requests.post(url, headers=headers, json=message)
            
            if response.status_code in [200, 202]:
                print(f"✓ Successfully sent CV reply to {to_email}")
                return True
            else:
                print(f"✗ Failed to send reply to {to_email}: {response.status_code} - {response.text}")
                return False

        except Exception as e:
            print(f"Error sending reply: {e}")
            return False

    def check_incoming_emails(self, hours_back: float = 1) -> int:
        """Check incoming emails from the past N hours"""
        try:
            token = self._get_access_token()
            headers = {"Authorization": f"Bearer {token}"}

            # Filter for unread emails from the past N hours
            cutoff_time = (datetime.utcnow() - timedelta(hours=hours_back)).isoformat()
            filter_query = f"isRead eq false and receivedDateTime gt {cutoff_time}Z"

            url = f"{GRAPH_API}/me/mailFolders/inbox/messages"
            params = {
                "$filter": filter_query,
                "$top": 50,
                "$orderby": "receivedDateTime desc"
            }

            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()

            messages = response.json().get("value", [])
            processed_count = 0

            for msg in messages:
                message_id = msg.get("id")
                sender = msg.get("from", {}).get("emailAddress", {})
                sender_email = sender.get("address", "")
                sender_name = sender.get("name", sender_email.split("@")[0])
                subject = msg.get("subject", "")
                received_date = msg.get("receivedDateTime", "")

                # Skip if already processed
                if self._is_already_processed(message_id):
                    continue

                print(f"\n📧 New email from {sender_name} ({sender_email})")
                print(f"   Subject: {subject}")

                # Get full email body to check for recruiter content
                body = self._get_email_body(message_id)

                # Check if it's from a recruiter
                if self._is_recruiter_email(subject, body, sender_email):
                    print(f"   → Detected as recruiter email ✓")
                    
                    # Send reply with CV
                    if self._send_reply_with_cv(message_id, sender_email, sender_name):
                        self._mark_as_processed(message_id, sender_email, sender_name, subject, received_date)
                        processed_count += 1
                    else:
                        print(f"   → Failed to send reply")
                else:
                    print(f"   → Not a recruiter email, skipping")
                    self._mark_as_processed(message_id, sender_email, sender_name, subject, received_date)

                # Be polite with API rate limiting
                time.sleep(1)

            return processed_count

        except Exception as e:
            print(f"Error checking emails: {e}")
            return 0

    def run_continuous(self, check_interval_minutes: int = 5):
        """Run continuous monitoring"""
        print(f"Starting recruiter auto-responder (checking every {check_interval_minutes} minutes)...")
        
        try:
            while True:
                print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Checking for recruiter emails...")
                count = self.check_incoming_emails(hours_back=1)
                
                if count > 0:
                    print(f"✓ Processed {count} recruiter email(s)")
                else:
                    print("No new recruiter emails found")
                
                time.sleep(check_interval_minutes * 60)
        except KeyboardInterrupt:
            print("\nStopped by user")

if __name__ == "__main__":
    import sys
    
    responder = RecruiterAutoResponder()
    
    # Check if running in continuous mode
    if len(sys.argv) > 1 and sys.argv[1] == "--continuous":
        check_interval = int(sys.argv[2]) if len(sys.argv) > 2 else 5
        responder.run_continuous(check_interval_minutes=check_interval)
    else:
        # Run once to check current emails
        print("Running recruiter auto-responder...")
        hours = float(sys.argv[1]) if len(sys.argv) > 1 else 0.5
        count = responder.check_incoming_emails(hours_back=hours)
        print(f"\nProcessed {count} recruiter email(s)")
    
    # Uncomment below to run continuously:
    # responder.run_continuous(check_interval_minutes=5)
