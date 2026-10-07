"""Microsoft Graph authentication shared by outreach and operator mail."""
from __future__ import annotations

import json
import os
from typing import Any, Dict

import msal
import requests
from dotenv import load_dotenv

from helpers import log

load_dotenv()

TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
SENDER_UPN = os.getenv("SENDER_UPN")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPES = ["Mail.Send"]
TOKEN_CACHE_FILE = os.getenv("TOKEN_CACHE_FILE", "msal_token_cache.json")
GRAPH_SENDMAIL = f"https://graph.microsoft.com/v1.0/users/{SENDER_UPN}/sendMail"


def load_cache():
    cache = msal.SerializableTokenCache()
    if os.path.exists(TOKEN_CACHE_FILE):
        with open(TOKEN_CACHE_FILE, "r", encoding="utf-8") as handle:
            cache.deserialize(handle.read())
    return cache


def save_cache(cache):
    if cache.has_state_changed:
        with open(TOKEN_CACHE_FILE, "w", encoding="utf-8") as handle:
            handle.write(cache.serialize())


def get_access_token() -> str:
    cache = load_cache()
    result = None
    if CLIENT_SECRET and str(CLIENT_SECRET).strip():
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
        except Exception as exc:
            raise RuntimeError(
                "Confidential client token acquisition failed. Check CLIENT_SECRET, "
                "CLIENT_ID, and that the app registration has Application permissions "
                f"and admin consent. Original error: {exc}"
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
            flow = app.initiate_device_flow(scopes=SCOPES)
            if "user_code" not in flow:
                raise RuntimeError(f"Failed to create device flow: {flow}")
            log(flow["message"])
            result = app.acquire_token_by_device_flow(flow)

    save_cache(cache)
    if not result or "access_token" not in result:
        if isinstance(result, dict) and result.get("error"):
            raise RuntimeError(
                f"Token error: {result.get('error')} - {result.get('error_description', '')}"
            )
        raise RuntimeError(f"Token error: {result}")
    return result["access_token"]


def graph_post_sendmail(access_token: str, payload: Dict[str, Any]) -> requests.Response:
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    return requests.post(GRAPH_SENDMAIL, headers=headers, data=json.dumps(payload), timeout=60)
