#!/usr/bin/env python3
"""Stage 2 — Apollo contact enrichment for already-qualified accounts.

Apollo is used only to resolve orgs (domain → org id) and to search people
at those stored domains. It is not used to discover the company universe.
No email is sent from this job.

Default is a dry-run cap: --limit-accounts 15. Full run: --full.

Usage:
  python enrich_apollo.py
  python enrich_apollo.py --limit-accounts 2
  python enrich_apollo.py --full
  python enrich_apollo.py --probe   # org enrich only, one domain, to verify the key
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests
import yaml
from dotenv import load_dotenv

from db import Database, normalize_domain, open_db, utcnow
from exclusions import BECHTLE_DOMAINS

load_dotenv()

HERE = os.path.dirname(os.path.abspath(__file__))
APOLLO_BASE = "https://api.apollo.io/api/v1"

TITLE_BUCKETS = {
    "cio_cdo": ("cio", "cdo", "chief information", "chief digital"),
    "it_lead": (
        "leiter it",
        "it-leiter",
        "it leiter",
        "head of it",
        "vp it",
        "director it",
        "it director",
        "information technology",
    ),
    "master_data": (
        "stammdaten",
        "master data",
        "sap mdg",
        "mdg",
        "data governance",
        "data quality",
    ),
    "s4": ("s/4hana", "s4hana", "s/4 hana", "programme", "transformation", "erp"),
    "project": (
        "projektleiter",
        "project manager",
        "program manager",
        "programme manager",
        "it-projekte",
        "it project",
        "sap project",
        "sap lead",
        "leiter sap",
    ),
    "organisation": ("organisation", "organization", "leiter organisation", "digital"),
}

IT_TITLES = [
    "CIO",
    "CDO",
    "Head of IT",
    "IT Director",
    "Director IT",
    "VP IT",
    "Leiter IT",
    "IT-Leiter",
    "Head of Information Technology",
    "Head of Digital",
    "Leiter Digitalisierung",
]
PROJECT_TITLES = [
    "SAP MDG",
    "S/4HANA Programme",
    "S/4HANA Project",
    "SAP Project Manager",
    "Projektleiter SAP",
    "Projektleiter S/4HANA",
    "Program Manager",
    "Head of ERP",
    "Leiter SAP",
    "SAP Lead",
    "Head of Applications",
    "IT Project Manager",
    "Leiter IT-Projekte",
    "Head of IT Projects",
    "Transformation Manager",
    "Leiter Stammdaten",
    "Head of Master Data",
    "Head of Data Governance",
    "Leiter Organisation",
]


def load_config(path: Optional[str] = None) -> dict:
    with open(path or os.path.join(HERE, "config.yaml"), "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def api_key() -> str:
    key = (os.getenv("APOLLO_API_KEY") or "").strip()
    if not key:
        raise SystemExit(
            "APOLLO_API_KEY is not set. Stage 2 needs it in .env; Stage 1 does not."
        )
    return key


def headers(key: str) -> dict:
    return {
        "Content-Type": "application/json",
        "Cache-Control": "no-cache",
        "X-Api-Key": key,
        "x-api-key": key,
    }


class RateLimitError(RuntimeError):
    def __init__(self, retry_after: float, body: str):
        super().__init__(body)
        self.retry_after = retry_after


def _retry_after(resp: requests.Response) -> float:
    raw = resp.headers.get("Retry-After") or resp.headers.get("retry-after")
    if raw:
        try:
            return float(raw)
        except ValueError:
            return 60.0
    return 60.0


def apollo_request(
    method: str,
    path: str,
    key: str,
    *,
    params: Optional[dict] = None,
    json_body: Optional[dict] = None,
    sleep_s: float = 1.2,
    retries: int = 5,
    timeout: int = 45,
) -> dict:
    url = f"{APOLLO_BASE}{path}"
    last_error: Optional[Exception] = None
    for attempt in range(retries):
        try:
            resp = requests.request(
                method,
                url,
                headers=headers(key),
                params=params,
                json=json_body,
                timeout=timeout,
            )
        except requests.RequestException as exc:
            last_error = exc
            time.sleep(min(30, sleep_s * (attempt + 1)))
            continue
        if resp.status_code == 429:
            wait = _retry_after(resp)
            time.sleep(wait)
            last_error = RateLimitError(wait, resp.text[:300])
            continue
        if resp.status_code >= 500:
            last_error = RuntimeError(f"Apollo {resp.status_code}: {resp.text[:300]}")
            time.sleep(min(30, sleep_s * (attempt + 1)))
            continue
        if resp.status_code >= 400:
            raise RuntimeError(f"Apollo {path} {resp.status_code}: {resp.text[:500]}")
        time.sleep(sleep_s)
        if not resp.content:
            return {}
        try:
            return resp.json()
        except ValueError as exc:
            raise RuntimeError(f"Apollo {path} returned non-JSON") from exc
    raise RuntimeError(f"Apollo {path} failed after retries: {last_error}")


def enrich_org(key: str, domain: str, cfg: dict) -> dict:
    pipe = cfg.get("pipeline") or {}
    data = apollo_request(
        "GET",
        "/organizations/enrich",
        key,
        params={"domain": domain},
        sleep_s=float(pipe.get("apollo_sleep_seconds", 1.2)),
        retries=int(pipe.get("apollo_max_retries", 5)),
        timeout=int(pipe.get("apollo_timeout_seconds", 45)),
    )
    return data.get("organization") or {}


def people_search(
    key: str,
    domain: str,
    org_id: Optional[str],
    cfg: dict,
    titles: Optional[List[str]] = None,
) -> List[dict]:
    pipe = cfg.get("pipeline") or {}
    per_page = int(pipe.get("people_per_page", 100))
    max_pages = int(pipe.get("people_max_pages", 3))
    title_list = titles if titles is not None else list(cfg.get("suggested_titles") or [])
    found: List[dict] = []
    seen = set()
    for page in range(1, max_pages + 1):
        params: List[Tuple[str, str]] = [
            ("person_locations[]", loc)
            for loc in (cfg.get("apollo_person_locations") or ["Germany"])
        ]
        for sen in cfg.get("apollo_seniorities") or ["director", "vp", "c_suite", "head", "manager"]:
            params.append(("person_seniorities[]", sen))
        for title in title_list:
            params.append(("person_titles[]", title))
        params.append(("q_organization_domains_list[]", domain))
        if org_id:
            params.append(("organization_ids[]", org_id))
        params.append(("include_similar_titles", "true"))
        params.append(("per_page", str(per_page)))
        params.append(("page", str(page)))

        data = apollo_request(
            "POST",
            "/mixed_people/api_search",
            key,
            params=params,
            json_body={},
            sleep_s=float(pipe.get("apollo_sleep_seconds", 1.2)),
            retries=int(pipe.get("apollo_max_retries", 5)),
            timeout=int(pipe.get("apollo_timeout_seconds", 45)),
        )
        people = data.get("people") or data.get("contacts") or []
        if not people:
            break
        for person in people:
            pid = person.get("id")
            if not pid or pid in seen:
                continue
            seen.add(pid)
            found.append(person)
        if len(people) < per_page:
            break
    return found


def search_people_for_account(key: str, domain: str, org_id: Optional[str], cfg: dict) -> List[dict]:
    """IT leadership + project/SAP field titles, merged and de-duped."""
    by_id: Dict[str, dict] = {}
    for titles in (IT_TITLES, PROJECT_TITLES):
        for person in people_search(key, domain, org_id, cfg, titles=titles):
            pid = person.get("id")
            if pid:
                by_id[str(pid)] = person
    return list(by_id.values())


def bulk_match(key: str, person_ids: Sequence[str], cfg: dict) -> List[dict]:
    if not person_ids:
        return []
    pipe = cfg.get("pipeline") or {}
    matches: List[dict] = []
    ids = [str(pid) for pid in person_ids if pid]
    for i in range(0, len(ids), 10):
        chunk = ids[i : i + 10]
        data = apollo_request(
            "POST",
            "/people/bulk_match",
            key,
            json_body={
                "details": [{"id": pid} for pid in chunk],
                "reveal_personal_emails": False,
            },
            sleep_s=float(pipe.get("apollo_sleep_seconds", 1.2)),
            retries=int(pipe.get("apollo_max_retries", 5)),
            timeout=int(pipe.get("apollo_timeout_seconds", 45)),
        )
        batch = data.get("matches") or data.get("people") or []
        for match in batch:
            if match and match.get("id"):
                matches.append(match)
    return matches


def title_bucket(title: str) -> str:
    blob = (title or "").lower()
    for bucket, needles in TITLE_BUCKETS.items():
        if any(n in blob for n in needles):
            return bucket
    return "other"


def is_personal_email(email: Optional[str], personal_domains: Iterable[str]) -> bool:
    if not email or "@" not in email:
        return True
    domain = email.rsplit("@", 1)[-1].strip().lower()
    return domain in {d.lower() for d in personal_domains}


def pick_contacts(people: List[dict], lo: int, hi: int) -> List[dict]:
    ranked: List[Tuple[int, str, dict]] = []
    seen_ids = set()
    for person in people:
        pid = person.get("id") or person.get("apollo_id")
        if not pid or pid in seen_ids:
            continue
        seen_ids.add(pid)
        title = person.get("title") or ""
        bucket = title_bucket(title)
        # Prefer people Apollo already flags as having a work email.
        has_email = 1 if person.get("has_email") else 0
        ranked.append((has_email, bucket, person))

    chosen: List[dict] = []
    used_buckets = set()
    # First pass: one per function bucket.
    for has_email, bucket, person in sorted(ranked, key=lambda x: (-x[0], x[1])):
        if bucket in used_buckets and bucket != "other":
            continue
        chosen.append(person)
        used_buckets.add(bucket)
        if len(chosen) >= hi:
            return chosen
    # Fill remaining slots with leftover people (different titles).
    chosen_ids = {p.get("id") for p in chosen}
    for has_email, bucket, person in sorted(ranked, key=lambda x: -x[0]):
        if person.get("id") in chosen_ids:
            continue
        chosen.append(person)
        if len(chosen) >= hi:
            break
    return chosen[:hi]


def person_to_contact(account_id: int, person: dict, personal_domains: List[str]) -> Optional[dict]:
    pid = person.get("id") or person.get("apollo_id")
    if not pid:
        return None
    email = (person.get("email") or person.get("work_email") or "").strip() or None
    if email and email.lower() in {"unavailable", "not_found", "n/a"}:
        email = None
    if email and is_personal_email(email, personal_domains):
        email = None
    country = person.get("country") or (person.get("address") or {}).get("country")
    city = person.get("city") or (person.get("address") or {}).get("city")
    return {
        "account_id": account_id,
        "apollo_person_id": str(pid),
        "first_name": person.get("first_name"),
        "last_name": person.get("last_name") or person.get("last_name_obfuscated"),
        "title": person.get("title"),
        "seniority": person.get("seniority")
        or (person.get("employment_history") or [{}])[0].get("title"),
        "email": email,
        "email_status": person.get("email_status"),
        "linkedin_url": person.get("linkedin_url"),
        "city": city,
        "country": country or "Germany",
        "lawful_basis": "legitimate_interest_b2b",
        "raw_json": json.dumps(person, ensure_ascii=False, default=str),
    }


def probe(domain: str, cfg: dict) -> int:
    key = api_key()
    print(f"Probing Apollo org enrich for domain={domain} (key not printed)")
    org = enrich_org(key, domain, cfg)
    if not org:
        print("No organization returned. Check the key scope for organizations/enrich.")
        return 1
    print(f"  org id : {org.get('id')}")
    print(f"  name   : {org.get('name')}")
    print(f"  domain : {org.get('primary_domain')}")
    print(f"  city   : {org.get('city')}")
    print(f"  country: {org.get('country')}")
    print("Apollo API is working.")
    return 0


def enrich_accounts(
    db_path: Optional[str],
    cfg: dict,
    limit: Optional[int],
    full: bool,
) -> None:
    key = api_key()
    pipe = cfg.get("pipeline") or {}
    if full:
        cap = None
    else:
        cap = limit if limit is not None else int(pipe.get("default_limit_accounts", 15))
    personal = list(cfg.get("personal_mail_domains") or [])
    suppressed = set(cfg.get("suppressed_domains") or []) | BECHTLE_DOMAINS
    lo = int(pipe.get("contacts_min_per_account", 3))
    hi = int(pipe.get("contacts_max_per_account", 8))

    with open_db(db_path) as (db, conn):
        accounts = db.qualified_accounts(conn, limit=cap)
        run_id = db.start_run(
            conn,
            "apollo",
            notes=(
                f"Stage 2 contacts-only; limit={cap if cap else 'full'}; "
                "no industry/revenue search; no outreach"
            ),
        )
        written = 0
        skipped = 0
        errors: List[str] = []
        try:
            already_ids = {
                str(r["apollo_person_id"])
                for r in db.fetchall(
                    conn,
                    "SELECT apollo_person_id FROM contacts WHERE apollo_person_id IS NOT NULL AND email IS NOT NULL AND email != ''",
                )
                if r.get("apollo_person_id")
            }
            for acc in accounts:
                domain = normalize_domain(acc.get("domain"))
                if not domain or domain in suppressed or acc.get("status") != "qualified":
                    skipped += 1
                    continue
                have = db.fetchone(
                    conn,
                    "SELECT COUNT(*) AS n FROM contacts WHERE account_id=? AND email IS NOT NULL AND email != ''",
                    (acc["id"],),
                )
                have_n = int((have or {}).get("n") or 0)
                if have_n >= hi:
                    print(f"→ {acc['name']} ({domain}) skip, already {have_n} emails")
                    skipped += 1
                    continue
                print(f"→ {acc['name']} ({domain})")
                try:
                    org_id = acc.get("apollo_org_id")
                    # People search is 0 credits by domain. Skip org enrich when
                    # we already have an id, and do not fail the account if enrich
                    # is out of credits — search still works on domain alone.
                    if not org_id:
                        try:
                            org = enrich_org(key, domain, cfg)
                            org_id = org.get("id")
                        except RuntimeError as exc:
                            if "insufficient credits" not in str(exc).lower():
                                raise
                            print("  org enrich skipped (credits); searching by domain")
                    if org_id:
                        db.execute(
                            conn,
                            "UPDATE accounts SET apollo_org_id = ?, updated_at = ? WHERE id = ?",
                            (org_id, utcnow(), acc["id"]),
                        )
                    people = search_people_for_account(key, domain, org_id, cfg)
                    picked = pick_contacts(people, lo, hi)
                    ids = [
                        p.get("id")
                        for p in picked
                        if p.get("id") and str(p.get("id")) not in already_ids
                    ]
                    enriched_by_id: Dict[str, dict] = {}
                    if ids:
                        try:
                            for match in bulk_match(key, ids, cfg):
                                if match and match.get("id"):
                                    enriched_by_id[str(match["id"])] = match
                        except RuntimeError as exc:
                            print(f"  bulk_match skipped: {exc}")
                    for person in picked:
                        pid = str(person.get("id") or "")
                        merged = {**person, **(enriched_by_id.get(pid) or {})}
                        rec = person_to_contact(int(acc["id"]), merged, personal)
                        if not rec:
                            continue
                        db.upsert_contact(conn, rec)
                        written += 1
                        try:
                            from pipeline_crm import upsert_contact as crm_upsert_contact

                            crm_upsert_contact(domain, rec)
                        except Exception as crm_exc:
                            print(f"  CRM contact sync skipped: {crm_exc}")
                    conn.commit()
                    print(
                        f"  org={org_id or '—'} people={len(people)} stored={len(picked)}"
                    )
                except Exception as exc:
                    errors.append(f"{domain}: {exc}")
                    print(f"  ERROR {domain}: {exc}")
                    conn.rollback()
            note = f"skipped={skipped}; errors={len(errors)}"
            if errors:
                note += " | " + " ; ".join(errors)[:1500]
            db.finish_run(
                conn,
                run_id,
                rows_written=written,
                notes=note,
                error="; ".join(errors)[:2000] if errors else None,
            )
        except Exception as exc:
            db.finish_run(conn, run_id, rows_written=written, error=str(exc))
            raise

        n_contacts = db.contact_count(conn)
        print("")
        print(f"Stage 2 done. contacts upserted this run={written}, table size={n_contacts}")
        print("No email was sent. Lawful basis: legitimate interest for B2B.")


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Stage 2: Apollo contacts for qualified domains")
    parser.add_argument("--db", help="SQLite path (default: DB_PATH / outreach_log.sqlite)")
    parser.add_argument("--config", help="Path to config.yaml")
    parser.add_argument(
        "--limit-accounts",
        type=int,
        default=None,
        help="Max qualified accounts to enrich (default 15 unless --full)",
    )
    parser.add_argument("--full", action="store_true", help="Enrich every qualified account")
    parser.add_argument(
        "--probe",
        nargs="?",
        const="beiersdorf.com",
        help="Verify the Apollo key with org enrich (default domain: beiersdorf.com)",
    )
    args = parser.parse_args(argv)
    cfg = load_config(args.config)
    if args.probe:
        return probe(normalize_domain(args.probe), cfg)
    enrich_accounts(args.db, cfg, args.limit_accounts, args.full)
    return 0


if __name__ == "__main__":
    sys.exit(main())
