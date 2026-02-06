import os
import csv
import time
import math
import argparse
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple, Optional, Set

import requests
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://api.apollo.io/api/v1"
PEOPLE_SEARCH_ENDPOINT = f"{BASE_URL}/mixed_people/api_search"
PEOPLE_BULK_ENRICH_ENDPOINT = f"{BASE_URL}/people/bulk_match"

# --- Smart exclusions (tune to your liking) ---
EXCLUDE_COMPANY_KEYWORDS = [
    # Global consultancies / SIs / agencies (common non-end-customer noise)
    "accenture", "deloitte", "pwc", "kpmg", "ernst", "ey", "capgemini", "ibm",
    "infosys", "tata consultancy", "tcs", "wipro", "cognizant", "ntt data", "atos",
    "cgi", "hcl", "tech mahindra", "sap consulting", "bearingpoint",
    "mckinsey", "bain", "bcg", "oliver wyman",
    "system integrator", "systems integrator", "si partner",
    "recruiting", "staffing", "headhunt", "talent acquisition",
]

EXCLUDE_TITLE_KEYWORDS = [
    # Roles you likely don't want for end-customer buying cycles
    "recruiter", "talent", "sales development", "sdr", "bdr", "account executive",
    "marketing", "growth", "partner", "principal consultant", "consultant",
]

def apollo_headers(api_key: str) -> Dict[str, str]:
    # Docs/examples use X-Api-Key style headers for API Search and enrichment tutorials.
    return {
        "accept": "application/json",
        "Content-Type": "application/json",
        "Cache-Control": "no-cache",
        "X-Api-Key": api_key,
    }

def build_query_params(filters: Dict[str, Any]) -> List[Tuple[str, str]]:
    """
    Apollo People API Search expects many filters as repeated query params like:
      person_titles[]=...&person_titles[]=...
    This helper converts python lists -> repeated params.
    """
    params: List[Tuple[str, str]] = []
    for k, v in filters.items():
        if v is None:
            continue
        if isinstance(v, bool):
            params.append((k, "true" if v else "false"))
        elif isinstance(v, (int, float)):
            params.append((k, str(v)))
        elif isinstance(v, str):
            params.append((k, v))
        elif isinstance(v, list):
            # Use brackets convention: key[]=value1&key[]=value2
            key = f"{k}[]"
            for item in v:
                if item is None:
                    continue
                params.append((key, str(item)))
        else:
            # Fallback: stringify
            params.append((k, str(v)))
    return params

def post_with_backoff(url: str, headers: Dict[str, str], params: List[Tuple[str, str]],
                      json_body: Optional[Dict[str, Any]] = None,
                      max_retries: int = 6) -> Dict[str, Any]:
    delay = 1.0
    for attempt in range(max_retries):
        r = requests.post(url, headers=headers, params=params, json=json_body, timeout=60)
        if r.status_code == 200:
            return r.json()
        # Basic rate-limit/backoff handling
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(delay)
            delay = min(delay * 2, 30)
            continue
        # Hard failure
        try:
            err = r.json()
        except Exception:
            err = {"text": r.text}
        raise RuntimeError(f"Request failed: {r.status_code} {err}")
    raise RuntimeError(f"Request failed after retries: {url}")

def normalize_text(s: Optional[str]) -> str:
    return (s or "").strip().lower()

def is_excluded_company(company_name: Optional[str]) -> bool:
    n = normalize_text(company_name)
    return any(kw in n for kw in EXCLUDE_COMPANY_KEYWORDS)

def is_excluded_title(title: Optional[str]) -> bool:
    t = normalize_text(title)
    return any(kw in t for kw in EXCLUDE_TITLE_KEYWORDS)

def parse_people_from_search(resp: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Be defensive: Apollo may use different keys across versions.
    We try common ones.
    """
    if "people" in resp and isinstance(resp["people"], list):
        return resp["people"]
    if "results" in resp and isinstance(resp["results"], list):
        return resp["results"]
    if "contacts" in resp and isinstance(resp["contacts"], list):
        return resp["contacts"]
    return []

def get_person_id(p: Dict[str, Any]) -> Optional[str]:
    # Docs mention person_id; some payloads use id.
    return p.get("person_id") or p.get("id")

def chunked(lst: List[Any], n: int) -> List[List[Any]]:
    return [lst[i:i+n] for i in range(0, len(lst), n)]

def load_existing_person_ids(output_csv: str) -> Set[str]:
    if not os.path.exists(output_csv):
        return set()
    ids: Set[str] = set()
    with open(output_csv, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            pid = row.get("person_id")
            if pid:
                ids.add(pid)
    return ids

def write_rows(output_csv: str, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    fieldnames = sorted(rows[0].keys())
    file_exists = os.path.exists(output_csv)
    with open(output_csv, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        for r in rows:
            writer.writerow(r)

def default_country_list() -> List[str]:
    # Keep editable; Apollo location strings can be country names or "City, Country" formats.
    return [
        # Europe
        "Germany", "Austria", "Switzerland", "Netherlands", "Belgium", "France",
        "United Kingdom", "Ireland", "Spain", "Italy", "Portugal",
        "Sweden", "Norway", "Denmark", "Finland", "Poland", "Czech Republic",
        "Hungary", "Romania", "Greece", "Turkey",
        # Middle East
        "United Arab Emirates", "Saudi Arabia", "Qatar", "Kuwait", "Bahrain", "Oman",
        "Israel", "Jordan", "Lebanon",
        # Asia
        "India", "Singapore", "Malaysia", "Indonesia", "Thailand", "Vietnam",
        "Philippines", "Japan", "South Korea", "Hong Kong", "Taiwan",
    ]

def build_filter_mode(mode: str) -> Dict[str, Any]:
    """
    These are example “recipes” aligned to SAP MDG ICP.
    Adjust titles/industries/countries to match what your Apollo instance returns best.
    """
    countries = default_country_list()

    # Email status values per Apollo docs include: verified, unverified, likely to engage, unavailable. :contentReference[oaicite:7]{index=7}
    high_deliverability_email_status = ["verified", "likely to engage"]

    # Titles: intentionally redundant variations to catch title formatting differences
    core_titles = [
        "chief data officer", "cdo",
        "chief information officer", "cio",
        "head of data governance", "director data governance", "data governance lead",
        "master data manager", "master data lead", "mdm lead", "mdm manager",
        "data quality manager", "data quality lead",
        "head of erp", "erp director", "head of it applications", "it applications director",
        "sap enterprise architect", "enterprise architect sap", "head of sap", "sap director",
        "process owner", "procurement process owner", "finance process owner", "supply chain process owner",
    ]

    # Industries: these need to match Apollo's stored values; if you use organization_industries, it can be case-sensitive. :contentReference[oaicite:8]{index=8}
    # Start broad, then tighten. (You can also switch to industry tag IDs if you manage them.)
    mdg_industries = [
        "automotive",
        "chemicals",
        "pharmaceuticals",
        "medical devices",
        "oil & energy",
        "utilities",
        "logistics & supply chain",
        "food & beverages",
        "food production",
        "consumer goods",
        "retail",
        "telecommunications",
        "banking",
        "insurance",
        "industrial automation",
        "machinery",
        "mining & metals",
        "aviation & aerospace",
        "defense & space",
    ]

    # Employee ranges: Apollo commonly uses ranges like "501,1000" etc; adjust if your tenant expects other values.
    # The bigger the company, the more likely multi-entity + complex master data.
    enterprise_employee_ranges = ["1001,2000", "2001,5000", "5001,10000", "10001"]  # 10k+ as "10001" is commonly used in Apollo ecosystems
    mid_to_enterprise_ranges = ["201,500", "501,1000", "1001,2000", "2001,5000", "5001,10000", "10001"]

    if mode == "high_intent":
        return {
            # Company HQ filter (end customers in your regions)
            "organization_locations": countries,

            # People filters
            "person_titles": core_titles,
            "include_similar_titles": False,

            # Deliverability
            "contact_email_status": high_deliverability_email_status,

            # Firmographics (tight)
            "organization_industries": mdg_industries,
            "organization_num_employees_ranges": enterprise_employee_ranges,

            # Pagination
            "per_page": 100,
        }

    if mode == "scalable":
        expanded_titles = core_titles + [
            "data manager", "data governance manager", "mdm architect",
            "sap architect", "sap solution architect", "erp manager", "it manager erp",
            "head of master data", "data steward manager",
        ]
        return {
            "organization_locations": countries,
            "person_titles": expanded_titles,
            "include_similar_titles": True,
            "contact_email_status": high_deliverability_email_status,
            "organization_industries": mdg_industries,
            "organization_num_employees_ranges": mid_to_enterprise_ranges,
            "per_page": 100,
        }

    if mode == "hiring_signal":
        # Target companies hiring for MDM/Data Governance roles recently
        # (Using org job title filters is a common pattern; if your Apollo plan/tenant doesn't support these params, remove them.)
        since = (datetime.utcnow() - timedelta(days=120)).date().isoformat()
        return {
            "organization_locations": countries,
            "person_titles": core_titles,
            "include_similar_titles": True,
            "contact_email_status": high_deliverability_email_status,
            "organization_industries": mdg_industries,
            "organization_num_employees_ranges": mid_to_enterprise_ranges,

            # Hiring signal
            "q_organization_job_titles": [
                "master data", "data governance", "data steward", "data quality", "mdm", "sap mdg"
            ],
            "organization_job_posted_at_range_min": since,

            "per_page": 100,
        }

    raise ValueError(f"Unknown mode: {mode}")

def main():
    parser = argparse.ArgumentParser(description="Apollo SAP MDG prospecting bot: search -> enrich -> export emails")
    parser.add_argument("--mode", choices=["high_intent", "scalable", "hiring_signal"], default="high_intent")
    parser.add_argument("--max_pages", type=int, default=50, help="Safety cap. API supports up to 500 pages.")
    parser.add_argument("--output", type=str, default="apollo_mdg_leads.csv")
    parser.add_argument("--dry_run", action="store_true", help="Search only; do not enrich emails.")
    parser.add_argument("--sleep", type=float, default=0.4, help="Delay between API calls to be polite.")
    args = parser.parse_args()

    api_key = os.getenv("APOLLO_API_KEY")
    if not api_key:
        raise SystemExit("Missing APOLLO_API_KEY env var. Put it in .env or export it in your shell.")

    headers = apollo_headers(api_key)
    base_filters = build_filter_mode(args.mode)

    existing_ids = load_existing_person_ids(args.output)
    print(f"[i] Mode={args.mode} | Output={args.output} | Already have {len(existing_ids)} people in CSV")

    all_people: List[Dict[str, Any]] = []
    for page in range(1, args.max_pages + 1):
        filters = dict(base_filters)
        filters["page"] = page

        params = build_query_params(filters)
        try:
            
            resp = post_with_backoff(PEOPLE_SEARCH_ENDPOINT, headers=headers, params=params, json_body={})
        except Exception as e:
            print(f'posting error {e}')
            time.sleep(5)
            print(f'trying again')
            resp = post_with_backoff(PEOPLE_SEARCH_ENDPOINT, headers=headers, params=params, json_body={})

        people = parse_people_from_search(resp)

        if not people:
            print(f"[i] Page {page}: 0 results; stopping.")
            break

        # Basic cleaning + exclusions
        kept = []
        for p in people:
            pid = get_person_id(p)
            if not pid:
                continue
            if pid in existing_ids:
                continue

            title = p.get("title") or ""
            org_name = p.get("organization_name") or p.get("company") or p.get("organization", {}).get("name")

            if is_excluded_title(title):
                continue
            if is_excluded_company(org_name):
                continue

            kept.append(p)

        all_people.extend(kept)
        print(f"[i] Page {page}: {len(people)} raw | {len(kept)} kept | total kept={len(all_people)}")

        time.sleep(args.sleep)

    if args.dry_run:
        print(f"[DRY RUN] Found {len(all_people)} people to enrich (after exclusions).")
        return

    # Enrich in batches of 10 (Bulk People Enrichment supports up to 10 per call). :contentReference[oaicite:9]{index=9}
    rows_to_write: List[Dict[str, Any]] = []
    batches = chunked(all_people, 10)
    print(f"[i] Enriching {len(all_people)} people in {len(batches)} batches of 10...")

    for i, batch in enumerate(batches, start=1):
        details = []
        # Use IDs from search results
        for p in batch:
            pid = get_person_id(p)
            if pid:
                details.append({"id": pid})

        enrich_params = build_query_params({
            # Don’t pull personal emails unless you explicitly want them.
            "reveal_personal_emails": False,
            "reveal_phone_number": False,
        })

        enrich_resp = post_with_backoff(
            PEOPLE_BULK_ENRICH_ENDPOINT,
            headers=headers,
            params=enrich_params,
            json_body={"details": details},
        )

        matches = enrich_resp.get("matches", []) if isinstance(enrich_resp, dict) else []
        for m in matches:
            person_id = m.get("id") or m.get("person_id")
            org = m.get("organization") or {}

            row = {
                "person_id": person_id,
                "first_name": m.get("first_name"),
                "last_name": m.get("last_name"),
                "name": m.get("name"),
                "title": m.get("title"),
                "linkedin_url": m.get("linkedin_url"),

                "email": m.get("email"),
                "email_status": m.get("email_status"),

                "organization_id": m.get("organization_id") or org.get("id"),
                "organization_name": org.get("name"),
                "organization_domain": org.get("primary_domain") or org.get("website_url"),
                "organization_website": org.get("website_url"),
                "organization_country": org.get("country"),
                "organization_city": org.get("city"),
            }

            # Post-exclusion again (defensive)
            if is_excluded_company(row.get("organization_name")):
                continue
            if is_excluded_title(row.get("title")):
                continue

            rows_to_write.append(row)

        # Flush periodically
        if rows_to_write:
            write_rows(args.output, rows_to_write)
            existing_ids.update([r["person_id"] for r in rows_to_write if r.get("person_id")])
            rows_to_write = []

        print(f"[i] Batch {i}/{len(batches)} complete.")
        time.sleep(args.sleep)

    print(f"[✓] Done. CSV saved to: {args.output}")

if __name__ == "__main__":
    main()
