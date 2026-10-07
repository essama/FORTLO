"""Auto-refuel the outreach list from Apollo when it is empty.

Tier 1 (bechtle_band): German HQ, Bechtle-sized (€5bn–€10bn), Head of ERP / SAP.
Tier 2 (small_de): small German companies (config: refuel_small_de), Head of
ERP / IT / MDG. Accounts already in the CRM (or tried recently) are skipped, the
Apollo page cursor is persisted, and only genuinely NEW contacts are counted.
Military, financial-services and alcohol companies are excluded by scoring.
Caps at refuel.contacts_per_day new work emails per run.

Leadgen gate (config `leadgen`): every Apollo org is also scored by the profile
ICP in ./leadgen before any people search. In `enforce` mode only orgs leadgen
marks "pursue" are enriched and put in the campaign; in `shadow` mode the
score is only recorded. Scores land in customers.leadgen_*.

Leadgen sourcing (config `leadgen.search_tier`), run first:
  leadgen_queue      companies ticked on the leadgen /search page
  leadgen_hiring     profile search limited to companies with open SAP MDG / master-data jobs
                     (config `leadgen.signals.hiring_tier`)
  leadgen_lookalike  Claude names companies like the wins; kept only if Apollo has the same domain
  leadgen_icp        Apollo search built from the profile: DACH, won sectors, SAP, won size band
"""
from __future__ import annotations

import copy
import json
import os
import re
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

import yaml
from dotenv import load_dotenv

from build_accounts import score_account
from db import normalize_domain
from enrich_apollo import (
    api_key,
    apollo_request,
    bulk_match,
    is_personal_email,
    people_search,
    person_to_contact,
)
from exclusions import BECHTLE_DOMAINS
from helpers import log
from pipeline_crm import account_has_contacts, upsert_contact, upsert_customer

load_dotenv()

HERE = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(HERE, "logger", "refuel_state.json")
ALLOWED_EMAIL_STATUS = {"verified", "likely to engage"}

# NAICS prefixes we never target: financial services, alcohol, military, public sector,
# education and non-profit bodies.
EXCLUDED_NAICS = {
    "finance_insurance": ("52",),
    "alcohol": ("31212", "31213", "31214", "4248", "445310", "72241"),
    "military": ("336992", "332992", "332993", "332994", "928110", "336414"),
    "public_sector": ("92",),
    "education": ("61",),
    "nonprofit": ("813",),
}
PUBLIC_SECTOR_NAME_KEYWORDS = [
    "universit", "hochschule", "ministerium", "ministry", "bundesamt", "bundesagentur",
    "academy", "akademie", "stiftung", "foundation", "school of", "institut",
    "united nations", "unesco", "embl", "daad",
]
ERP_TITLES = [
    "Head of ERP",
    "Leiter ERP",
    "ERP Director",
    "Director ERP",
    "VP ERP",
    "Head of SAP",
    "Leiter SAP",
    "SAP Lead",
    "Head of Applications",
    "ERP Manager",
    "SAP ERP",
    "Leiter SAP ERP",
]


def load_config(path: Optional[str] = None) -> dict:
    with open(path or os.path.join(HERE, "config.yaml"), "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def _org_to_raw(org: dict) -> dict:
    domain = normalize_domain(
        org.get("primary_domain") or org.get("website_url") or org.get("domain")
    )
    revenue = org.get("annual_revenue") or org.get("organization_revenue")
    try:
        revenue = float(revenue) if revenue is not None else None
    except (TypeError, ValueError):
        revenue = None
    employees = org.get("estimated_num_employees") or org.get("organization_headcount")
    try:
        employees = int(employees) if employees is not None else None
    except (TypeError, ValueError):
        employees = None
    country = org.get("country") or "Germany"
    return {
        "name": (org.get("name") or "").strip(),
        "legal_name": (org.get("name") or "").strip(),
        "domain": domain,
        "website": org.get("website_url") or (f"https://{domain}" if domain else None),
        "linkedin_url": org.get("linkedin_url"),
        "city": org.get("city"),
        "country": country,
        "revenue_eur": revenue,
        "employees": employees,
        "industry": org.get("industry") or "",
        "apollo_org_id": org.get("id"),
        "source_urls": [f"apollo:org:{org.get('id')}"] if org.get("id") else [],
    }


def search_bechtle_band_orgs(key: str, cfg: dict, pages: int = 5) -> List[dict]:
    pipe = cfg.get("pipeline") or {}
    icp = cfg.get("icp") or {}
    min_eur = int(icp.get("revenue_min_eur", 5_000_000_000))
    max_eur = int(icp.get("revenue_max_eur", 10_000_000_000))
    found: List[dict] = []
    seen = set()
    for page in range(1, pages + 1):
        params: List[Tuple[str, str]] = [
            ("organization_locations[]", "Germany"),
            ("revenue_range[min]", str(min_eur)),
            ("revenue_range[max]", str(max_eur)),
            ("organization_num_employees_ranges[]", "5001,10000"),
            ("organization_num_employees_ranges[]", "10001,20000"),
            ("organization_num_employees_ranges[]", "20001,50000"),
            ("per_page", "100"),
            ("page", str(page)),
        ]
        data = apollo_request(
            "POST",
            "/mixed_companies/search",
            key,
            params=params,
            json_body={},
            sleep_s=float(pipe.get("apollo_sleep_seconds", 1.2)),
            retries=int(pipe.get("apollo_max_retries", 5)),
            timeout=int(pipe.get("apollo_timeout_seconds", 45)),
        )
        orgs = data.get("organizations") or data.get("accounts") or data.get("companies") or []
        if not orgs:
            break
        for org in orgs:
            oid = org.get("id") or normalize_domain(org.get("primary_domain") or "")
            if not oid or oid in seen:
                continue
            seen.add(oid)
            found.append(org)
        if len(orgs) < 100:
            break
    return found


def _load_state() -> Dict[str, Any]:
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as fh:
            data = json.load(fh)
            return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def _save_state(state: Dict[str, Any]) -> None:
    try:
        os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
        with open(STATE_FILE, "w", encoding="utf-8") as fh:
            json.dump(state, fh, indent=2)
    except OSError as exc:
        log(f"[!] Could not save refuel state: {exc}")


def _word_hit(text: str, words: List[str]) -> bool:
    for w in words:
        w = w.lower().strip()
        if w and re.search(r"(?<![a-z0-9])" + re.escape(w) + (r"(?![a-z0-9])" if len(w) <= 3 else ""), text):
            return True
    return False


LEADERSHIP = re.compile(
    r"(?<![a-z0-9])(head|leiter|leiterin|leitung|director|direktor|chief|cio|cdo|vp|vice president|lead|bereichsleiter|abteilungsleiter)(?![a-z0-9])"
)
SPECIFIC_DOMAIN = ("erp", "sap", "mdg", "master data", "stammdaten", "data governance")


def _title_ok(title: str, must: List[str], reject: List[str], need_leader: bool = False) -> bool:
    t = (title or "").lower()
    if not t:
        return False
    if need_leader and not LEADERSHIP.search(t):
        # plain "IT Manager" is not enough, but "ERP Manager" / "Master Data Manager" are
        if not (_word_hit(t, list(SPECIFIC_DOMAIN)) and "manager" in t):
            return False
    if reject and re.search(
        r"(?<![a-z0-9])(" + "|".join(re.escape(w.lower()) for w in reject) + r")(?![a-z0-9])", t
    ):
        return False
    return _word_hit(t, must)


def search_orgs_page(
    key: str,
    cfg: dict,
    page: int,
    employee_ranges: List[str],
    min_eur: Optional[int] = None,
    max_eur: Optional[int] = None,
    base_params: Optional[List[Tuple[str, str]]] = None,
) -> List[dict]:
    """One page of Apollo orgs. `base_params` (leadgen profile filters) replaces the size/location filters."""
    if base_params is not None:
        params: List[Tuple[str, str]] = list(base_params)
    else:
        params = [("organization_locations[]", "Germany")]
        if min_eur is not None:
            params.append(("revenue_range[min]", str(min_eur)))
        if max_eur is not None:
            params.append(("revenue_range[max]", str(max_eur)))
        for rng in employee_ranges:
            params.append(("organization_num_employees_ranges[]", rng))
    params += [("per_page", "100"), ("page", str(page))]
    return _org_search(key, cfg, params)


def _org_search(key: str, cfg: dict, params: List[Tuple[str, str]]) -> List[dict]:
    pipe = cfg.get("pipeline") or {}
    data = apollo_request(
        "POST",
        "/mixed_companies/search",
        key,
        params=params,
        json_body={},
        sleep_s=float(pipe.get("apollo_sleep_seconds", 1.2)),
        retries=int(pipe.get("apollo_max_retries", 5)),
        timeout=int(pipe.get("apollo_timeout_seconds", 45)),
    )
    return data.get("organizations") or data.get("accounts") or data.get("companies") or []


def _naics_exclusion(org: dict) -> Optional[str]:
    codes = org.get("naics_codes") or []
    if isinstance(codes, (str, int)):
        codes = [codes]
    for code in codes:
        code = str(code.get("code") if isinstance(code, dict) else code).strip()
        for label, prefixes in EXCLUDED_NAICS.items():
            if code.startswith(prefixes):
                return f"{label}:naics{code}"
    return None


def _leadgen_gate(cfg: dict):
    """Build the leadgen gate, or None (FORTLO's own rules then apply unchanged)."""
    lg = cfg.get("leadgen") or {}
    if not lg.get("enabled"):
        return None
    try:
        from leadgen.gate import LeadGate

        gate = LeadGate(pursue_at=int(lg.get("pursue_at", 60)))
        if not gate.icp().get("win_count"):
            log("[!] Leadgen gate off: the profile ICP has no wins (check LEADGEN_PROFILE).")
            return None
        log(f"Leadgen gate on ({lg.get('mode', 'enforce')}), pursue at {gate.pursue_at}, "
            f"model {'on' if gate.client else 'off (no ANTHROPIC_API_KEY)'}")
        return gate
    except Exception as exc:  # never block refuel on the gate
        log(f"[!] Leadgen gate unavailable, using FORTLO rules only: {exc}")
        return None


def _prefetch(tier: Dict[str, Any], orgs: List[dict]) -> None:
    gate = tier.get("gate")
    if gate is None or not orgs:
        return
    try:
        summary = gate.prefetch([_org_to_raw(org) for org in orgs])
        if summary.get("requested"):
            log(f"Leadgen AI classified {summary.get('classified')}/{summary.get('requested')} orgs"
                + (f", errors: {summary['errors'][:1]}" if summary.get("errors") else ""))
    except Exception as exc:
        log(f"  leadgen prefetch skipped: {exc}")


def _profile_sourcing(cfg: dict, gate, key: str, tiers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Point the leadgen_icp tier at the profile filters, and build the queue + lookalike tiers.

    Returns the extra tiers (picked on the leadgen page, then Claude lookalikes) to run first.
    Any failure here leaves FORTLO's tiers unchanged.
    """
    lg_tier = next((t for t in tiers if t["name"] == "leadgen_icp"), None)
    if gate is None or lg_tier is None:
        return []
    st_cfg = (cfg.get("leadgen") or {}).get("search_tier") or {}
    forced = os.getenv("LEADGEN_SOURCING_FORCE", "").lower() in {"1", "true", "yes"}
    if not (st_cfg.get("profile_sourcing") or forced):
        return []
    from leadgen import sourcing

    icp = gate.icp()
    spec = sourcing.profile_spec(icp, st_cfg)
    lg_tier["base_params"] = sourcing.apollo_params(spec)
    lg_tier["base_params_no_tech"] = sourcing.apollo_params(spec, technologies=False)
    lg_tier["spec_hash"] = sourcing.spec_hash(spec)
    people_cfg = copy.deepcopy(cfg)
    people_cfg["apollo_person_locations"] = spec["locations"]
    lg_tier["people_cfg"] = people_cfg
    if st_cfg.get("ai_titles", True):
        extra = sourcing.title_variants(gate.client, lg_tier["titles"])
        lg_tier["titles"] = sourcing.merge_titles(lg_tier["titles"], extra)
    log(f"Leadgen profile search: {spec['explain']} Tags: {', '.join(spec['keyword_tags'][:8])}. "
        f"{len(lg_tier['titles'])} titles.")

    def list_tier(name: str) -> Dict[str, Any]:
        return {**lg_tier, "name": name, "base_params": None, "list": True}

    lg_tier["big_groups"] = list(cfg.get("too_big_groups") or [])
    lg_tier["max_per_group"] = int(st_cfg.get("max_contacts_per_group", 3))
    out = [list_tier("leadgen_queue")]
    sig_cfg = _signals_cfg(cfg)
    if sig_cfg and sig_cfg.get("hiring_tier", True):
        tier = list_tier("leadgen_hiring")
        tier["hiring_params"] = sourcing.apollo_params(spec, technologies=False) + [
            ("q_organization_job_titles[]", title) for title in sig_cfg.get("hiring_titles") or HIRING_TITLES
        ]
        out.append(tier)
    if st_cfg.get("lookalikes", True):
        tier = list_tier("leadgen_lookalike")
        tier["lookalikes_per_run"] = int(st_cfg.get("lookalikes_per_run", 25))
        tier["lookalike_revenue_max_eur"] = float(st_cfg.get("lookalike_revenue_max_eur", 10_000_000_000))
        tier["icp"] = icp
        tier["countries"] = spec["locations"]
        out.append(tier)
    return out


def _list_tier_orgs(tier: Dict[str, Any], key: str, cfg: dict, dry_run: bool) -> List[dict]:
    from leadgen import sourcing

    if tier["name"] == "leadgen_queue":
        return sourcing.queue_pending()
    if tier["name"] == "leadgen_hiring":
        return _hiring_orgs(tier, key, cfg, dry_run)
    entries = sourcing.lookalikes(
        tier["icp"], tier["gate"].client, want=tier["lookalikes_per_run"], countries=tier["countries"],
        exclude=list(cfg.get("too_big_groups") or []), revenue_max_eur=tier["lookalike_revenue_max_eur"],
    )
    orgs: List[dict] = []
    updates: Dict[str, str] = {}
    for entry in entries:
        try:
            org = sourcing.resolve_in_apollo(entry, lambda params: _org_search(key, cfg, params))
        except Exception as exc:
            log(f"  lookalike lookup failed for {entry['name']}: {exc}")
            continue
        updates[entry["name"]] = "processed" if org else "not_found"
        if org:
            if not org.get("country") and entry.get("country"):
                org["country"] = entry["country"]
            orgs.append(org)
        if dry_run:
            log(f"  [dry-run] lookalike {entry['name']} ({entry.get('domain')}): "
                f"{'found ' + str(org.get('primary_domain')) if org else 'not in Apollo'}. {entry.get('why')}")
    if not dry_run and updates:
        sourcing.mark_lookalikes(updates)
    log(f"Refuel[{tier['name']}]: {len(entries)} Claude suggestions, {len(orgs)} found in Apollo")
    return orgs


HIRING_TITLES = ["SAP MDG", "Master Data", "Stammdaten", "Data Governance", "S/4HANA"]


def _signals_cfg(cfg: dict) -> Optional[dict]:
    """config `leadgen.signals` when switched on (or forced for one run), else None."""
    sig = (cfg.get("leadgen") or {}).get("signals") or {}
    forced = os.getenv("LEADGEN_SIGNALS_FORCE", "").lower() in {"1", "true", "yes"}
    return sig if (sig.get("enabled") or forced) else None


def _hiring_orgs(tier: Dict[str, Any], key: str, cfg: dict, dry_run: bool) -> List[dict]:
    """One page of profile-matching companies that Apollo lists with matching open jobs. Cursor wraps."""
    state = tier["state"]
    cursor = int(state.setdefault("cursor", {}).get(tier["name"], 1))
    orgs = _org_search(key, cfg, tier["hiring_params"] + [("per_page", "100"), ("page", str(cursor))])
    if not orgs and cursor > 1:
        cursor = 1
        orgs = _org_search(key, cfg, tier["hiring_params"] + [("per_page", "100"), ("page", "1")])
    state["cursor"][tier["name"]] = cursor + 1 if orgs else 1
    log(f"Refuel[{tier['name']}]: page {cursor} returned {len(orgs)} orgs hiring for SAP / master data")
    return orgs


def _job_postings(key: str, cfg: dict, org_id: str) -> List[dict]:
    pipe = cfg.get("pipeline") or {}
    data = apollo_request(
        "GET",
        f"/organizations/{org_id}/job_postings",
        key,
        params={"per_page": 100, "page": 1},
        sleep_s=float(pipe.get("apollo_sleep_seconds", 1.2)),
        retries=int(pipe.get("apollo_max_retries", 5)),
        timeout=int(pipe.get("apollo_timeout_seconds", 45)),
    )
    return data.get("organization_job_postings") or data.get("job_postings") or []


def _signal_runtime(cfg: dict, key: str) -> Optional[Dict[str, Any]]:
    sig = _signals_cfg(cfg)
    if not sig:
        return None
    return {
        "min_score": int(sig.get("min_score", 45)),
        "jobs_left": int(sig.get("jobs_per_run", 60)),
        "research_min_score": int(sig.get("research_min_score", 50)),
        "research_left": int(sig.get("research_per_run", 10)),
        "fetch_jobs": lambda org_id: _job_postings(key, cfg, org_id),
        "stats": {"jobs_checked": 0, "researched": 0, "with_signals": 0, "errors": 0},
    }


def _apply_signals(gate, sig: Dict[str, Any], org: dict, raw: dict, scored: dict, verdict: dict,
                   dry_run: bool, big_groups: Optional[List[str]] = None) -> dict:
    """Fetch buying signals for a promising company and re-score it. Returns the (new) verdict."""
    if verdict["score"] < sig["min_score"] or str(scored.get("exclusion_reason") or "").startswith("known_mega_group"):
        return verdict
    if big_groups and _big_group(scored.get("name") or "", big_groups):
        return verdict  # excluded below anyway: do not spend job lookups or research on it
    jobs = sig["jobs_left"] > 0
    research = sig["research_left"] > 0 and verdict["score"] >= sig["research_min_score"]
    if not (jobs or research):
        return verdict
    probe = {**raw, "apollo_org_id": org.get("organization_id") or org.get("id") or raw.get("apollo_org_id")}
    found = gate.collect_signals(probe, fetch_jobs=sig["fetch_jobs"] if jobs else None, research=research)
    if jobs:
        sig["jobs_left"] -= 1
        sig["stats"]["jobs_checked"] += 1
    if found["research_tried"]:
        sig["research_left"] -= 1
        sig["stats"]["researched"] += 1
    if found["errors"]:
        sig["stats"]["errors"] += 1
        log(f"  leadgen signals for {raw.get('domain')}: {'; '.join(found['errors'])}")
    if not (found["hiring"] or found["research"]):
        return verdict
    sig["stats"]["with_signals"] += 1
    new = gate.judge(raw, fortlo_status=scored.get("status"), exclusion_reason=scored.get("exclusion_reason"))
    if dry_run or new["decision"] != verdict["decision"]:
        log(f"  {'[dry-run] ' if dry_run else ''}signals {scored.get('name')} ({raw.get('domain')}): "
            f"{verdict['score']} -> {new['score']} {new['decision']}: "
            + "; ".join(s["summary"][:70] for s in (found["hiring"] + found["research"])[:3]))
    return new


OVERRIDABLE = {"too_big", "too_small", "needs_review"}


def _big_group(name: str, groups: List[str]) -> Optional[str]:
    from leadgen.sourcing import big_group

    return big_group(name, groups)


def _group_key(name: str) -> str:
    from leadgen.sourcing import group_key

    return group_key(name)


def _process_org(
    key: str,
    cfg: dict,
    org: dict,
    tier: Dict[str, Any],
    summary: Dict[str, Any],
    state: Dict[str, Any],
    already_emails: set,
    suppressed: set,
    personal: List[str],
    cap: int,
    dry_run: bool,
) -> bool:
    """Handle one org. Returns True if it consumed Apollo people/enrich calls."""
    raw = _org_to_raw(org)
    domain = raw.get("domain") or ""
    if not domain or domain in suppressed:
        summary["skipped_accounts"] += 1
        return False
    employees_defaulted = False
    if tier.get("screen_orgs"):
        reason = _naics_exclusion(org)
        if not reason:
            blob = (raw.get("name") or "").lower()
            reason = next(
                (f"public_sector:{k}" for k in PUBLIC_SECTOR_NAME_KEYWORDS if k in blob), None
            )
        if reason:
            summary["skipped_accounts"] += 1
            summary.setdefault("excluded", {})[reason.split(":")[0]] = (
                summary.get("excluded", {}).get(reason.split(":")[0], 0) + 1
            )
            if dry_run:
                log(f"  [dry-run] skip {raw.get('name')} ({domain}): {reason}")
            return False
        if raw.get("employees") is None:
            # Apollo already filtered by employee range; keep the account scoreable.
            raw["employees"] = tier["scoring_cfg"]["icp"]["employee_fallback_min"]
            employees_defaulted = True
    attempted = state.setdefault("attempted", {})
    last = attempted.get(domain)
    if last and last >= (date.today() - timedelta(days=tier["retry_days"])).isoformat():
        summary["known_accounts"] += 1
        return False
    if account_has_contacts(raw.get("name") or domain, domain):
        summary["known_accounts"] += 1
        if not dry_run:
            attempted[domain] = date.today().isoformat()
        return False
    if not dry_run:
        attempted[domain] = date.today().isoformat()

    scored = score_account(raw, tier["scoring_cfg"])
    scored["apollo_org_id"] = raw.get("apollo_org_id")
    scored["suggested_titles_json"] = json.dumps(tier["titles"], ensure_ascii=False)
    qualified = scored.get("status") == "qualified"
    gate = tier.get("gate")
    if gate is not None:
        try:
            verdict = gate.judge(raw, fortlo_status=scored.get("status"), exclusion_reason=scored.get("exclusion_reason"))
            if tier.get("signals"):
                verdict = _apply_signals(gate, tier["signals"], org, raw, scored, verdict, dry_run,
                                         tier.get("big_groups") or cfg.get("too_big_groups"))
        except Exception as exc:
            verdict = None
            log(f"  leadgen judge failed for {domain}: {exc}")
        if verdict:
            scored["leadgen_score"] = verdict["score"]
            scored["leadgen_decision"] = verdict["decision"]
            scored["leadgen_reason"] = verdict["reason"]
            scored["leadgen_scored_at"] = verdict["scored_at"]
            if not scored.get("industry") and verdict.get("industry"):
                scored["industry"] = verdict["industry"]
            if tier.get("gate_mode") == "enforce":
                pursue = verdict["decision"] == "pursue"
                mega = str(scored.get("exclusion_reason") or "").startswith("known_mega_group")
                if pursue and not qualified and scored.get("status") in OVERRIDABLE and not mega:
                    scored["exclusion_reason"] = f"leadgen override of {scored.get('status')}"
                    scored["status"] = "qualified"
                    qualified = True
                elif not pursue and qualified:
                    qualified = False
                    summary["leadgen_held"] = summary.get("leadgen_held", 0) + 1
                    if dry_run:
                        log(f"  [dry-run] leadgen hold {scored['name']} ({domain}): {verdict['score']}")
    big = tier.get("big_groups") and _big_group(scored.get("name") or "", tier["big_groups"])
    if big:
        # FORTLO's too_big_groups list stays a hard exclusion for leadgen tiers too.
        qualified = False
        scored["status"] = "too_big"
        scored["exclusion_reason"] = f"known_mega_group:{big}"
    if qualified and tier.get("screen_orgs"):
        rev = scored.get("revenue_eur")
        label = "Leadgen profile match" if tier["name"].startswith("leadgen") else "Small German company"
        scored["score_notes"] = (
            f"{label} (Apollo): revenue {'€%.0fM' % (rev / 1e6) if rev else 'unpublished'}, "
            + (
                f"{tier['scoring_cfg']['icp']['employee_fallback_min']}+ employees (Apollo size filter)"
                if employees_defaulted
                else f"{scored.get('employees')} employees"
            )
        )
        if employees_defaulted:
            scored["employees"] = None
    scored["in_email_campaign"] = 1 if (qualified and tier["auto_campaign"]) else 0
    if gate is not None and tier.get("gate_mode") == "enforce" and scored.get("leadgen_decision") == "hold":
        scored["in_email_campaign"] = 0
    if not dry_run:
        try:
            upsert_customer(scored)
        except Exception as exc:
            summary["errors"].append(f"{domain}: customer upsert {exc}")
            return False
    if not qualified:
        summary["skipped_accounts"] += 1
        if dry_run:
            log(f"  [dry-run] skip {scored['name']} ({domain}): {scored.get('status')} {scored.get('exclusion_reason')}")
        return False
    group_cap = tier.get("max_per_group")
    group = _group_key(scored.get("name") or domain) if group_cap else ""
    groups = state.setdefault("group_contacts", {})
    room = tier["max_per_company"]
    if group_cap:
        room = min(room, group_cap - int(groups.get(group, 0)))
        if room <= 0:
            summary["skipped_accounts"] += 1
            summary["group_capped"] = summary.get("group_capped", 0) + 1
            if dry_run:
                log(f"  [dry-run] skip {scored['name']} ({domain}): group {group} already has {group_cap} contacts")
            return False
    summary["new_accounts"] += 1
    try:
        people = people_search(key, domain, raw.get("apollo_org_id"), tier.get("people_cfg") or cfg, titles=tier["titles"])
    except Exception as exc:
        summary["errors"].append(f"{domain}: people search {exc}")
        return True
    people = [p for p in people if _title_ok(p.get("title"), tier["must"], tier["reject"], tier.get("screen_orgs", False))]
    if dry_run:
        log(
            f"  [dry-run] {scored['name']} ({domain}) {scored.get('score_notes')}: "
            + "; ".join(str(p.get("title")) for p in people[:5])
        )
        summary["dry_run_candidates"] += len(people[:room])
        if group_cap:
            groups[group] = int(groups.get(group, 0)) + len(people[:room])  # dry run: state is not saved
        return True
    ids = [p.get("id") for p in people if p.get("id")]
    enriched_by_id: Dict[str, dict] = {}
    if ids:
        try:
            for match in bulk_match(key, ids[: min(40, tier["max_per_company"] + 2)], cfg):
                if match and match.get("id"):
                    enriched_by_id[str(match["id"])] = match
        except Exception as exc:
            log(f"  bulk_match skipped for {domain}: {exc}")
    stored_here = 0
    for person in people:
        if stored_here >= room or summary["new_contacts"] >= cap:
            break
        pid = str(person.get("id") or "")
        merged = {**person, **(enriched_by_id.get(pid) or {})}
        rec = person_to_contact(0, merged, personal)
        if not rec or not rec.get("email"):
            continue
        if (rec.get("email_status") or "").strip().lower() not in ALLOWED_EMAIL_STATUS:
            continue
        if not _title_ok(rec.get("title"), tier["must"], tier["reject"], tier.get("screen_orgs", False)):
            continue
        email = rec["email"].lower()
        if email in already_emails or is_personal_email(email, personal):
            continue
        rec["account_name"] = scored["name"]
        contact_id, is_new = upsert_contact(domain, rec, return_new=True)
        if not contact_id:
            continue
        already_emails.add(email)
        if is_new:
            summary["new_contacts"] += 1
            stored_here += 1
    if group_cap and stored_here:
        groups[group] = int(groups.get(group, 0)) + stored_here
    if stored_here:
        summary["companies"].append(f"{scored['name']} ({stored_here})")
        log(f"Refuel[{tier['name']}] stored {stored_here} new contacts at {scored['name']}")
    return True


def _build_tiers(cfg: dict) -> List[Dict[str, Any]]:
    refuel_cfg = cfg.get("refuel") or {}
    titles = list(refuel_cfg.get("titles") or ERP_TITLES)
    tiers: List[Dict[str, Any]] = [
        {
            "name": "bechtle_band",
            "scoring_cfg": cfg,
            "titles": titles,
            "must": ["erp", "sap"],
            "reject": [],
            "max_per_company": 100,
            "auto_campaign": True,
            "retry_days": 30,
        }
    ]
    lg_tier = ((cfg.get("leadgen") or {}).get("search_tier") or {})
    if (cfg.get("leadgen") or {}).get("enabled") and lg_tier.get("enabled"):
        lcfg = copy.deepcopy(cfg)
        icp = lcfg["icp"]
        icp["revenue_min_eur"] = int(lg_tier.get("revenue_min_eur", 1_000_000_000))
        icp["revenue_max_eur"] = int(lg_tier.get("revenue_max_eur", 135_000_000_000))
        icp["employee_fallback_min"] = int(lg_tier.get("employee_min", 2000))
        icp["employee_fallback_max"] = int(lg_tier.get("employee_max", 1_000_000))
        icp["qualify_on_employees"] = True
        tiers.append(
            {
                "name": "leadgen_icp",
                "screen_orgs": True,
                "scoring_cfg": lcfg,
                "titles": list(lg_tier.get("titles") or titles),
                "must": list(lg_tier.get("title_must_match") or ["erp", "sap", "mdg", "master data", "stammdaten", "data governance"]),
                "reject": list(lg_tier.get("title_reject") or (cfg.get("refuel_small_de") or {}).get("title_reject") or []),
                "max_per_company": int(lg_tier.get("max_contacts_per_company", 3)),
                "auto_campaign": bool(lg_tier.get("auto_campaign", True)),
                "retry_days": int(lg_tier.get("retry_days", 30)),
                "employee_ranges": list(lg_tier.get("employee_ranges") or [
                    "2001,5000", "5001,10000", "10001,20000", "20001,50000", "50001,100000", "100001,1000000"]),
                "min_eur": icp["revenue_min_eur"],
                "max_eur": icp["revenue_max_eur"],
                "max_orgs_per_run": int(lg_tier.get("max_orgs_per_run", 120)),
                "pages_per_run": int(lg_tier.get("pages_per_run", 2)),
            }
        )
    small = cfg.get("refuel_small_de") or {}
    if small.get("enabled"):
        scfg = copy.deepcopy(cfg)
        icp = scfg["icp"]
        icp["revenue_min_eur"] = int(small.get("revenue_min_eur", 20_000_000))
        icp["revenue_max_eur"] = int(small.get("revenue_max_eur", 5_000_000_000))
        icp["employee_fallback_min"] = int(small.get("employee_min", 200))
        icp["employee_fallback_max"] = int(small.get("employee_max", 5000))
        icp["qualify_on_employees"] = True
        tiers.append(
            {
                "name": "small_de",
                "screen_orgs": True,
                "scoring_cfg": scfg,
                "titles": list(small.get("titles") or titles),
                "must": list(small.get("title_must_match") or ["erp", "sap", "it"]),
                "reject": list(small.get("title_reject") or []),
                "max_per_company": int(small.get("max_contacts_per_company", 3)),
                "auto_campaign": bool(small.get("auto_campaign", False)),
                "retry_days": int(small.get("retry_days", 30)),
                "employee_ranges": list(
                    small.get("employee_ranges") or ["201,500", "501,1000", "1001,2000", "2001,5000"]
                ),
                "max_orgs_per_run": int(small.get("max_orgs_per_run", 120)),
                "pages_per_run": int(small.get("pages_per_run", 2)),
            }
        )
    return tiers


def refuel_head_of_erp(limit_contacts: int = 100, dry_run: Optional[bool] = None) -> Dict[str, Any]:
    if dry_run is None:
        dry_run = os.getenv("REFUEL_DRY_RUN", "").lower() in {"1", "true", "yes"}
    cfg = load_config()
    refuel_cfg = cfg.get("refuel") or {}
    cap = int(refuel_cfg.get("contacts_per_day") or limit_contacts)
    personal = list(cfg.get("personal_mail_domains") or [])
    suppressed = set(cfg.get("suppressed_domains") or []) | BECHTLE_DOMAINS

    summary: Dict[str, Any] = {
        "requested": cap,
        "new_contacts": 0,
        "new_accounts": 0,
        "skipped_accounts": 0,
        "known_accounts": 0,
        "dry_run_candidates": 0,
        "errors": [],
        "companies": [],
        "dry_run": bool(dry_run),
    }
    try:
        key = api_key()
    except SystemExit as exc:
        summary["errors"].append(str(exc))
        log(f"[!] Refuel aborted: {exc}")
        return summary

    state = _load_state()
    already_emails: set = set()
    tiers = _build_tiers(cfg)
    gate = _leadgen_gate(cfg)
    gate_mode = ((cfg.get("leadgen") or {}).get("mode") or "enforce").lower()
    if gate is not None and gate.client is None and gate_mode == "enforce":
        # Apollo leaves industry blank, so without the model no org can reach the pursue score.
        log("[!] Leadgen: no ANTHROPIC_API_KEY, so industries stay blank. Shadow mode this run (FORTLO rules decide).")
        gate_mode = "shadow"
    for tier in tiers:
        tier["gate"] = gate
        tier["gate_mode"] = gate_mode
    summary["leadgen"] = "off" if gate is None else tiers[0]["gate_mode"]
    sig_rt = _signal_runtime(cfg, key) if gate is not None else None
    try:
        list_tiers = _profile_sourcing(cfg, gate, key, tiers)
    except Exception as exc:  # never block refuel on profile sourcing
        list_tiers = []
        for tier in tiers:
            tier.pop("base_params", None)
        log(f"[!] Leadgen profile sourcing unavailable, using the size search: {exc}")
    for tier in tiers + list_tiers:
        tier["signals"] = sig_rt
        tier["state"] = state

    def done() -> bool:
        return summary["new_contacts"] >= cap or (dry_run and summary["dry_run_candidates"] >= cap)

    # Leadgen list tiers: companies picked on the leadgen page, then Claude lookalikes of the wins.
    for tier in list_tiers:
        if done():
            break
        try:
            orgs = _list_tier_orgs(tier, key, cfg, dry_run)
        except Exception as exc:
            summary["errors"].append(f"{tier['name']}: {exc}")
            continue
        _prefetch(tier, orgs)
        handled: List[str] = []
        for org in orgs:
            if done():
                break
            _process_org(key, cfg, org, tier, summary, state, already_emails, suppressed, personal, cap, dry_run)
            handled.append(org.get("primary_domain") or "")
        if tier["name"] == "leadgen_queue" and handled and not dry_run:
            from leadgen import sourcing

            sourcing.queue_done(handled)
        if not dry_run:
            _save_state(state)

    # Tier 1: Bechtle band (small pool, mostly already known).
    t1 = tiers[0]
    try:
        orgs = search_bechtle_band_orgs(key, cfg)
        log(f"Refuel[{t1['name']}]: Apollo returned {len(orgs)} orgs")
    except Exception as exc:
        orgs = []
        summary["errors"].append(f"{t1['name']}: org search {exc}")
    _prefetch(t1, orgs)
    for org in orgs:
        if done():
            break
        _process_org(key, cfg, org, t1, summary, state, already_emails, suppressed, personal, cap, dry_run)
    if not dry_run:
        _save_state(state)

    # Tier 2: small German companies, paged with a persistent cursor.
    for tier in tiers[1:]:
        if done():
            break
        cursor = int((state.setdefault("cursor", {})).get(tier["name"], 1))
        if tier.get("spec_hash"):
            # Profile filters changed (new win, new config): start the paging again.
            specs = state.setdefault("cursor_spec", {})
            if specs.get(tier["name"]) != tier["spec_hash"]:
                cursor = 1
                specs[tier["name"]] = tier["spec_hash"]
        processed = 0
        for _ in range(tier["pages_per_run"]):
            if done() or processed >= tier["max_orgs_per_run"]:
                break
            if cursor > 100:  # Apollo paging ceiling
                cursor = 1
            try:
                orgs = search_orgs_page(
                    key, cfg, cursor, tier["employee_ranges"], tier.get("min_eur"), tier.get("max_eur"),
                    base_params=tier.get("base_params"),
                )
                if not orgs and cursor == 1 and tier.get("base_params_no_tech"):
                    # Apollo's SAP technology filter matched nothing; the leadgen score still checks SAP.
                    log(f"Refuel[{tier['name']}]: no orgs with the SAP technology filter, retrying without it")
                    tier["base_params"] = tier.pop("base_params_no_tech")
                    orgs = search_orgs_page(
                        key, cfg, cursor, tier["employee_ranges"], base_params=tier["base_params"]
                    )
            except Exception as exc:
                summary["errors"].append(f"{tier['name']}: org search page {cursor}: {exc}")
                break
            log(f"Refuel[{tier['name']}]: page {cursor} returned {len(orgs)} orgs")
            if not orgs:
                cursor = 1
                break
            _prefetch(tier, orgs)
            finished_page = True
            for org in orgs:
                if done() or processed >= tier["max_orgs_per_run"]:
                    finished_page = False
                    break
                if _process_org(
                    key, cfg, org, tier, summary, state, already_emails, suppressed, personal, cap, dry_run
                ):
                    processed += 1
            if finished_page:
                cursor += 1
            if not dry_run:
                state["cursor"][tier["name"]] = cursor
                _save_state(state)
        if not dry_run:
            state["cursor"][tier["name"]] = cursor
            _save_state(state)

    if sig_rt:
        summary["signals"] = sig_rt["stats"]
    log(
        f"Refuel done{' (DRY RUN)' if dry_run else ''}: new_contacts={summary['new_contacts']} "
        f"new_accounts={summary['new_accounts']} known={summary['known_accounts']} "
        f"skipped={summary['skipped_accounts']} leadgen_held={summary.get('leadgen_held', 0)} "
        f"errors={len(summary['errors'])}"
    )
    return summary
