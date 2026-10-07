"""Find new companies from the profile ICP (Phase A: sourcing).

Three sources, all ending in the same leadgen score and gate:

  profile_spec(icp)        Apollo company-search filters built from the won customers
                           (DACH, won sectors as keyword tags, SAP, the won size band).
  lookalikes(icp, client)  Claude names real companies that look like the wins. Each one
                           is looked up in Apollo by name and kept only when the domain
                           matches, so invented companies drop out.
  parse_query(text, ...)   a plain-English request turned into the same filters.

The model proposes companies, filters, and titles. It never sets a score or a decision.
Apollo calls are injected (`search_fn`) so this module stays testable offline.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

from leadgen.ai_enrich import state_dir
from leadgen.relationships import normalize

DACH_LOCATIONS = ["Germany", "Austria", "Switzerland"]

# Canonical leadgen sector -> Apollo/LinkedIn industry keyword tags.
SECTOR_TAGS = {
    "manufacturing": [
        "industrial machinery", "machinery", "mechanical or industrial engineering", "automotive",
        "electrical/electronic manufacturing", "industrial automation",
    ],
    "pharma & life sciences": ["pharmaceuticals", "biotechnology", "medical devices", "life sciences"],
    "chemicals & agri": ["chemicals", "agrochemicals", "agriculture"],
    "energy": ["oil & energy", "utilities", "renewables & environment"],
    "logistics": ["logistics & supply chain"],
    "retail & consumer": ["consumer goods", "food production"],
}
# IT services wins (Bechtle) are mostly competitors or channel, not end customers.
SKIP_SECTORS = {"it services", "other"}

APOLLO_EMPLOYEE_BUCKETS = [
    (1, 10), (11, 20), (21, 50), (51, 100), (101, 200), (201, 500), (501, 1000),
    (1001, 2000), (2001, 5000), (5001, 10000), (10001, 20000), (20001, 50000),
    (50001, 100000), (100001, 1000000),
]
DEFAULT_TECHNOLOGIES = ["sap"]
_TECH_UID = re.compile(r"^[a-z0-9_]{2,40}$")


# --------------------------------------------------------------------- filters

def employee_ranges(employee_min: int | None) -> list[str]:
    floor = int(employee_min or 0)
    return [f"{lo},{hi}" for lo, hi in APOLLO_EMPLOYEE_BUCKETS if hi >= floor]


def profile_spec(icp: dict, cfg: dict | None = None) -> dict:
    """Apollo company-search filters from the ICP. `cfg` is config.yaml `leadgen.search_tier`."""
    cfg = cfg or {}
    band = icp.get("size_band") or {}
    skip = {s.lower() for s in (cfg.get("skip_sectors") or SKIP_SECTORS)}
    sectors = [s for s in (icp.get("won_sectors") or {}) if s not in skip]
    tags: list[str] = []
    for sector in sectors:
        for tag in SECTOR_TAGS.get(sector, []):
            if tag not in tags:
                tags.append(tag)
    tags += [t for t in (cfg.get("extra_keyword_tags") or []) if t not in tags]
    rev_min = cfg.get("revenue_min_eur") or band.get("revenue_min_eur") or 1_000_000_000
    rev_max = cfg.get("revenue_max_eur") or band.get("revenue_max_eur") or 135_000_000_000
    emp_min = min(int(cfg.get("employee_min") or 10**9), int(band.get("employees_min") or 10**9))
    if emp_min == 10**9:
        emp_min = 2000
    return {
        "source": "profile",
        "locations": list(cfg.get("locations") or DACH_LOCATIONS),
        "keyword_tags": tags,
        "technologies": list(cfg.get("technologies") if cfg.get("technologies") is not None else DEFAULT_TECHNOLOGIES),
        "revenue_min_eur": int(rev_min),
        "revenue_max_eur": int(rev_max),
        "employee_ranges": employee_ranges(emp_min),
        "titles": [],
        "explain": "Built from the profile: won sectors "
        + (", ".join(sectors) or "none")
        + f", DACH, SAP, revenue EUR {rev_min / 1e9:.1f}bn-{rev_max / 1e9:.0f}bn, {emp_min}+ employees.",
    }


def apollo_params(spec: dict, *, technologies: bool = True) -> list[tuple[str, str]]:
    """Apollo /mixed_companies/search query params for a spec (no paging)."""
    params: list[tuple[str, str]] = [("organization_locations[]", loc) for loc in spec.get("locations") or []]
    if spec.get("revenue_min_eur"):
        params.append(("revenue_range[min]", str(int(spec["revenue_min_eur"]))))
    if spec.get("revenue_max_eur"):
        params.append(("revenue_range[max]", str(int(spec["revenue_max_eur"]))))
    params += [("organization_num_employees_ranges[]", r) for r in spec.get("employee_ranges") or []]
    params += [("q_organization_keyword_tags[]", t) for t in spec.get("keyword_tags") or []]
    if technologies:
        params += [("currently_using_any_of_technology_uids[]", t) for t in spec.get("technologies") or []]
    return params


def spec_hash(spec: dict) -> str:
    keys = ("locations", "keyword_tags", "technologies", "revenue_min_eur", "revenue_max_eur", "employee_ranges")
    blob = json.dumps({k: spec.get(k) for k in keys}, sort_keys=True)
    return hashlib.sha1(blob.encode("utf-8")).hexdigest()[:12]


def _str_list(value, limit: int, max_len: int = 60) -> list[str]:
    out: list[str] = []
    for item in value if isinstance(value, list) else []:
        text = str(item or "").strip()
        if text and len(text) <= max_len and text.lower() not in {o.lower() for o in out}:
            out.append(text)
        if len(out) >= limit:
            break
    return out


def _int_or(value, default):
    try:
        number = int(float(value))
    except (TypeError, ValueError):
        return default
    return number if number >= 0 else default


def clean_spec(raw: dict, defaults: dict) -> dict:
    """Validate a model-proposed spec. Anything missing or malformed falls back to the profile spec."""
    raw = raw if isinstance(raw, dict) else {}
    locations = _str_list(raw.get("locations"), 10, 40) or defaults["locations"]
    tags = _str_list(raw.get("keyword_tags"), 15, 50)
    techs = [t.lower() for t in _str_list(raw.get("technologies"), 5, 40) if _TECH_UID.match(t.lower())]
    rev_min = _int_or(raw.get("revenue_min_eur"), defaults["revenue_min_eur"])
    rev_max = _int_or(raw.get("revenue_max_eur"), defaults["revenue_max_eur"])
    if rev_max and rev_min and rev_max < rev_min:
        rev_min, rev_max = rev_max, rev_min
    emp_min = raw.get("employee_min")
    ranges = employee_ranges(_int_or(emp_min, 0)) if emp_min is not None else defaults["employee_ranges"]
    return {
        "source": "query",
        "locations": locations,
        "keyword_tags": tags,
        "technologies": techs if "technologies" in raw else defaults["technologies"],
        "revenue_min_eur": rev_min,
        "revenue_max_eur": rev_max,
        "employee_ranges": ranges,
        "titles": _str_list(raw.get("titles"), 25),
        "explain": str(raw.get("explain") or "")[:400],
    }


# --------------------------------------------------------------------- model calls

PARSE_SYSTEM = (
    "You turn a plain-English B2B lead request into Apollo.io company-search filters for an "
    "SAP Master Data Governance (MDG) consultancy. The user JSON has `request` and `profile_defaults` "
    "(filters built from the consultant's won customers). Use a default only when the request does not "
    "say otherwise. keyword_tags are LinkedIn-style industry tags in English (e.g. 'pharmaceuticals', "
    "'industrial machinery', 'chemicals'). technologies are Apollo technology uids in lower snake case "
    "(use ['sap'] for SAP users, [] when the request does not need SAP). Revenue is in EUR. "
    "titles are job titles to contact (German and English), optional. "
    "Return one JSON object and no markdown: {\"locations\": [], \"keyword_tags\": [], \"technologies\": [], "
    "\"revenue_min_eur\": int, \"revenue_max_eur\": int, \"employee_min\": int, \"titles\": [], "
    "\"explain\": \"one sentence on how you read the request\"}."
)

LOOKALIKE_SYSTEM = (
    "You suggest prospect companies for an SAP Master Data Governance (MDG) consultant. "
    "The user JSON lists the consultant's won customers, the target sectors, countries and revenue band. "
    "Name real companies you are confident exist that look like the wins: same sectors, revenue inside "
    "`revenue_band_eur`, headquarters or a large business unit in the target countries, known or likely to run "
    "SAP ERP / S/4HANA. Prefer less obvious mid-size groups (Mittelstand leaders, family-owned groups, "
    "hidden champions) over household-name conglomerates. Do not list any company in `exclude`. Do not list "
    "banks, insurers, defence, alcohol, public sector or IT consultancies. domain is the main corporate website domain (no https, no path). "
    "Return one JSON object and no markdown: {\"companies\": [{\"name\", \"domain\", \"country\", "
    "\"sector\", \"why\"}]}. why is at most 15 words and names the win it resembles."
)

TITLES_SYSTEM = (
    "You list job titles used in German, Austrian and Swiss companies for the people who own or decide "
    "on SAP master data, SAP MDG, data governance or the SAP ERP / S/4HANA platform. Include German and "
    "English variants (e.g. 'Leiter Stammdatenmanagement', 'Head of Master Data Management'). Only "
    "director, head or lead level, plus product owner for MDG. The user JSON has `known_titles` to avoid "
    "repeating and `personas`. Return one JSON object and no markdown: {\"titles\": []} with at most 25 titles."
)


def parse_query(text: str, client, defaults: dict) -> dict:
    from leadgen.model_review import _parse_json

    if client is None:
        raise RuntimeError("ANTHROPIC_API_KEY is not set, so a plain-English search cannot be read.")
    payload = {"request": text[:500], "profile_defaults": {k: v for k, v in defaults.items() if k != "explain"}}
    raw = _parse_json(client.complete(payload, system=PARSE_SYSTEM))
    spec = clean_spec(raw, defaults)
    spec["request"] = text[:500]
    return spec


def _json_load(path: Path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return default


def _json_save(path: Path, data) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=1), encoding="utf-8")
    tmp.replace(path)


def lookalike_path() -> Path:
    return state_dir() / "lookalikes.json"


def _clean_domain(value: str) -> str:
    text = (value or "").strip().lower().replace("https://", "").replace("http://", "")
    text = text.split("/")[0]
    return text[4:] if text.startswith("www.") else text


def lookalikes(icp: dict, client, *, want: int = 25, ask: int = 15, exclude: list[str] | None = None,
               countries: list[str] | None = None, revenue_max_eur: float | None = None,
               path: Path | None = None) -> list[dict]:
    """Return up to `want` pool entries with status 'new'. Asks Claude for more when the pool runs low."""
    from leadgen.model_review import _parse_json

    path = path or lookalike_path()
    pool = _json_load(path, {"companies": []})
    rows = pool.get("companies") or []
    for row in rows:  # big groups suggested before the exclusion list existed
        if row.get("status") == "new" and big_group(row["name"], exclude or []):
            row["status"] = "excluded"
    fresh = [row for row in rows if row.get("status") == "new"]
    if len(fresh) < want and client is not None:
        known = [row["name"] for row in rows] + [w["company"] for w in icp.get("wins") or []] + list(exclude or [])
        sectors = [s for s in (icp.get("won_sectors") or {}) if s not in SKIP_SECTORS]
        band = icp.get("size_band") or {}
        payload = {
            "wins": [
                {"company": w["company"], "sector": next((s for s, names in (icp.get("won_sectors") or {}).items()
                                                          if w["company"] in names), "")}
                for w in icp.get("wins") or []
            ],
            "target_sectors": sectors,
            "countries": countries or DACH_LOCATIONS,
            "revenue_band_eur": [band.get("revenue_min_eur"), revenue_max_eur or band.get("revenue_max_eur")],
            "how_many": ask,
            "exclude": known[:400],
        }
        try:
            raw = _parse_json(client.complete(payload, system=LOOKALIKE_SYSTEM))
        except Exception as exc:  # keep the pool we have
            pool["last_error"] = f"{exc.__class__.__name__}: {str(exc)[:160]}"
            _json_save(path, pool)
            return fresh[:want]
        seen = {normalize(row["name"]) for row in rows} | {_clean_domain(row.get("domain", "")) for row in rows}
        seen |= {normalize(name) for name in known}
        stamp = date.today().isoformat()
        for item in raw.get("companies") or []:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "").strip()[:120]
            domain = _clean_domain(str(item.get("domain") or ""))
            if not name or normalize(name) in seen or (domain and domain in seen):
                continue
            if big_group(name, exclude or []):
                continue
            seen |= {normalize(name), domain}
            row = {
                "name": name,
                "domain": domain,
                "country": str(item.get("country") or "")[:40],
                "sector": str(item.get("sector") or "")[:40],
                "why": str(item.get("why") or "")[:200],
                "status": "new",
                "suggested_at": stamp,
                "model": getattr(client, "model", ""),
            }
            rows.append(row)
            fresh.append(row)
        pool["companies"] = rows
        pool.pop("last_error", None)
        _json_save(path, pool)
    return fresh[:want]


def mark_lookalikes(updates: dict[str, str], path: Path | None = None) -> None:
    """Set status per lookalike name: resolved / not_found / processed."""
    path = path or lookalike_path()
    pool = _json_load(path, {"companies": []})
    for row in pool.get("companies") or []:
        if row["name"] in updates:
            row["status"] = updates[row["name"]]
            row["checked_at"] = date.today().isoformat()
    _json_save(path, pool)


def resolve_in_apollo(entry: dict, search_fn: Callable[[list], list]) -> dict | None:
    """Find the Apollo org for a model-suggested company. Kept only on a domain or exact-name match."""
    orgs = search_fn([("q_organization_name", entry["name"]), ("per_page", "5"), ("page", "1")]) or []
    want_domain = _clean_domain(entry.get("domain") or "")
    want_name = normalize(entry["name"])
    for org in orgs:
        if want_domain and _clean_domain(org.get("primary_domain") or org.get("website_url") or "") == want_domain:
            return org
    for org in orgs:
        if want_name and normalize(org.get("name") or "") == want_name:
            return org
    return None


def title_path() -> Path:
    return state_dir() / "title_variants.json"


def title_variants(client, known: list[str], personas: list[str] | None = None, *, max_age_days: int = 30,
                   path: Path | None = None) -> list[str]:
    """Extra contact titles from Claude, cached for `max_age_days`. Empty without a client."""
    from leadgen.model_review import _parse_json

    path = path or title_path()
    cached = _json_load(path, {})
    fresh_until = cached.get("at") and date.fromisoformat(cached["at"]) + timedelta(days=max_age_days)
    if cached.get("titles") and fresh_until and fresh_until >= date.today():
        return list(cached["titles"])
    if client is None:
        return list(cached.get("titles") or [])
    try:
        raw = _parse_json(client.complete({"known_titles": known, "personas": personas or []}, system=TITLES_SYSTEM))
    except Exception:
        return list(cached.get("titles") or [])
    known_lower = {k.lower() for k in known}
    titles = [t for t in _str_list(raw.get("titles"), 25) if t.lower() not in known_lower]
    _json_save(path, {"titles": titles, "at": date.today().isoformat(), "model": getattr(client, "model", "")})
    return titles


GROUP_NOISE = {
    "group", "gruppe", "groupe", "ag", "se", "gmbh", "kgaa", "co", "kg", "holding", "international",
    "inc", "ltd", "plc", "sa", "nv", "the", "und", "and", "company", "germany", "deutschland", "darmstadt",
}


def _words(name: str) -> list[str]:
    return re.findall(r"[a-z0-9äöüß]+", (name or "").lower().replace("&", " "))


def big_group(name: str, groups: list[str]) -> str | None:
    """The `too_big_groups` entry this company belongs to (whole words, so 'Bayer' is not 'Bayerische')."""
    words = _words(name)
    for group in groups:
        needle = _words(group)
        if needle and any(words[i:i + len(needle)] == needle for i in range(len(words))):
            return group
    return None


def group_key(name: str) -> str:
    """Rough corporate-group key so 'Merck KGaA', 'Merck Group' and 'Merck Life Science' count as one."""
    words = [w for w in _words(name) if w not in GROUP_NOISE]
    if not words:
        return normalize(name)
    return words[0] if len(words[0]) >= 4 else "".join(words[:2])


def merge_titles(base: list[str], extra: list[str], limit: int = 40) -> list[str]:
    out: list[str] = []
    for title in list(base) + list(extra):
        if title and title.lower() not in {o.lower() for o in out}:
            out.append(title)
    return out[:limit]


# --------------------------------------------------------------------- queue (picked on the page)

def queue_path() -> Path:
    return state_dir() / "queue.json"


QUEUE_ORG_FIELDS = (
    "id", "name", "primary_domain", "website_url", "linkedin_url", "city", "country",
    "annual_revenue", "organization_revenue", "estimated_num_employees", "industry",
)


def queue_add(orgs: list[dict], source: str, path: Path | None = None) -> int:
    path = path or queue_path()
    data = _json_load(path, {"pending": [], "done": []})
    have = {_clean_domain(o.get("primary_domain") or "") for o in data["pending"]}
    added = 0
    for org in orgs:
        domain = _clean_domain(org.get("primary_domain") or org.get("website_url") or "")
        if not domain or domain in have:
            continue
        have.add(domain)
        row = {k: org.get(k) for k in QUEUE_ORG_FIELDS if org.get(k) is not None}
        row["primary_domain"] = domain
        row["_source"] = source[:200]
        row["_added_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
        data["pending"].append(row)
        added += 1
    _json_save(path, data)
    return added


def queue_pending(path: Path | None = None) -> list[dict]:
    return list(_json_load(path or queue_path(), {"pending": []}).get("pending") or [])


def queue_done(domains: list[str], path: Path | None = None) -> None:
    path = path or queue_path()
    data = _json_load(path, {"pending": [], "done": []})
    gone = {_clean_domain(d) for d in domains}
    keep, done = [], data.get("done") or []
    for row in data.get("pending") or []:
        (done if _clean_domain(row.get("primary_domain") or "") in gone else keep).append(row)
    data["pending"], data["done"] = keep, done[-500:]
    _json_save(path, data)


# --------------------------------------------------------------------- plain-English search (leadgen page)

def plain_search(text: str, gate, search_fn: Callable[[list], list], *, per_page: int = 25,
                 st_cfg: dict | None = None, known_fn: Callable[[str, str], bool] | None = None,
                 big_groups: list[str] | None = None) -> dict:
    """Read the request with Claude, run one Apollo page, score every company with the gate.

    No people search and no contact credits: picking companies only queues them for the next refuel.
    """
    defaults = profile_spec(gate.icp(), st_cfg)
    spec = parse_query(text, gate.client, defaults)
    params = apollo_params(spec) + [("per_page", str(per_page)), ("page", "1")]
    orgs = search_fn(params) or []
    note = ""
    if not orgs and spec.get("technologies"):
        orgs = search_fn(apollo_params(spec, technologies=False) + [("per_page", str(per_page)), ("page", "1")]) or []
        note = "No company matched Apollo's SAP technology filter, so it was dropped; the score still checks SAP."
    raws = [org_to_raw(org) for org in orgs]
    gate.prefetch(raws)
    rows = []
    for org, raw in zip(orgs, raws):
        verdict = gate.judge(raw)
        rows.append({
            "org": {k: org.get(k) for k in QUEUE_ORG_FIELDS if org.get(k) is not None},
            "name": raw["name"],
            "domain": raw["domain"],
            "country": raw.get("country") or "",
            "revenue_eur": raw.get("revenue_eur"),
            "employees": raw.get("employees"),
            "industry": verdict.get("industry") or raw.get("industry") or "",
            "ai": verdict.get("ai_inferred"),
            "score": verdict["score"],
            "decision": verdict["decision"],
            "reason": verdict["reason"],
            "known": bool(known_fn and raw["domain"] and known_fn(raw["name"], raw["domain"])),
            "big_group": big_group(raw["name"], big_groups or []),
        })
    rows.sort(key=lambda r: -r["score"])
    return {"request": text, "spec": spec, "rows": rows, "note": note}


def org_to_raw(org: dict) -> dict:
    """Apollo org -> the raw shape the gate scores (same fields as FORTLO's refuel)."""
    def num(value, cast):
        try:
            return cast(value) if value not in (None, "") else None
        except (TypeError, ValueError):
            return None

    domain = _clean_domain(org.get("primary_domain") or org.get("website_url") or org.get("domain") or "")
    return {
        "name": (org.get("name") or "").strip(),
        "domain": domain,
        "country": org.get("country") or "",
        "revenue_eur": num(org.get("annual_revenue") or org.get("organization_revenue"), float),
        "employees": num(org.get("estimated_num_employees"), int),
        "industry": org.get("industry") or "",
    }
