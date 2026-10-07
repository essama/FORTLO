"""Buying signals (Phase B): is this company likely buying SAP MDG / S/4 help right now?

Two sources, both cached per domain in LEADGEN_STATE_DIR/signals.json:

  hiring    Apollo job postings. An open "SAP MDG" / "Stammdaten" / "Master Data" role is the
            strongest public sign of a running program. Cheap; checked for every scored candidate.
  research  Claude with Claude web search: S/4 or MDG programs, ERP consolidation, M&A, a new CIO.
            About USD 0.15 per company, so only for the best candidates and a few per run.

Every signal carries a source URL. Research signals are kept only when the URL is one the
web search actually cited, so the model cannot invent evidence. Signals add points in
score.py; the model never sets the score.
"""

from __future__ import annotations

import json
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

from leadgen.ai_enrich import state_dir

STRONG_TITLE = re.compile(
    r"(?<![a-z0-9])(mdg|mdm|master[ -]?data|stammdaten\w*|data governance|daten-?governance)(?![a-z0-9])"
)
MDG_TITLE = re.compile(r"(?<![a-z0-9])(mdg|master data governance)(?![a-z0-9])")
# Operational data-entry roles: a weak signal, and only in DACH
CLERICAL_TITLE = re.compile(
    r"(?<![a-z0-9])(specialist|spezialist\w*|analyst|administrator|steward|associate|coordinator|clerk|assistant|"
    r"sachbearbeiter\w*|mitarbeiter\w*|pflege|data entry|junior|agent|support)(?![a-z0-9])"
)
LEAD_TITLE = re.compile(
    r"(?<![a-z0-9])(head|lead|leiter\w*|director|direktor\w*|vp|chief|architect|architekt\w*|owner)(?![a-z0-9])"
)
SAP_TITLE = re.compile(r"(?<![a-z0-9])(s/?4\s?hana|s/4|s4|sap)(?![a-z0-9])")
JUNK_TITLE = re.compile(
    r"(?<![a-z0-9])(ausbildung|apprentice|azubi|praktikum|intern(ship)?|werkstudent|student|trainee)(?![a-z0-9])"
)
# SAP roles that say nothing about master data or the ERP core
SAP_NOISE = re.compile(
    r"(?<![a-z0-9])(hcm|hr|successfactors|security|authori[sz]ation\w*|berechtigung\w*|basis|ariba|concur|"
    r"fieldglass|payroll)(?![a-z0-9])"
)
DACH_COUNTRIES = {"germany", "deutschland", "austria", "österreich", "switzerland", "schweiz", "suisse"}
HIRING_VERSION = 4
RESEARCH_RETRY_DAYS = 3

POINTS = {
    "hiring_mdg": 15,
    "hiring_md_ops": 5,
    "mdg_program": 15,
    "s4_program": 10,
    "hiring_sap": 8,
    "erp_consolidation": 8,
    "m_and_a": 5,
    "leadership_change": 5,
}
LABELS = {
    "hiring_mdg": "hiring for master data / MDG",
    "hiring_md_ops": "hiring master-data staff",
    "hiring_sap": "hiring for SAP / S/4",
    "mdg_program": "SAP MDG program",
    "s4_program": "S/4HANA program",
    "erp_consolidation": "ERP consolidation",
    "m_and_a": "merger or carve-out",
    "leadership_change": "new IT / data leadership",
}
SAP_TYPES = {"hiring_mdg", "hiring_sap", "mdg_program", "s4_program"}
MAX_POINTS = 15  # signals rank companies; they should not flatten everyone to 100
HIRING_DAYS = 7
RESEARCH_DAYS = 30

RESEARCH_SYSTEM = (
    "You research one company for an SAP Master Data Governance (MDG) consultant. Use web search. "
    "Find recent public evidence (last 24 months) of: an SAP S/4HANA program (s4_program), an SAP MDG or "
    "master-data program (mdg_program), ERP consolidation or harmonisation (erp_consolidation), a merger, "
    "acquisition or carve-out (m_and_a), a new CIO / CDO / head of data (leadership_change). "
    "Only report what a cited source says. Every signal needs the exact URL you read. "
    "Your whole answer must be one JSON object, with no text before or after it and no markdown: {\"uses_sap\": \"yes|likely|unknown\", \"signals\": "
    "[{\"type\", \"summary\", \"date\", \"source_url\"}], \"summary\": \"one sentence\"}. "
    "summary of a signal is at most 25 words; date is YYYY-MM or empty. Return an empty signals list when "
    "nothing is found."
)


# --------------------------------------------------------------------- store

def store_path() -> Path:
    return state_dir() / "signals.json"


def _load(path: Path | None = None) -> dict:
    path = path or store_path()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def _save(data: dict, path: Path | None = None) -> None:
    path = path or store_path()
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=1), encoding="utf-8")
    tmp.replace(path)


def _fresh(stamp: str | None, days: int) -> bool:
    if not stamp:
        return False
    try:
        return date.fromisoformat(stamp[:10]) + timedelta(days=days) >= date.today()
    except ValueError:
        return False


def cached(domain: str, path: Path | None = None) -> list[dict]:
    """All stored signals for a domain (hiring + research), whatever their age."""
    row = _load(path).get((domain or "").lower()) or {}
    hiring_rows = row.get("hiring") if row.get("hiring_v") == HIRING_VERSION else []
    return list(hiring_rows or []) + list(row.get("research") or [])


# --------------------------------------------------------------------- hiring (Apollo job postings)

def hiring_from_postings(postings: list[dict], max_age_days: int = 120) -> list[dict]:
    out, seen = [], set()
    cutoff = date.today() - timedelta(days=max_age_days)
    for post in postings or []:
        title = str(post.get("title") or "").strip()
        low = title.lower()
        if not title or JUNK_TITLE.search(low):
            continue
        country = str(post.get("country") or "").strip()
        in_dach = country.lower() in DACH_COUNTRIES
        senior = not CLERICAL_TITLE.search(low) and (in_dach or LEAD_TITLE.search(low))
        if STRONG_TITLE.search(low) and (senior or (MDG_TITLE.search(low) and in_dach)):
            # master-data role at lead level anywhere (global CoEs are run from HQ), or any non-clerical one in DACH
            kind = "hiring_mdg"
        elif STRONG_TITLE.search(low) and in_dach:
            kind = "hiring_md_ops"
        elif SAP_TITLE.search(low) and not SAP_NOISE.search(low) and in_dach:
            kind = "hiring_sap"  # a generic SAP role only counts in DACH
        else:
            continue
        posted = str(post.get("posted_at") or "")[:10]
        try:
            if posted and date.fromisoformat(posted) < cutoff:
                continue
        except ValueError:
            pass
        key = (kind, low)
        if key in seen:
            continue
        seen.add(key)
        city = ", ".join(x for x in (post.get("city"), country) if x)
        out.append({
            "type": kind,
            "summary": f"Open role: {title[:120]}" + (f" ({city})" if city else ""),
            "date": posted,
            "source_url": str(post.get("url") or "")[:400],
            "source": "apollo_jobs",
        })
    # strongest first, at most a handful
    out.sort(key=lambda s: s["date"] or "", reverse=True)
    out.sort(key=lambda s: s["type"] != "hiring_mdg")
    return out[:6]


def hiring(domain: str, org_id: str | None, fetch_fn: Callable[[str], list], *, path: Path | None = None) -> list[dict]:
    """Job-posting signals, cached HIRING_DAYS. fetch_fn(org_id) returns Apollo job postings."""
    domain = (domain or "").lower()
    if not domain or not org_id:
        return []
    data = _load(path)
    row = data.setdefault(domain, {})
    if row.get("hiring_v") == HIRING_VERSION and _fresh(row.get("hiring_at"), HIRING_DAYS):
        return list(row.get("hiring") or [])
    postings = fetch_fn(org_id)  # errors propagate: the caller decides whether to keep going
    row["hiring"] = hiring_from_postings(postings)
    row["hiring_at"] = date.today().isoformat()
    row["hiring_v"] = HIRING_VERSION
    row["postings_seen"] = len(postings or [])
    _save(data, path)
    return list(row["hiring"])


# --------------------------------------------------------------------- research (Claude + web search)

def _clean_research(raw: dict, cited: set[str]) -> list[dict]:
    out = []
    for item in raw.get("signals") or []:
        if not isinstance(item, dict):
            continue
        kind = str(item.get("type") or "").strip().lower()
        url = str(item.get("source_url") or "").strip()
        if kind not in POINTS or kind.startswith("hiring") or not url.startswith("http"):
            continue
        if cited and url.rstrip("/") not in cited:
            continue  # not a page the search actually returned
        out.append({
            "type": kind,
            "summary": str(item.get("summary") or "")[:220],
            "date": str(item.get("date") or "")[:10],
            "source_url": url[:400],
            "source": "claude_web",
        })
    return out[:6]


def research(name: str, domain: str, client, *, path: Path | None = None) -> dict:
    """Web research for one company, cached RESEARCH_DAYS. Returns {'signals', 'uses_sap', 'summary', 'cached'}."""
    domain = (domain or "").lower()
    data = _load(path)
    row = data.setdefault(domain, {})
    if _fresh(row.get("research_at"), RESEARCH_DAYS):
        return {"signals": list(row.get("research") or []), "uses_sap": row.get("uses_sap", "unknown"),
                "summary": row.get("research_summary", ""), "cached": True}
    if _fresh(row.get("research_failed_at"), RESEARCH_RETRY_DAYS):
        return {"signals": [], "uses_sap": "unknown", "summary": "", "cached": True}
    if client is None or not hasattr(client, "web_search"):
        return {"signals": [], "uses_sap": "unknown", "summary": "", "cached": True}
    try:
        text, cited = client.web_search({"company": name, "domain": domain}, system=RESEARCH_SYSTEM)
        raw = _json_object(_strip_citations(text))
    except Exception:
        row["research_failed_at"] = date.today().isoformat()
        _save(data, path)
        raise
    found = _clean_research(raw, {u.rstrip("/") for u in cited})
    uses_sap = str(raw.get("uses_sap") or "unknown").lower()
    row.update({
        "research": found,
        "research_at": date.today().isoformat(),
        "uses_sap": uses_sap if uses_sap in {"yes", "likely", "unknown"} else "unknown",
        "research_summary": str(raw.get("summary") or "")[:300],
        "research_model": getattr(client, "model", ""),
    })
    _save(data, path)
    return {"signals": found, "uses_sap": row["uses_sap"], "summary": row["research_summary"], "cached": False}


def _json_object(text: str) -> dict:
    """The JSON object in a model answer, also when it is wrapped in prose or a code fence."""
    from leadgen.model_review import _parse_json

    try:
        return _parse_json(text)
    except (ValueError, json.JSONDecodeError):
        pass
    start, end = text.find("{"), text.rfind("}")
    if start == -1 or end <= start:
        raise ValueError("no JSON object in the research answer")
    parsed = json.loads(text[start:end + 1])
    if not isinstance(parsed, dict):
        raise ValueError("research answer was not a JSON object")
    return parsed


def _strip_citations(text: str) -> str:
    """Claude appends [[1]](url) markers inside JSON strings; drop them before parsing."""
    return re.sub(r"\[\[\d+\]\]\([^)]*\)", "", text or "")


# --------------------------------------------------------------------- scoring

def points(signals: list[dict]) -> tuple[int, list[str], bool]:
    """(points, reasons, sap_documented) for score.py. One count per signal type."""
    best: dict[str, dict] = {}
    for sig in signals or []:
        kind = sig.get("type")
        if kind in POINTS and kind not in best:
            best[kind] = sig
    total = min(MAX_POINTS, sum(POINTS[k] for k in best))
    reasons = []
    for kind, sig in sorted(best.items(), key=lambda kv: -POINTS[kv[0]]):
        when = f", {sig['date']}" if sig.get("date") else ""
        reasons.append(f"signal: {LABELS[kind]} ({sig.get('summary', '')[:90]}{when}; {sig.get('source_url', '')[:120]})")
    return total, reasons, any(k in SAP_TYPES for k in best)


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
