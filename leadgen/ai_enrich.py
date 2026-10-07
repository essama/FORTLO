"""Fill blank account firmographics with Claude. Results are cached and always tagged AI-inferred.

Apollo's company search leaves `industry` empty on most FORTLO accounts, so the
sector part of the score cannot match without this. The model only classifies;
it never sets a score. Unknown stays unknown.
"""

from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timezone
from pathlib import Path

from leadgen.relationships import normalize

SECTORS = (
    "manufacturing",
    "pharma & life sciences",
    "chemicals & agri",
    "energy",
    "it services",
    "logistics",
    "retail & consumer",
    "financial services",
    "public sector & education",
    "defence",
    "alcohol",
    "other",
)

SYSTEM = (
    "You classify companies for a B2B SAP master-data consultancy. "
    "For each company in the user JSON, return what you know with confidence. "
    "Use 'unknown' when you are not sure. Do not guess revenue. "
    "sector must be one of: " + ", ".join(SECTORS) + ". "
    "uses_sap is 'yes' only when the company is publicly known to run SAP ERP or S/4HANA, "
    "'likely' for large manufacturers or pharma groups where SAP is the norm, otherwise 'unknown'. "
    "Return one JSON object and no markdown: {\"companies\": [{\"key\", \"industry\", \"sector\", "
    "\"uses_sap\", \"hq_country\", \"note\"}]}. industry is a short label (max 5 words). "
    "note is at most 15 words."
)

BATCH = 20  # Claude can take over 2 minutes on 40 companies
_LOCK = threading.Lock()


def state_dir() -> Path:
    path = Path(os.environ.get("LEADGEN_STATE_DIR") or Path(__file__).resolve().parent / "data")
    path.mkdir(parents=True, exist_ok=True)
    return path


def cache_path() -> Path:
    return state_dir() / "ai_enrichment.json"


def account_key(account: dict) -> str:
    domain = (account.get("domain") or "").strip().lower()
    return domain or normalize(account.get("name") or "")


def load_cache(path: Path | None = None) -> dict:
    path = path or cache_path()
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def save_cache(cache: dict, path: Path | None = None) -> None:
    path = path or cache_path()
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(cache, ensure_ascii=False, indent=1), encoding="utf-8")
    tmp.replace(path)


def needs_enrichment(account: dict) -> bool:
    return not (account.get("industry") or "").strip()


def _clean(row: dict) -> dict:
    sector = (row.get("sector") or "unknown").strip().lower()
    if sector not in SECTORS:
        sector = "unknown"
    uses_sap = (row.get("uses_sap") or "unknown").strip().lower()
    if uses_sap not in {"yes", "likely", "unknown", "no"}:
        uses_sap = "unknown"
    return {
        "industry": str(row.get("industry") or "")[:80],
        "sector": sector,
        "uses_sap": uses_sap,
        "hq_country": str(row.get("hq_country") or "")[:60],
        "note": str(row.get("note") or "")[:200],
    }


def enrich(accounts: list[dict], client, limit: int = 200, cache: dict | None = None) -> dict:
    """Classify up to `limit` uncached accounts that have no industry. Returns a summary."""
    from leadgen.model_review import _parse_json

    own_cache = cache is None
    with _LOCK:
        cache = load_cache() if own_cache else cache
        todo = []
        seen = set()
        for account in accounts:
            key = account_key(account)
            if not key or key in cache or key in seen or not needs_enrichment(account):
                continue
            seen.add(key)
            todo.append({"key": key, "name": account.get("name") or "", "domain": account.get("domain") or ""})
            if len(todo) >= limit:
                break

        summary = {"requested": len(todo), "classified": 0, "errors": []}
        if client is None or not todo:
            return summary
        model = getattr(client, "model", "")
        for start in range(0, len(todo), BATCH):
            batch = todo[start : start + BATCH]
            try:
                raw = client.complete({"companies": batch}, system=SYSTEM)
                rows = _parse_json(raw).get("companies") or []
            except Exception as exc:  # network, HTTP, or bad JSON: keep what we have
                summary["errors"].append(f"{exc.__class__.__name__}: {str(exc)[:120]}")
                continue
            wanted = {item["key"] for item in batch}
            stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            for row in rows:
                if not isinstance(row, dict) or row.get("key") not in wanted:
                    continue
                cache[row["key"]] = {**_clean(row), "model": model, "at": stamp}
                summary["classified"] += 1
            if own_cache:
                save_cache(cache)
        return summary


def apply(accounts: list[dict], cache: dict | None = None) -> list[dict]:
    """Return copies of the accounts with AI fields attached where the CRM field is blank."""
    cache = load_cache() if cache is None else cache
    out = []
    for account in accounts:
        row = dict(account)
        hit = cache.get(account_key(account))
        if hit and needs_enrichment(account):
            row["ai"] = hit
        out.append(row)
    return out
