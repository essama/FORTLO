"""One pass: CRM clients, profile end customers, ICP, personas, ranked leads."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

from leadgen import ai_enrich
from leadgen.adapter import SnapshotCrmAdapter, SqliteCrmAdapter
from leadgen.historical import load_historical_contacts
from leadgen.icp import build_icp
from leadgen.model_review import apply_review, client_from_env
from leadgen.personas import build_personas
from leadgen.profile_source import load_profile
from leadgen.relationships import build_relationships, cv_client_check
from leadgen.score import PROBE_BANK, PROBE_MATCH, PURSUE_AT, rank_accounts

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
# Override with LEADGEN_PROFILE / LEADGEN_CV (the EC2 container sets both).
DEFAULT_PROFILE = Path(r"C:\Users\essam\Desktop\Essam Azzam\Essam\CVs\Profil.docx")
DEFAULT_CV = Path(r"C:\Users\essam\Downloads\Essam_Azzam_CV.docx")


def _cv_text(path: Path) -> str:
    if not path.exists():
        return ""
    from leadgen.profile_source import paragraphs_from_docx

    return "\n".join(paragraphs_from_docx(path))


def _adapter(invoice_db: Path | None, account_db: Path | None, crm_snapshot: Path, accounts_snapshot: Path):
    if invoice_db and invoice_db.exists():
        return SqliteCrmAdapter(invoice_db, account_db if account_db and account_db.exists() else None)
    return SnapshotCrmAdapter(crm_snapshot, accounts_snapshot if accounts_snapshot.exists() else None)


def run(
    profile_path: Path | None = None,
    cv_path: Path | None = None,
    invoice_db: Path | None = None,
    account_db: Path | None = None,
    crm_snapshot: Path | None = None,
    accounts_snapshot: Path | None = None,
    lead_limit: int = 25,
    use_model: bool | None = None,
    model_client=None,
    enrich_limit: int | None = None,
) -> dict:
    profile_path = Path(profile_path or os.environ.get("LEADGEN_PROFILE") or DEFAULT_PROFILE)
    cv_path = Path(cv_path or os.environ.get("LEADGEN_CV") or DEFAULT_CV)
    invoice_db = Path(invoice_db) if invoice_db else (
        Path(os.environ["LEADGEN_INVOICE_DB"]) if os.environ.get("LEADGEN_INVOICE_DB") else None
    )
    account_db = Path(account_db) if account_db else (
        Path(os.environ["LEADGEN_FORTLO_DB"]) if os.environ.get("LEADGEN_FORTLO_DB") else None
    )
    crm_snapshot = Path(crm_snapshot or DATA / "crm_snapshot.json")
    accounts_snapshot = Path(accounts_snapshot or DATA / "accounts_snapshot.json")

    gaps = []
    if not profile_path.exists():
        profile = {"summary": "", "engagements": [], "excluded": [], "path": str(profile_path), "paragraph_count": 0}
        gaps.append(f"Profile file is missing: {profile_path}")
    else:
        profile = load_profile(profile_path)

    try:
        adapter = _adapter(invoice_db, account_db, crm_snapshot, accounts_snapshot)
        companies = adapter.recruiting_companies()
        invoices = adapter.invoices()
        accounts = adapter.sourced_accounts()
    except FileNotFoundError as exc:
        companies, invoices, accounts = [], [], []
        gaps.append(str(exc))

    if not companies:
        gaps.append("No recruiting companies were loaded from the CRM adapter.")
    if not invoices:
        gaps.append("No invoices were loaded. Deal size will stay blank.")
    if not accounts:
        gaps.append("No sourced accounts were loaded. The ranked list contains the probe accounts only.")
    if not profile["engagements"]:
        gaps.append("No end-customer engagements were parsed from the profile. The ICP is partial.")

    historical_path = DATA / "historical_contacts.json"
    historical_contacts = load_historical_contacts(historical_path)
    if not historical_path.exists():
        gaps.append(f"Historical client contacts file is missing: {historical_path}")

    if use_model is not False and model_client is None:
        model_client = client_from_env()
    if use_model is False:
        model_client = None

    relationships = build_relationships(profile["engagements"], companies, invoices)
    icp = build_icp(
        profile["engagements"],
        profile.get("summary") or "",
        relationships,
        companies=companies,
        invoices=invoices,
    )
    personas = build_personas(profile["engagements"], relationships, historical_contacts, icp["channel"])

    # AI fills blank industries (cached). Bounded per run so a page refresh stays fast.
    enrichment = {"requested": 0, "classified": 0, "errors": []}
    if model_client is not None and accounts:
        limit = enrich_limit if enrich_limit is not None else int(os.environ.get("LEADGEN_ENRICH_PER_RUN") or 80)
        enrichment = ai_enrich.enrich(accounts, model_client, limit=limit)
        for err in enrichment["errors"][:3]:
            gaps.append(f"AI industry classification error: {err}")
    accounts = ai_enrich.apply(accounts)
    blank = sum(1 for a in accounts if ai_enrich.needs_enrichment(a) and not a.get("ai"))
    if blank:
        gaps.append(
            f"{blank} accounts have no industry and no AI classification yet. "
            + ("They are classified a batch per refresh." if model_client else "Set ANTHROPIC_API_KEY to classify them.")
        )
    cv_text = _cv_text(cv_path)
    coverage = cv_client_check(profile["engagements"], cv_text)
    coverage["cv_path"] = str(cv_path)
    if not cv_text:
        coverage["note"] = f"CV file was not readable: {cv_path}"
        gaps.append(coverage["note"])
    elif coverage["missing_on_cv"]:
        gaps.append("Clients named on the profile are missing from the CV: " + ", ".join(coverage["missing_on_cv"]))

    probes = [
        rank_accounts([PROBE_BANK], icp, companies)[0],
        rank_accounts([PROBE_MATCH], icp, companies)[0],
    ]
    ranked = rank_accounts(accounts, icp, companies)
    low = [row for row in ranked if row["score"] < 40][:8]
    pursue = [row for row in ranked if row["decision"] == "pursue"]

    result = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "sources": {
            "profile": str(profile_path),
            "cv": str(cv_path),
            "crm": str(invoice_db) if invoice_db else str(crm_snapshot),
            "accounts": str(account_db) if account_db else str(accounts_snapshot),
            "historical_contacts": str(historical_path),
            "crm_write": "none",
        },
        "gaps": gaps,
        "historical_contacts": historical_contacts,
        "relationships": relationships,
        "cv_check": coverage,
        "icp": icp,
        "personas": personas,
        "leads": ranked[:lead_limit],
        "lead_count": len(ranked),
        "pursue_count": len(pursue),
        "recruiting_companies": [row["company_name"] for row in companies if row.get("company_name")],
        "pursue_at": PURSUE_AT,
        "score_spread": _spread(ranked),
        "enrichment": enrichment,
        "low_scores": low,
        "probes": probes,
        "excluded_early_career": [
            {"company": item["company"], "reason": item.get("retention_basis")}
            for item in profile.get("excluded") or []
        ],
    }
    if use_model is False:
        return apply_review(result, None, required=False)
    return apply_review(result, model_client)


def _spread(ranked: list[dict]) -> list[dict]:
    buckets = [(80, 101), (60, 80), (40, 60), (20, 40), (0, 20)]
    return [
        {"band": f"{low}-{min(high, 100)}", "count": sum(1 for row in ranked if low <= row["score"] < high)}
        for low, high in buckets
    ]
