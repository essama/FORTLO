#!/usr/bin/env python3
"""Stage 1 — public-web German account list for FORTE4.

No Apollo. Scores curated public-source candidates against the ICP,
upserts into the FORTLO database, writes SOURCES.md and accounts.csv.

Usage:
  python build_accounts.py
  python build_accounts.py --db outreach_log.sqlite
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from collections import Counter
from typing import Any, Dict, List, Optional, Tuple

import yaml

from db import Database, normalize_domain, open_db
from exclusions import (
    alcohol_hit,
    bechtle_hit,
    defense_hit,
    finance_hit,
    too_big_group_hit,
)

HERE = os.path.dirname(os.path.abspath(__file__))
STATUS_ORDER = ("qualified", "needs_review", "too_small", "too_big", "excluded_sector")


def load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def load_config(path: Optional[str] = None) -> dict:
    return load_yaml(path or os.path.join(HERE, "config.yaml"))


def load_candidates(path: Optional[str] = None) -> List[dict]:
    data = load_yaml(path or os.path.join(HERE, "candidates.yaml"))
    return list(data.get("candidates") or [])


def fmt_eur(value: Optional[float]) -> str:
    if value is None:
        return "—"
    bn = value / 1_000_000_000
    return f"€{bn:.2f}bn"


def size_band(revenue: Optional[float], min_eur: float, max_eur: float) -> str:
    if revenue is None:
        return "unknown"
    if revenue < min_eur:
        return "below_5bn"
    if revenue > max_eur:
        return "above_10bn"
    return "5_10bn"


def why_in_band(rec: dict, cfg: dict) -> str:
    icp = cfg["icp"]
    centre = icp["bechtle_revenue_eur"]
    revenue = rec.get("revenue_eur")
    year = rec.get("revenue_year")
    if revenue is None:
        return (
            f"Revenue unpublished; DE HQ; employees {rec.get('employees')} "
            f"in fallback {icp['employee_fallback_min']:,}–{icp['employee_fallback_max']:,}"
        )
    delta = revenue - centre
    side = "above" if delta >= 0 else "below"
    return (
        f"{fmt_eur(revenue)} ({year}) is {side} Bechtle centre "
        f"{fmt_eur(centre)} and inside €5–10bn"
    )


def score_account(raw: dict, cfg: dict) -> dict:
    icp = cfg["icp"]
    min_eur = float(icp["revenue_min_eur"])
    max_eur = float(icp["revenue_max_eur"])
    titles = list(cfg.get("suggested_titles") or [])

    name = (raw.get("name") or "").strip()
    legal = (raw.get("legal_name") or name).strip()
    domain = normalize_domain(raw.get("domain") or raw.get("website"))
    industry = (raw.get("industry") or "").strip()
    city = (raw.get("city") or "").strip()
    country = (raw.get("country") or "Germany").strip()
    revenue = raw.get("revenue_eur")
    employees = raw.get("employees")
    sources = list(raw.get("source_urls") or [])

    rec = {
        "name": name,
        "legal_name": legal,
        "domain": domain,
        "website": raw.get("website") or (f"https://{domain}" if domain else None),
        "linkedin_url": raw.get("linkedin_url"),
        "city": city,
        "country": country,
        "revenue_eur": revenue,
        "revenue_year": raw.get("revenue_year"),
        "employees": employees,
        "industry": industry,
        "size_band": size_band(revenue, min_eur, max_eur),
        "suggested_titles_json": json.dumps(titles, ensure_ascii=False),
        "source_urls_json": json.dumps(sources, ensure_ascii=False),
        "status": "needs_review",
        "exclusion_reason": None,
        "score_notes": None,
    }

    if not domain:
        rec["status"] = "needs_review"
        rec["exclusion_reason"] = "missing_domain"
        rec["score_notes"] = "No public domain; cannot qualify or enrich"
        return rec

    if domain in set(cfg.get("suppressed_domains") or []):
        rec["status"] = "excluded_sector"
        rec["exclusion_reason"] = "suppressed_domain"
        rec["score_notes"] = "Domain is on the suppressed / Bechtle-group list"
        return rec

    hit = bechtle_hit(name=f"{name} {legal}", domain=domain)
    if hit:
        rec["status"] = "excluded_sector"
        rec["exclusion_reason"] = f"bechtle_group:{hit}"
        rec["score_notes"] = "Bechtle is the ICP size analogue, not a target account"
        return rec

    hit = finance_hit(name=f"{name} {legal}", industry=industry)
    if hit:
        rec["status"] = "excluded_sector"
        rec["exclusion_reason"] = f"finance:{hit}"
        rec["score_notes"] = "Hard exclude: banks, insurers, asset managers, PE/VC, payments"
        return rec

    hit = defense_hit(name=f"{name} {legal}", industry=industry)
    if hit:
        rec["status"] = "excluded_sector"
        rec["exclusion_reason"] = f"defense:{hit}"
        rec["score_notes"] = "Hard exclude: Bundeswehr / weapons / Wehrtechnik / defence electronics"
        return rec

    hit = alcohol_hit(name=f"{name} {legal}", industry=industry)
    if hit:
        rec["status"] = "excluded_sector"
        rec["exclusion_reason"] = f"alcohol:{hit}"
        rec["score_notes"] = "Hard exclude: alcohol producers / traders (breweries, wine, spirits)"
        return rec

    hit = too_big_group_hit(name=name, legal_name=legal)
    if hit:
        rec["status"] = "too_big"
        rec["exclusion_reason"] = f"known_mega_group:{hit}"
        rec["size_band"] = "above_10bn"
        rec["score_notes"] = (
            "Reject groups clearly larger than Bechtle even if a vendor shows a low number"
        )
        return rec

    if country.lower() not in {"germany", "deutschland", "de", "deu"}:
        rec["status"] = "excluded_sector"
        rec["exclusion_reason"] = f"geo:{country}"
        rec["score_notes"] = "ICP is Germany HQ or primary AG/GmbH operating in DE"
        return rec

    if raw.get("foreign_parent"):
        rec["status"] = "needs_review"
        rec["exclusion_reason"] = "foreign_parent_de_operating_company"
        rec["score_notes"] = (
            "German operating company of a foreign parent; review before Apollo"
        )
        return rec

    if revenue is None:
        emp_min = int(icp["employee_fallback_min"])
        emp_max = int(icp["employee_fallback_max"])
        if employees is not None and emp_min <= int(employees) <= emp_max and icp.get("qualify_on_employees"):
            rec["status"] = "qualified"
            rec["exclusion_reason"] = None
            rec["score_notes"] = f"No public revenue; {int(employees):,} employees in {emp_min:,}–{emp_max:,} band"
            return rec
        if employees is not None and emp_min <= int(employees) <= emp_max:
            rec["status"] = "needs_review"
            rec["exclusion_reason"] = "unpublished_revenue_employee_fallback"
            rec["score_notes"] = why_in_band(rec, cfg)
            return rec
        if employees is not None and int(employees) > emp_max:
            rec["status"] = "too_big"
            rec["exclusion_reason"] = "unpublished_revenue_employees_above_fallback"
            rec["score_notes"] = f"No public revenue; employees {employees} > {emp_max}"
            return rec
        rec["status"] = "too_small"
        rec["exclusion_reason"] = "unpublished_revenue_employees_below_fallback"
        rec["score_notes"] = (
            f"No public revenue; employees {employees} outside {emp_min}–{emp_max}"
        )
        return rec

    if float(revenue) > max_eur:
        rec["status"] = "too_big"
        rec["exclusion_reason"] = f"revenue>{max_eur}"
        rec["score_notes"] = f"{fmt_eur(revenue)} exceeds €10bn ceiling"
        return rec
    if float(revenue) < min_eur:
        rec["status"] = "too_small"
        rec["exclusion_reason"] = f"revenue<{min_eur}"
        rec["score_notes"] = f"{fmt_eur(revenue)} below €5bn floor"
        return rec

    rec["status"] = "qualified"
    rec["exclusion_reason"] = None
    rec["score_notes"] = why_in_band(rec, cfg)
    return rec


def write_sources_md(rows: List[dict], path: str, cfg: dict) -> None:
    icp = cfg["icp"]
    lines = [
        "# Sources — FORTE4 German account list (Stage 1)",
        "",
        "Every revenue figure below has a public URL. No invented numbers. No invented emails.",
        "",
        "## Bechtle size analogue (not a target)",
        "",
        f"- FY{icp['bechtle_revenue_year']} revenue **{fmt_eur(icp['bechtle_revenue_eur'])}**, "
        f"business volume **{fmt_eur(icp['bechtle_business_volume_eur'])}**, "
        f"**{icp['bechtle_employees']:,}** employees.",
        "- https://reports.bechtle.com/annual-report/2025",
        "- https://reports.bechtle.com/annual-report/2025/combined-management-report/economic-report/earnings-position.html",
        "- https://www.bechtle.com/dam/jcr:03890ff2-5b9a-4176-979f-95a170f38fac/pr-bechtle-fy-2025.pdf",
        "",
        "## ICP band",
        "",
        f"- Geography: Germany (HQ or primary AG/GmbH operating in DE).",
        f"- Revenue: €5bn–€10bn. Centre = Bechtle. Reject groups clearly larger than Bechtle.",
        f"- Unpublished revenue: `needs_review` only if employees "
        f"{icp['employee_fallback_min']:,}–{icp['employee_fallback_max']:,} and HQ is DE.",
        "",
        "## Company figures",
        "",
    ]
    for rec in sorted(rows, key=lambda r: (r["name"] or "").lower()):
        lines.append(f"### {rec['legal_name']} (`{rec['domain']}`)")
        lines.append("")
        lines.append(f"- Status: `{rec['status']}`")
        lines.append(f"- City: {rec.get('city') or '—'}")
        lines.append(f"- Industry: {rec.get('industry') or '—'}")
        lines.append(
            f"- Revenue: {fmt_eur(rec.get('revenue_eur'))} "
            f"(year {rec.get('revenue_year') or '—'})"
        )
        if rec.get("employees") is not None:
            lines.append(f"- Employees: {int(rec['employees']):,}")
        if rec.get("score_notes"):
            lines.append(f"- Notes: {rec['score_notes']}")
        urls = json.loads(rec.get("source_urls_json") or "[]")
        if urls:
            lines.append("- Sources:")
            for url in urls:
                lines.append(f"  - {url}")
        else:
            lines.append("- Sources: none (no revenue stored)")
        lines.append("")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines).rstrip() + "\n")


def write_accounts_csv(rows: List[dict], path: str) -> None:
    fields = [
        "name",
        "legal_name",
        "domain",
        "website",
        "city",
        "country",
        "revenue_eur",
        "revenue_year",
        "employees",
        "industry",
        "size_band",
        "status",
        "exclusion_reason",
        "score_notes",
        "source_urls_json",
    ]
    with open(path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for rec in sorted(rows, key=lambda r: (STATUS_ORDER.index(r["status"]) if r["status"] in STATUS_ORDER else 99, r["name"].lower())):
            writer.writerow({k: rec.get(k) for k in fields})


def print_summary(rows: List[dict]) -> None:
    counts = Counter(r["status"] for r in rows)
    qualified = [r for r in rows if r["status"] == "qualified"]
    rejected = len(rows) - len(qualified)
    print("")
    print("=" * 88)
    print("Stage 1 — German account list")
    print("=" * 88)
    print(f"  candidates scored : {len(rows)}")
    print(f"  qualified         : {len(qualified)}")
    print(f"  rejected          : {rejected}")
    for status in STATUS_ORDER:
        if status == "qualified":
            continue
        print(f"    {status:16s}: {counts.get(status, 0)}")
    print("")
    print("Qualified accounts")
    print("-" * 88)
    header = f"{'name':22s} {'domain':24s} {'revenue':>10s} {'year':>6s} {'city':16s} why in band"
    print(header)
    print("-" * 88)
    for rec in sorted(qualified, key=lambda r: (-(r.get("revenue_eur") or 0), r["name"])):
        print(
            f"{rec['name'][:22]:22s} {rec['domain'][:24]:24s} "
            f"{fmt_eur(rec.get('revenue_eur')):>10s} "
            f"{str(rec.get('revenue_year') or '—'):>6s} "
            f"{(rec.get('city') or '—')[:16]:16s} {rec.get('score_notes') or ''}"
        )
    print("-" * 88)
    print("")


def run(db_path: Optional[str], config_path: Optional[str], candidates_path: Optional[str]) -> Tuple[List[dict], Dict[str, int]]:
    cfg = load_config(config_path)
    raw_list = load_candidates(candidates_path)
    scored = [score_account(raw, cfg) for raw in raw_list]

    write_sources_md(scored, os.path.join(HERE, "SOURCES.md"), cfg)
    write_accounts_csv(scored, os.path.join(HERE, "accounts.csv"))

    with open_db(db_path) as (db, conn):
        run_id = db.start_run(
            conn,
            "accounts",
            notes="Stage 1 public-web account compile; lawful_basis=legitimate_interest_b2b; no outreach",
        )
        try:
            for rec in scored:
                db.upsert_account(conn, rec)
                try:
                    from pipeline_crm import upsert_customer

                    upsert_customer(rec)
                except Exception as crm_exc:
                    print(f"CRM sync skipped for {rec.get('name')}: {crm_exc}")
            conn.commit()
            db.finish_run(conn, run_id, rows_written=len(scored))
            counts = db.counts_by_status(conn)
        except Exception as exc:
            db.finish_run(conn, run_id, rows_written=0, error=str(exc))
            raise

    print_summary(scored)
    print(f"Database : {db_path or Database().path}")
    print(f"Wrote    : SOURCES.md, accounts.csv")
    print(f"DB counts: {counts}")
    return scored, counts


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Stage 1: compile and score German accounts")
    parser.add_argument("--db", help="SQLite path (default: DB_PATH / outreach_log.sqlite)")
    parser.add_argument("--config", help="Path to config.yaml")
    parser.add_argument("--candidates", help="Path to candidates.yaml")
    args = parser.parse_args(argv)
    run(args.db, args.config, args.candidates)
    return 0


if __name__ == "__main__":
    sys.exit(main())
