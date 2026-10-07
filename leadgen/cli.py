"""Run the ICP flow from the shell.

Examples:
    python -m leadgen run                      # print ICP brief and top leads
    python -m leadgen serve                    # web page (LEADGEN_HOST / --port)
    python -m leadgen enrich --limit 400       # AI-classify accounts with no industry (needs ANTHROPIC_API_KEY)
    python -m leadgen score-crm                # preview leadgen scores for every FORTLO customer
    python -m leadgen score-crm --write        # store them in customers.leadgen_* (backup first)
"""

from __future__ import annotations

import argparse
import json
import os
from collections import Counter

from leadgen.pipeline import run


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build the ICP from CRM clients and the profile, then score leads.")
    parser.add_argument("command", choices=("run", "serve", "enrich", "score-crm", "drafts"), nargs="?", default="run")
    parser.add_argument("--port", type=int, default=int(os.environ.get("LEADGEN_PORT") or 8010))
    parser.add_argument("--limit", type=int, default=400, help="enrich: max accounts to classify")
    parser.add_argument("--write", action="store_true", help="score-crm: write to the FORTLO database")
    parser.add_argument("--no-model", action="store_true", help="skip every model call")
    args = parser.parse_args(argv)

    if args.command == "serve":
        from leadgen.app import serve

        serve(args.port)
        return 0

    if args.command == "enrich":
        from leadgen import ai_enrich
        from leadgen.model_review import client_from_env

        client = client_from_env()
        if client is None:
            print("ANTHROPIC_API_KEY is not set.")
            return 1
        summary = ai_enrich.enrich(_accounts(), client, limit=args.limit)
        print(json.dumps(summary, indent=2))
        return 0 if not summary["errors"] else 2

    if args.command == "drafts":
        import outreach

        from leadgen import personalize
        from leadgen.model_review import client_from_env

        con = outreach.connect()
        items = personalize.candidates(con, limit=min(args.limit, 50))
        print(f"{len(items)} contacts at companies with buying signals")
        if args.no_model:
            for item in items[:20]:
                print(f"  {item['company']}: {item['facts'][0]['fact'][:100]}")
            return 0
        print(json.dumps(personalize.draft(con, client_from_env(), items), indent=2))
        return 0

    if args.command == "score-crm":
        from leadgen import ai_enrich
        from leadgen.crm_sync import write_scores
        from leadgen.score import score_account

        db = os.environ.get("LEADGEN_FORTLO_DB")
        if not db:
            print("Set LEADGEN_FORTLO_DB to the FORTLO sqlite file.")
            return 1
        result = run(use_model=False, lead_limit=0)
        accounts = ai_enrich.apply(_accounts())
        companies = [{"company_name": n} for n in result["recruiting_companies"]]
        ranked = [score_account(account, result["icp"], companies) for account in accounts]
        counts = Counter(row["decision"] for row in ranked)
        print(f"{len(ranked)} customers scored: {dict(counts)}")
        for row in sorted(ranked, key=lambda r: -r["score"])[:20]:
            print(f"  {row['score']:3d} {row['decision']:6s} {row['account']}")
        if args.write:
            print(write_scores(db, ranked, accounts))
        else:
            print("Preview only. Add --write to store leadgen_score / leadgen_decision / leadgen_reason.")
        return 0

    result = run(use_model=False if args.no_model else None)
    brief = {
        "generated_at": result["generated_at"],
        "icp_summary": result["icp"]["summary_line"],
        "gaps": result["gaps"],
        "model": result["model"],
        "win_count": result["icp"]["win_count"],
        "size_band": result["icp"]["size_band"],
        "won_sectors": result["icp"]["won_sectors"],
        "channel": [
            {"agency": row["agency"], "invoices": row["invoices"], "net_eur": row["net_eur"], "end_customers": row["end_customers"]}
            for row in result["icp"]["channel"]
        ],
        "score_spread": result["score_spread"],
        "pursue_count": result["pursue_count"],
        "probes": [{"account": row["account"], "score": row["score"]} for row in result["probes"]],
        "top_leads": [
            {"account": row["account"], "score": row["score"], "decision": row["decision"], "sector": row["sector"]}
            for row in result["leads"][:15]
        ],
    }
    print(result["icp"]["narrative"])
    print()
    print(json.dumps(brief, indent=2, ensure_ascii=False))
    return 0


def _accounts() -> list[dict]:
    """The same account list the pipeline scores (FORTLO sqlite when configured, else the snapshot)."""
    from pathlib import Path

    from leadgen.pipeline import DATA, _adapter

    invoice_db = Path(os.environ["LEADGEN_INVOICE_DB"]) if os.environ.get("LEADGEN_INVOICE_DB") else None
    account_db = Path(os.environ["LEADGEN_FORTLO_DB"]) if os.environ.get("LEADGEN_FORTLO_DB") else None
    adapter = _adapter(invoice_db, account_db, DATA / "crm_snapshot.json", DATA / "accounts_snapshot.json")
    return adapter.sourced_accounts()


if __name__ == "__main__":
    raise SystemExit(main())
