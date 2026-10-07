"""Lead gate for FORTLO's Apollo refuel.

FORTLO finds companies in Apollo; this module decides which of them are worth
spending Apollo contact credits on and putting into the email campaign, using
the ICP built from the profile and the invoice CRM.

    gate = LeadGate()
    gate.prefetch(raw_orgs)            # one batched AI call for blank industries
    verdict = gate.judge(raw_org, fortlo_status="qualified")
    verdict["decision"]                # "pursue" or "hold"
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

from leadgen import ai_enrich, signals as signal_store
from leadgen.model_review import client_from_env
from leadgen.score import PURSUE_AT, score_account


def _as_account(raw: dict, fortlo_status: str | None = None, exclusion_reason: str | None = None) -> dict:
    return {
        "name": raw.get("name") or "",
        "domain": raw.get("domain") or "",
        "industry": raw.get("industry") or "",
        "country": raw.get("country") or "",
        "employee_count": raw.get("employees") if raw.get("employees") is not None else raw.get("employee_count"),
        "annual_revenue": raw.get("revenue_eur") if raw.get("revenue_eur") is not None else raw.get("annual_revenue"),
        "icp_status": fortlo_status or raw.get("status") or raw.get("icp_status") or "",
        "exclusion_reason": exclusion_reason or raw.get("exclusion_reason") or "",
        "score_notes": raw.get("score_notes") or "",
        "tech_stack": raw.get("tech_stack") or "",
    }


class LeadGate:
    """Builds the ICP once (refreshed every `ttl_seconds`) and scores FORTLO accounts against it."""

    def __init__(self, ttl_seconds: int = 6 * 3600, client=None, use_model: bool = True, pursue_at: int | None = None):
        self.ttl_seconds = ttl_seconds
        self.client = client if client is not None else (client_from_env() if use_model else None)
        self.pursue_at = pursue_at if pursue_at is not None else PURSUE_AT
        self._icp = None
        self._companies: list[dict] = []
        self._built_at = 0.0

    def icp(self) -> dict:
        if self._icp is None or time.time() - self._built_at > self.ttl_seconds:
            from leadgen.pipeline import run

            result = run(use_model=False, lead_limit=0)
            self._icp = result["icp"]
            self._companies = [{"company_name": name} for name in result.get("recruiting_companies") or []]
            self._built_at = time.time()
        return self._icp

    def prefetch(self, raws: list[dict], limit: int = 200) -> dict:
        """Classify blank industries for a batch of orgs in as few model calls as possible."""
        if self.client is None:
            return {"requested": 0, "classified": 0, "errors": ["ANTHROPIC_API_KEY not set"]}
        return ai_enrich.enrich([_as_account(raw) for raw in raws], self.client, limit=limit)

    def collect_signals(self, raw: dict, *, fetch_jobs=None, research: bool = False) -> dict:
        """Fetch hiring signals (Apollo jobs) and, when asked, Claude web research. Errors are reported, not raised."""
        domain = raw.get("domain") or ""
        out = {"hiring": [], "research": [], "research_new": False, "research_tried": False, "errors": []}
        if fetch_jobs is not None:
            try:
                out["hiring"] = signal_store.hiring(domain, raw.get("apollo_org_id"), fetch_jobs)
            except Exception as exc:
                out["errors"].append(f"jobs: {exc.__class__.__name__}: {str(exc)[:120]}")
        if research:
            try:
                found = signal_store.research(raw.get("name") or domain, domain, self.client)
                out["research"], out["research_new"] = found["signals"], not found["cached"]
                out["research_tried"] = out["research_new"]
            except Exception as exc:
                out["research_tried"] = True  # a failed call still costs money and budget
                out["errors"].append(f"research: {exc.__class__.__name__}: {str(exc)[:120]}")
        return out

    def judge(self, raw: dict, fortlo_status: str | None = None, exclusion_reason: str | None = None) -> dict:
        account = ai_enrich.apply([_as_account(raw, fortlo_status, exclusion_reason)])[0]
        account["signals"] = signal_store.cached(account["domain"])
        if self.client is not None and ai_enrich.needs_enrichment(account) and not account.get("ai"):
            ai_enrich.enrich([account], self.client, limit=1)
            account = ai_enrich.apply([account])[0]
        row = score_account(account, self.icp(), self._companies)
        decision = "pursue" if row["score"] >= self.pursue_at and row["decision"] == "pursue" else "hold"
        return {
            "score": row["score"],
            "decision": decision,
            "reason": row["reason"][:1500],
            "sector": row["sector"],
            "industry": row["industry"],
            "ai_inferred": row["ai_inferred"],
            "signal_points": row["signal_points"],
            "signals": row["signals"],
            "scored_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        }
