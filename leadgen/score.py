"""Score accounts against the ICP. Every point has a visible reason.

Weights (max 100):
  sector that matches won customers      35 (25 if only one win in that sector; x0.8 if AI-inferred)
  size inside the won range              20 (partial credit near it, 5 when size is unknown)
  documented SAP / S/4 / MDG evidence    20 (10 when the model only says SAP is likely)
  HQ in DACH                             15
  sector and SAP evidence together       10
Buying signals (leadgen/signals.py)         up to +15, each with its source URL; an SAP-type signal
                                           also counts as documented SAP evidence. Not for IT services
                                           firms: their SAP MDG openings are consultant roles (competitors).
Caps: excluded sector or denylisted industry 10, existing end customer 15, recruiting company 15.
`decision` is "pursue" at PURSUE_AT or above. FORTLO's own size band (too_big / too_small)
is not used: the size fit comes from the won customers instead.
"""

from __future__ import annotations

import re

from leadgen.relationships import normalize

PURSUE_AT = 60
DACH = {"germany", "deutschland", "de", "deu", "austria", "österreich", "osterreich", "at", "switzerland", "schweiz", "ch"}

# Free-text industry -> canonical sector. Order matters: first hit wins.
SECTOR_WORDS = (
    ("financial services", ("bank", "sparkasse", "insurance", "insurer", "versicherung", "asset management",
                            "private equity", "venture capital", "payment", "fintech", "financial")),
    ("defence", ("defense", "defence", "weapons", "munition", "wehrtechnik", "military")),
    ("alcohol", ("alcohol", "brewery", "brauerei", "winery", "distiller", "spirits")),
    ("public sector & education", ("public sector", "government", "university", "hochschule", "education", "non-profit")),
    ("pharma & life sciences", ("pharma", "life science", "biotech", "medtech", "medical device", "health care products",
                                "healthcare products", "diagnostics")),
    ("chemicals & agri", ("chemical", "chemie", "agri", "crop", "fertili", "agro")),
    ("energy", ("energy", "energie", "oil", "gas", "utilit", "renewable", "power", "refin")),
    ("it services", ("system house", "systemhaus", "it services", "it / system", "information technology")),
    ("logistics", ("logistic", "transport", "shipping", "freight")),
    ("retail & consumer", ("retail", "consumer", "wholesale", "food", "fashion", "apparel")),
    ("manufacturing", ("manufactur", "machinery", "maschinenbau", "industrial", "automotive", "electrical",
                       "electronic", "steel", "metal", "building material", "intralogistic", "engineering",
                       "fertigung", "mechanical", "plastics", "packaging", "aerospace", "semiconductor")),
)
BLOCKED_SECTORS = {"financial services", "defence", "alcohol", "public sector & education"}
SAP_EVIDENCE = re.compile(r"(?<![a-z0-9])(sap|s/4|s4|s/4hana|s4hana|mdg|hana)(?![a-z0-9])")


def sector_of(industry: str) -> str | None:
    hay = (industry or "").lower()
    if not hay.strip():
        return None
    if hay.strip() == "it":
        return "it services"
    for sector, words in SECTOR_WORDS:
        if any(word in hay for word in words):
            return sector
    return "other"


def _evidence_text(account: dict) -> str:
    # Suggested titles are FORTLO's search list, not evidence about the account, so they are left out.
    parts = [account.get("score_notes") or "", account.get("industry") or "", account.get("tech_stack") or ""]
    return " ".join(str(part) for part in parts).lower()


_LEGAL_TAIL = ("", "ag", "gmbh", "se", "kg", "group", "gruppe", "holding")


def _is_known_end_customer(name: str, wins: list[dict]) -> str | None:
    """Same legal entity, or the same name plus a legal suffix. Not a sister company."""
    hay = normalize(name)
    if not hay:
        return None
    for win in wins:
        token = normalize(win["company"].split("(")[0])
        if len(token) < 5:
            continue
        if hay == token or (hay.startswith(token) and hay[len(token):] in _LEGAL_TAIL):
            return win["company"]
    return None


def _related_end_customer(name: str, wins: list[dict]) -> str | None:
    hay = normalize(name)
    if not hay or _is_known_end_customer(name, wins):
        return None
    for win in wins:
        token = normalize(win["company"].split("(")[0])
        if len(token) >= 5 and (hay.startswith(token) or token.startswith(hay)):
            return win["company"]
    return None


def _is_recruiting_company(name: str, companies: list[dict]) -> str | None:
    hay = normalize(name)
    for company in companies:
        token = normalize(company["company_name"])
        if len(token) >= 5 and token in hay:
            return company["company_name"]
    return None


def _hits_disqualifier(account: dict, rule: dict) -> bool:
    field = (rule.get("field") or "").lower()
    value = (rule.get("value") or "").lower()
    if not value:
        return False
    if field == "industry":
        return value in (account.get("industry") or "").lower()
    if field == "country":
        return value in (account.get("country") or "").lower()
    return value in _evidence_text(account)


def _num(value) -> float | None:
    try:
        return float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _size_points(account: dict, band: dict) -> tuple[int, str]:
    revenue = _num(account.get("annual_revenue"))
    employees = _num(account.get("employee_count"))
    low, high = band.get("revenue_min_eur"), band.get("revenue_max_eur")
    emp_low = band.get("employees_min")
    if revenue and low and high:
        label = f"revenue €{revenue / 1e9:.1f}bn" if revenue >= 1e9 else f"revenue €{revenue / 1e6:.0f}M"
        if low <= revenue <= high:
            return 20, f"{label} is inside the won range €{low / 1e9:.1f}bn–€{high / 1e9:.0f}bn"
        if low / 3 <= revenue < low:
            return 8, f"{label} is below the won range (from €{low / 1e9:.1f}bn)"
        if high < revenue <= high * 3:
            return 8, f"{label} is above the largest win"
        if revenue < low:
            return 0, f"{label} is far below the won range (from €{low / 1e9:.1f}bn)"
        return 0, f"{label} is far above the largest win"
    if employees and emp_low:
        if employees >= emp_low:
            return 12, f"{int(employees):,} employees, at or above the smallest win ({int(emp_low):,}); revenue unknown"
        if employees >= emp_low / 3:
            return 5, f"{int(employees):,} employees, below the smallest win; revenue unknown"
        return 0, f"{int(employees):,} employees is far below the smallest win"
    return 5, "size unknown"


def score_account(account: dict, icp: dict, companies: list[dict] | None = None) -> dict:
    companies = companies or []
    matched, missing, reasons = [], [], []
    score = 0
    ai = account.get("ai") or {}
    industry = (account.get("industry") or "").strip()
    country = (account.get("country") or "").strip() or (ai.get("hq_country") or "").strip()
    evidence = _evidence_text(account)
    won = icp.get("won_sectors") or {}

    # Sector
    sector = sector_of(industry)
    ai_sector = False
    if sector is None and ai.get("sector") and ai["sector"] != "unknown":
        sector, ai_sector = ai["sector"], True
    shown_industry = industry or (f"{ai.get('industry')} (AI)" if ai.get("industry") else "")
    tag = " (AI-inferred)" if ai_sector else ""
    blocked = sector in BLOCKED_SECTORS
    sector_hit = None
    if sector is None:
        missing.append("industry")
        reasons.append("Industry is empty and no AI classification is cached.")
    elif blocked:
        reasons.append(f"Sector {sector}{tag} is a hard exclusion.")
    elif sector in won:
        sector_hit = sector
        points = 35 if len(won[sector]) >= 2 else 25
        if ai_sector:
            points = round(points * 0.8)
        score += points
        matched.append(f"sector {sector}{tag} matches won customers {', '.join(won[sector][:3])}")
    else:
        reasons.append(f"Sector {sector}{tag} has no won customer.")

    # Size
    size_pts, size_note = _size_points(account, icp.get("size_band") or {})
    score += size_pts
    (matched if size_pts >= 12 else reasons).append(size_note[0].upper() + size_note[1:])
    if size_note == "size unknown":
        missing.append("revenue and employees")

    # Buying signals (job postings, sourced web research)
    from leadgen.signals import points as signal_points

    sig_pts, sig_reasons, sig_sap = signal_points(account.get("signals") or [])
    if sig_pts and (sector == "it services" or sector in BLOCKED_SECTORS):
        reasons.append("Buying signals ignored: an IT services firm hiring SAP staff is a competitor, not a buyer.")
        sig_pts, sig_reasons, sig_sap = 0, [], False

    # SAP evidence
    sap_hit = bool(SAP_EVIDENCE.search(evidence)) or sig_sap
    if sap_hit:
        score += 20
        matched.append("SAP / S/4 / MDG is documented on the account")
    elif ai.get("uses_sap") in {"yes", "likely"}:
        score += 10
        matched.append(f"model says SAP use is {ai['uses_sap']} (AI-inferred, unverified)")
    else:
        missing.append("SAP evidence")
        reasons.append("No SAP MDG or S/4 evidence on the account.")

    # Geography
    if not country:
        missing.append("country")
    elif country.lower() in DACH:
        score += 15
        matched.append(f"HQ in {country}")
    else:
        reasons.append(f"HQ {country} is outside DACH.")

    if sector_hit and (sap_hit or ai.get("uses_sap") == "yes"):
        score += 10
        matched.append("won sector and SAP together, the pattern on most wins")

    if sig_pts:
        score += sig_pts
        matched.extend(sig_reasons)

    # Caps
    status = (account.get("icp_status") or "").strip().lower()
    disqualifiers_hit = [
        rule.get("label") or rule.get("value") or "disqualifier"
        for rule in icp.get("disqualifiers") or []
        if _hits_disqualifier(account, rule)
    ]
    firmographic_match = bool(sector_hit) and country.lower() in DACH
    fails_disqualifier = firmographic_match and bool(disqualifiers_hit)
    if fails_disqualifier:
        score = min(score, 25)
        reasons.append("Firmographics match and a disqualifier fails: " + "; ".join(disqualifiers_hit) + ".")
    if blocked or status == "excluded_sector":
        score = min(score, 10)
        if status == "excluded_sector":
            extra = (account.get("exclusion_reason") or "").strip()
            reasons.append("FORTLO marks this account excluded_sector" + (f" ({extra})" if extra else "") + ".")

    name = account.get("name") or ""
    already = _is_known_end_customer(name, icp.get("wins") or [])
    related = _related_end_customer(name, icp.get("wins") or [])
    channel = _is_recruiting_company(name, companies)
    if already:
        score = min(score, 15)
        reasons.append(f"Already a delivered end customer ({already}). Use the existing relationship, not cold outreach.")
    elif related:
        reasons.append(f"Name is related to current end customer {related}. Check for a conflict before outreach.")
    if channel:
        score = min(score, 15)
        reasons.append(f"This name is a recruiting company in the client list ({channel}), not an end customer.")

    score = max(0, min(100, int(score)))
    decision = "pursue" if score >= PURSUE_AT and not already and not channel else "hold"
    persona = "Decision-maker" if decision == "pursue" else "none"
    if decision == "pursue":
        next_action = "Find the Head of Master Data / ERP and confirm an active SAP MDG or S/4 program before writing."
        if related:
            next_action = f"Check the conflict with current end customer {related}. " + next_action
        outreach = _outreach(account, sector_hit, icp)
    elif already or channel:
        next_action = "No cold outreach. Existing end customer or recruiting company."
        outreach = ""
    else:
        next_action = "Hold. " + " ".join(reasons[:2])
        outreach = ""

    reason = " ".join(r if r.endswith(".") else r + "." for r in reasons) if reasons else (
        "Matches the won pattern on every field that exists."
    )
    if matched:
        reason = "Matched: " + "; ".join(matched) + ". " + reason

    return {
        "account": name or "(unnamed)",
        "domain": account.get("domain") or "",
        "score": score,
        "decision": decision,
        "reason": reason.strip(),
        "matched_traits": matched,
        "missing_data": missing,
        "firmographic_match": firmographic_match,
        "fails_disqualifier": fails_disqualifier,
        "disqualifiers": disqualifiers_hit,
        "recommended_persona": persona,
        "outreach": outreach,
        "outreach_by": "rules" if outreach else "",
        "next_action": next_action,
        "probe": bool(account.get("probe")),
        "industry": shown_industry,
        "sector": sector or "",
        "ai_inferred": bool(ai),
        "country": country,
        "signal_points": sig_pts,
        "signals": list(account.get("signals") or []),
    }


def _outreach(account: dict, sector: str | None, icp: dict) -> str:
    won = (icp.get("won_sectors") or {}).get(sector or "", [])
    examples = ", ".join(won[:3]) or ", ".join(icp.get("example_accounts") or []) or "the won end customers"
    return (
        f"{account.get('name')}: same profile as {examples}. Delivered there: SAP MDG on S/4HANA, "
        "consolidation, business rules (BRF+), and replication, as lead or solution architect. "
        "Ask the Head of Master Data whether an MDG or S/4 master-data program is running or planned."
    )


def rank_accounts(accounts: list[dict], icp: dict, companies: list[dict]) -> list[dict]:
    ranked = [score_account(account, icp, companies) for account in accounts]
    ranked.sort(key=lambda row: (-row["score"], row["account"].lower()))
    return ranked


PROBE_BANK = {
    "name": "Probe Bank AG",
    "domain": "probe-bank.example",
    "industry": "bank",
    "country": "Germany",
    "employee_count": 8000,
    "annual_revenue": 2_000_000_000,
    "icp_status": "excluded_sector",
    "exclusion_reason": "bank",
    "score_notes": "",
    "probe": True,
}

PROBE_MATCH = {
    "name": "Probe Werke GmbH",
    "domain": "probe-werke.example",
    "industry": "manufacturing",
    "country": "Germany",
    "employee_count": 12000,
    "annual_revenue": 4_000_000_000,
    "icp_status": "qualified",
    "exclusion_reason": "",
    "score_notes": "SAP MDG rollout on S/4HANA",
    "probe": True,
}
