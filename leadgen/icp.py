"""Build a written ICP from won, retained end customers only."""

from __future__ import annotations

import json
import statistics
from pathlib import Path

from leadgen.relationships import NOT_STAFFING, normalize

FIRMOGRAPHICS_FILE = Path(__file__).resolve().parent / "data" / "wins_firmographics.json"


def load_firmographics(path: Path = FIRMOGRAPHICS_FILE) -> list[dict]:
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    return list(data.get("wins") or []) if isinstance(data, dict) else list(data)


def _firmo_for(company: str, firmographics: list[dict]) -> dict:
    key = normalize(company)
    return next((row for row in firmographics if normalize(row.get("company") or "") == key), {})


def size_band(wins: list[dict], firmographics: list[dict]) -> dict:
    rows = [_firmo_for(w["company"], firmographics) for w in wins]
    revenues = [f["revenue_eur"] for f in rows if f.get("revenue_eur")]
    employees = [f["employees"] for f in rows if f.get("employees")]
    if not revenues:
        return {"known": 0, "of": len(wins)}
    return {
        "known": len(revenues),
        "of": len(wins),
        # Half the smallest win to 1.5x the largest: a fit, not a hard wall.
        "revenue_min_eur": min(revenues) / 2,
        "revenue_max_eur": max(revenues) * 1.5,
        "revenue_median_eur": statistics.median(revenues),
        "smallest_win_eur": min(revenues),
        "largest_win_eur": max(revenues),
        "employees_min": min(employees) if employees else None,
    }


def won_sectors(wins: list[dict], firmographics: list[dict]) -> dict:
    out: dict[str, list[str]] = {}
    for win in wins:
        sector = _firmo_for(win["company"], firmographics).get("sector")
        if sector:
            out.setdefault(sector, []).append(win["company"])
    return dict(sorted(out.items(), key=lambda kv: -len(kv[1])))


def channel_profile(companies: list[dict], invoices: list[dict], relationships: dict) -> list[dict]:
    """Agencies that pay the invoices: volume, window, unit price, and the end customers they lead to."""
    links: dict[str, list[str]] = {}
    for row in (relationships.get("confirmed") or []) + (relationships.get("suggested") or []):
        links.setdefault(normalize(row["recruiting_company"]), []).append(
            row["end_customer"] + ("" if row.get("confirmed") else " (unconfirmed)")
        )
    rows = []
    for company in companies:
        mine = [inv for inv in invoices if inv["client_id"] == company["source_id"]]
        if not mine or any(skip in normalize(company["company_name"]) for skip in NOT_STAFFING):
            continue
        prices = [inv["price"] for inv in mine if inv.get("price")]
        rows.append(
            {
                "agency": company["company_name"],
                "contact": company.get("contact_name") or "",
                "invoices": len(mine),
                "net_eur": round(sum(inv["net_price"] for inv in mine), 2),
                "first": min(inv["date"] for inv in mine),
                "last": max(inv["date"] for inv in mine),
                "median_unit_price": statistics.median(prices) if prices else None,
                "end_customers": links.get(normalize(company["company_name"]), []),
            }
        )
    rows.sort(key=lambda row: -row["net_eur"])
    return rows

SECTOR_LEXICON = (
    ("manufacturing", ("manufacturing", "fertigung")),
    ("pharma", ("pharma", "pharmaceutical")),
    ("oil & gas", ("oil & gas", "oil and gas", "öl & gas")),
    ("energy", ("energy", "energie")),
)

TECH_LEXICON = (
    ("SAP MDG", ("sap mdg", "mdg-")),
    ("S/4HANA", ("s/4", "s4hana", "s/4hana")),
    ("BRF+", ("brf+", "brf +")),
    ("consolidation / MDC", ("mdc", "consolidation", "golden", "konsolid")),
    ("integration / IDoc", ("idoc", "replication", "replikation", "soap", "odata", "ariba")),
    ("mass change", ("mass-change", "mass change", "massen")),
    ("duplicate check", ("duplicate", "dublet")),
)


def _blob(engagement: dict) -> str:
    parts = [engagement.get("company") or "", engagement.get("title") or "", engagement.get("location") or ""]
    parts.extend(engagement.get("bullets") or [])
    return " ".join(parts).lower()


def _count_terms(engagements: list[dict], lexicon) -> list[dict]:
    rows = []
    for label, needles in lexicon:
        matched = [item["company"] for item in engagements if any(needle in _blob(item) for needle in needles)]
        rows.append(
            {
                "label": label,
                "count": len(matched),
                "of": len(engagements),
                "accounts": matched,
            }
        )
    return rows


def _confidence(count: int, total: int) -> str:
    if total == 0:
        return "none"
    if total < 8:
        base = "small sample"
    elif total < 20:
        base = "limited sample"
    else:
        base = "usable sample"
    share = count / total
    if share >= 0.7:
        strength = "this trait shows up on most wins"
    elif share >= 0.4:
        strength = "this trait shows up on some wins"
    elif count == 0:
        strength = "this trait is missing"
    else:
        strength = "this trait is thin"
    return f"{base}; {strength} ({count} of {total})"


def build_icp(
    engagements: list[dict],
    summary: str,
    relationships: dict,
    firmographics: list[dict] | None = None,
    companies: list[dict] | None = None,
    invoices: list[dict] | None = None,
) -> dict:
    wins = [item for item in engagements if item.get("outcome") == "won_retained"]
    total = len(wins)
    firmographics = load_firmographics() if firmographics is None else firmographics
    band = size_band(wins, firmographics)
    sectors_won = won_sectors(wins, firmographics)
    channel = channel_profile(companies or [], invoices or [], relationships)
    countries = {}
    missing_country = []
    for item in wins:
        if not item.get("countries"):
            missing_country.append(item["company"])
            continue
        for country in item["countries"]:
            countries.setdefault(country, []).append(item["company"])

    sectors = _count_terms(wins, SECTOR_LEXICON)
    summary_hits = []
    summary_lower = (summary or "").lower()
    for label, needles in SECTOR_LEXICON:
        if any(needle in summary_lower for needle in needles):
            summary_hits.append(label)
    techs = _count_terms(wins, TECH_LEXICON)
    tech_accounts = []
    for item in wins:
        if any(needle in _blob(item) for _label, needles in TECH_LEXICON for needle in needles):
            tech_accounts.append(item["company"])

    deal_rows = []
    for link in relationships.get("confirmed") or []:
        billing = link.get("billing") or {}
        if billing.get("invoice_count"):
            deal_rows.append(
                {
                    "end_customer": link["end_customer"],
                    "recruiting_company": link["recruiting_company"],
                    "net_eur": billing["net_eur"],
                    "invoice_count": billing["invoice_count"],
                    "window": f"{billing['first_date']} to {billing['last_date']}",
                }
            )

    missing_fields = [
        "funding stage",
        "sales cycle length",
        "lost-deal reasons",
    ]
    if not deal_rows:
        missing_fields.append("deal size")
    if missing_country:
        missing_fields.append("country on " + ", ".join(missing_country))
    if band.get("known", 0) < total:
        missing_fields.append(f"revenue on {total - band.get('known', 0)} of {total} wins")

    partial = True
    if total == 0:
        narrative = (
            "No closed-won end customers were found on the profile. "
            "The ICP is empty. Add Berufserfahrung lines, or map a CRM export through the adapter, then refresh. "
            "Employee count, revenue, funding stage, sales cycle, and lost reasons are missing, so nothing here is a full ICP."
        )
    else:
        country_bits = [f"{country} ({len(names)})" for country, names in sorted(countries.items(), key=lambda kv: -len(kv[1]))]
        tech_bits = [f"{row['label']} on {row['count']} of {total}" for row in techs if row["count"]]
        sector_sentence = (
            "The profile summary names these sectors: " + ", ".join(summary_hits) + ". "
            if summary_hits
            else "The profile summary does not name a sector. "
        )
        deal_sentence = (
            "Invoiced value is known only where a recruiting company is confirmed and has invoices: "
            + "; ".join(
                f"{row['end_customer']} via {row['recruiting_company']} net {row['net_eur']:,.0f} EUR across {row['invoice_count']} invoices ({row['window']})"
                for row in deal_rows
            )
            + ". That is billed delivery, not the end customer's contract value."
            if deal_rows
            else "No confirmed recruiting link has an invoice, so deal size is missing."
        )
        narrative = (
            f"Partial ICP from {total} delivered end-customer engagements on the profile. "
            "Wins are the Berufserfahrung entries. Early employment under Weitere Erfahrung is excluded. "
            "Recruiting companies are the channel, not the ICP account. "
            f"{sector_sentence}"
            "There is no industry field on the engagement, so a sector is counted only when the word appears on that account's line or bullets. "
            f"Country is taken from the city on the line: {', '.join(country_bits) or 'none parsed'}. "
            f"{len(missing_country)} engagements have a region rather than a mapped city ({', '.join(missing_country) or 'none'}). "
            f"Tech mentioned on the wins: {', '.join(tech_bits) or 'none'}. "
            f"{deal_sentence} "
            f"{_size_sentence(band)} "
            "Funding stage and sales cycle length are not on these records, so they are left blank. "
            f"Lost deals: none. Disqualifiers are empty because a disqualifier has to be more common in losses than in wins, and there are no losses to compare. "
            f"Sample size is {total}. Confidence stays limited until closed-won rows are in the dozens and lost reasons are stored."
        )

    evidence = [
        {
            "field": "sector words on the account line",
            "drove_pattern": any(row["count"] for row in sectors),
            "detail": ", ".join(f"{row['label']} {row['count']}/{total}" for row in sectors) or "no wins",
            "accounts": sum(row["count"] for row in sectors),
            "confidence": _confidence(sum(1 for row in sectors if row["count"]), total),
        },
        {
            "field": "sector words in the profile summary",
            "drove_pattern": bool(summary_hits),
            "detail": ", ".join(summary_hits) or "summary missing",
            "accounts": total if summary_hits else 0,
            "confidence": "one paragraph, not a per-account field" if summary_hits else "missing",
        },
        {
            "field": "country from the city on the line",
            "drove_pattern": bool(countries),
            "detail": ", ".join(f"{k} {len(v)}" for k, v in countries.items()) or "none",
            "accounts": total - len(missing_country),
            "confidence": _confidence(total - len(missing_country), total),
        },
        {
            "field": "tech stack in title or bullets",
            "drove_pattern": len(tech_accounts) > 0,
            "detail": "; ".join(f"{row['label']} {row['count']}/{total}" for row in techs),
            "accounts": len(set(tech_accounts)),
            "confidence": _confidence(len(set(tech_accounts)), total),
        },
        {
            "field": "deal size from confirmed invoices",
            "drove_pattern": bool(deal_rows),
            "detail": ", ".join(f"{row['end_customer']} {row['net_eur']:,.0f} EUR" for row in deal_rows) or "missing",
            "accounts": len(deal_rows),
            "confidence": _confidence(len(deal_rows), total),
        },
        {
            "field": "revenue and employees (wins_firmographics.json)",
            "drove_pattern": band.get("known", 0) > 0,
            "detail": _size_sentence(band),
            "accounts": band.get("known", 0),
            "confidence": _confidence(band.get("known", 0), total) + "; hand-entered approximate figures",
        },
        {
            "field": "sector per win (wins_firmographics.json)",
            "drove_pattern": bool(sectors_won),
            "detail": "; ".join(f"{k} {len(v)}" for k, v in sectors_won.items()) or "missing",
            "accounts": sum(len(v) for v in sectors_won.values()),
            "confidence": _confidence(sum(len(v) for v in sectors_won.values()), total),
        },
        {
            "field": "funding stage",
            "drove_pattern": False,
            "detail": "not stored",
            "accounts": 0,
            "confidence": _confidence(0, total),
        },
        {
            "field": "sales cycle length",
            "drove_pattern": False,
            "detail": "not stored; engagement dates are delivery length, not the time to close",
            "accounts": 0,
            "confidence": _confidence(0, total),
        },
        {
            "field": "lost reasons",
            "drove_pattern": False,
            "detail": "no lost deals in the CRM extract",
            "accounts": 0,
            "confidence": "no negative sample",
        },
    ]

    triggers = [
        {
            "label": row["label"],
            "count": row["count"],
            "of": total,
            "accounts": row["accounts"],
            "note": "Present on wins. Not tested against losses, because no lost deals are stored.",
        }
        for row in techs
        if row["count"] >= 2 or (total and row["count"] / total >= 0.3)
    ]

    sector_terms = list(sectors_won)
    for row in sectors:
        if row["count"]:
            sector_terms.append(row["label"])
    for label in summary_hits:
        if label not in sector_terms:
            sector_terms.append(label)

    return {
        "partial": partial or total < 20,
        "win_count": total,
        "wins": [
            {
                "company": item["company"],
                "title": item["title"],
                "dates": item["dates"],
                "location": item["location"],
                "countries": item["countries"],
                "via": item.get("via"),
            }
            for item in wins
        ],
        "summary_quote": summary,
        "countries": countries,
        "sector_terms": sector_terms,
        "tech_rows": techs,
        "missing_fields": missing_fields,
        "missing_country_accounts": missing_country,
        "triggers": triggers,
        "disqualifiers": [],
        "disqualifier_note": (
            "No disqualifiers. Lost deals are not in the extract, so no trait can be shown to be more common in losses than in wins. "
            "Accounts outside the won pattern still score low. That is a weak match, not a loss-based disqualifier."
        ),
        "evidence": evidence,
        "narrative": narrative,
        "summary_line": _summary_line(total, sectors_won, band, countries, techs),
        "example_accounts": [item["company"] for item in wins[:4]],
        "won_sectors": sectors_won,
        "size_band": band,
        "channel": channel,
    }


def _size_sentence(band: dict) -> str:
    if not band.get("known"):
        return "Revenue is not known for any win, so size is not part of the fit."
    return (
        f"Revenue is known for {band['known']} of {band['of']} wins: €{band['smallest_win_eur'] / 1e9:.1f}bn to "
        f"€{band['largest_win_eur'] / 1e9:.0f}bn, median €{band['revenue_median_eur'] / 1e9:.0f}bn. "
        f"Accounts score full size points from €{band['revenue_min_eur'] / 1e9:.1f}bn to "
        f"€{band['revenue_max_eur'] / 1e9:.0f}bn."
    )


def _summary_line(total: int, sectors_won: dict, band: dict, countries: dict, techs: list[dict]) -> str:
    if not total:
        return "No ICP yet: the profile has no delivered end-customer engagements."
    top = [k for k, v in sectors_won.items() if len(v) >= 2] or list(sectors_won)[:2]
    size = f"€{band['smallest_win_eur'] / 1e9:.0f}bn+ revenue" if band.get("known") else "Enterprise-size"
    geo = max(countries.items(), key=lambda kv: len(kv[1]))[0] if countries else "DACH"
    tech = "SAP MDG" if any(row["label"] == "SAP MDG" and row["count"] for row in techs) else "SAP"
    return (
        f"{size} {' and '.join(top) or 'industrial'} groups, mostly {geo}-based, running an {tech} program "
        f"on S/4HANA and buying a lead or solution architect, usually through a staffing agency "
        f"({total} delivered engagements)."
    )
