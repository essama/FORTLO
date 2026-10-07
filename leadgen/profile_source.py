"""End customers live on the profile. This reader does not edit the document."""

from __future__ import annotations

import re
import zipfile
from pathlib import Path

START_HEADINGS = {"berufserfahrung", "professional experience"}
STOP_HEADINGS = {
    "weitere erfahrung",
    "additional experience",
    "ausbildung",
    "education",
    "sprachen",
    "languages",
    "kernkompetenzen",
    "core competencies",
}

_LEFT = re.compile(
    r"^(?P<company>.+?)"
    r"(?:\s*\((?:via|über)\s+(?P<via>[^)]+)\))?"
    r"(?:\s+[—–]\s+|\s+-\s+)"
    r"(?P<location>.+)$",
    re.IGNORECASE,
)

_CITY_COUNTRY = {
    "mulfingen": "Germany",
    "neckarsulm": "Germany",
    "stuttgart": "Germany",
    "hamburg": "Germany",
    "basel": "Switzerland",
    "london": "United Kingdom",
    "düsseldorf": "Germany",
    "dusseldorf": "Germany",
    "singapur": "Singapore",
    "singapore": "Singapore",
    "ingelheim": "Germany",
    "mannheim": "Germany",
    "tokyo": "Japan",
}


def paragraphs_from_docx(path: Path) -> list[str]:
    import xml.etree.ElementTree as ET

    ns = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
    with zipfile.ZipFile(path) as archive:
        xml = archive.read("word/document.xml")

    tree = ET.fromstring(xml)
    lines = []
    for paragraph in tree.iter(ns + "p"):
        text = "".join(node.text or "" for node in paragraph.iter(ns + "t")).strip()
        if text:
            lines.append(text)
    return lines


def _countries(location: str) -> list[str]:
    found = []
    parts = re.split(r"\s*(?:&|/|,| und | and )\s*", location)
    for part in parts:
        key = part.strip().lower()
        country = _CITY_COUNTRY.get(key)
        if country and country not in found:
            found.append(country)
    return found


def _split_engagement(line: str) -> dict | None:
    pieces = [piece.strip() for piece in line.split("|")]
    if len(pieces) < 3:
        return None
    left, title, dates = pieces[0], pieces[1], pieces[2]
    match = _LEFT.match(left)
    if not match:
        return None
    location = match.group("location").strip()
    return {
        "company": re.sub(r"\s+", " ", match.group("company")).strip(" -"),
        "via": (match.group("via") or "").strip() or None,
        "location": location,
        "countries": _countries(location),
        "title": title.strip(),
        "dates": dates.strip(),
        "line": line.strip(),
        "bullets": [],
    }


def parse_engagements(lines: list[str]) -> dict:
    """Return sector summary, won engagements, and excluded early-career lines."""
    summary = ""
    for line in lines:
        lower = line.lower()
        if "pharma" in lower and ("manufacturing" in lower or "fertigung" in lower or "oil" in lower):
            summary = line
            break

    in_experience = False
    after_clients = False
    current = None
    engagements = []
    excluded = []
    for line in lines:
        key = line.strip().lower()
        if key in START_HEADINGS:
            in_experience = True
            continue
        if key in STOP_HEADINGS:
            if current:
                engagements.append(current)
                current = None
            in_experience = False
            after_clients = True
            continue
        if not in_experience:
            if after_clients:
                parsed = _split_engagement(line)
                if parsed:
                    parsed["outcome"] = "excluded_not_a_customer"
                    parsed["retention_basis"] = (
                        "Under Weitere Erfahrung / early employment. Not a closed customer engagement."
                    )
                    excluded.append(parsed)
            continue
        parsed = _split_engagement(line)
        if parsed:
            if current:
                engagements.append(current)
            current = parsed
            continue
        if current is not None:
            current["bullets"].append(line)

    if current:
        engagements.append(current)

    for item in engagements:
        item["outcome"] = "won_retained"
        item["retention_basis"] = (
            "Listed as delivered client work under Berufserfahrung. "
            "No churn or lost-deal field exists, so retention is inferred from a completed engagement."
        )

    return {"summary": summary, "engagements": engagements, "excluded": excluded}


def load_profile(path: Path) -> dict:
    lines = paragraphs_from_docx(Path(path))
    parsed = parse_engagements(lines)
    parsed["path"] = str(path)
    parsed["paragraph_count"] = len(lines)
    return parsed
