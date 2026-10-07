"""Finance + defense + alcohol name/industry/keyword lists for the FORTE4 ICP.

Hard exclude: banks, Sparkassen, insurers, asset managers, PE/VC,
payment/credit institutions, fintech lenders; Bundeswehr; weapons,
munitions, Wehrtechnik, military vehicles, defence electronics as
core business; alcohol producers/traders (breweries, wine, spirits).
Bechtle group is the size analogue, not a target.
"""

from __future__ import annotations

import re
from typing import Iterable, Optional

FINANCE_NAME_KEYWORDS = [
    "bank",
    "banken",
    "sparkasse",
    "landesbank",
    "volksbank",
    "raiffeisenbank",
    "kredit",
    "hypo",
    "pfandbrief",
    "versicher",
    "insurance",
    "insurer",
    "allianz",
    "munich re",
    "muenchener rueck",
    "münchener rück",
    "talanx",
    "hdi ",
    "ergo ",
    "generali",
    "axa ",
    "zurich",
    "signal iduna",
    "debeka",
    "huk-coburg",
    "r+v",
    "asset management",
    "asset-management",
    "kapitalanlage",
    "vermögensverwalt",
    "vermoegensverwalt",
    "private equity",
    "venture capital",
    "private-equity",
    "pe/vc",
    "fintech",
    "financial services",
    "finanzdienst",
    "capital markets",
    "investment management",
    "wealth management",
    "leasing",
    "finance",
    "financial",
    "trade republic",
    "broker",
    "capital",
    "payment",
    "payments",
    "zahlungsdienst",
    "kreditinstitut",
    "kreditwes",
    "commerzbank",
    "deutsche bank",
    "dz bank",
    "kfw",
    "deutsche börse",
    "deutsche boerse",
    "clearstream",
    "dws ",
    "union investment",
    "deka",
    "flatex",
    "n26",
    "wirecard",
    "paypal",
    "adyen",
    "klarna",
]

DEFENSE_NAME_KEYWORDS = [
    "bundeswehr",
    "wehrtechnik",
    "verteidigung",
    "defence",
    "defense",
    "rüstungs",
    "ruestungs",
    "rüstung",
    "ruestung",
    "waffen",
    "weapon",
    "munition",
    "munitions",
    "military",
    "militär",
    "militaer",
    "kampfpanzer",
    "panzer",
    "kampfjet",
    "kriegs",
    "rheinmetall",
    "hensoldt",
    "krauss-maffei",
    "krauss maffei wegmann",
    "kmw",
    "knds",
    "renault trucks defense",
    "diehl defence",
    "diehl defense",
    "mbda",
    "airbus defence",
    "thyssenkrupp marine",
    "tkms",
    "howaldtswerke",
    "hdw",
    "atlas elektronik",
    "es&s",
    "electronic sensing",
    "renk group",
    "nitrochemie",
    "nammo",
    "orbital",
]

# Matched as word prefixes (so "sekt" does not hit "Sektor", "wein" not "Weinheim").
ALCOHOL_KEYWORDS = [
    "brauerei",
    "brauhaus",
    "brewery",
    "breweries",
    "brewing",
    "brewer",
    "bier",
    "beer",
    "distill",
    "destillerie",
    "spirituos",
    "spirits",
    "whisky",
    "whiskey",
    "vodka",
    "weingut",
    "weinkellerei",
    "winery",
    "wine",
    "sekt ",
    "sektkellerei",
    "schnaps",
    "liquor",
    "likoer",
    "alcohol",
    "alkohol",
    "krombacher",
    "bitburger",
    "warsteiner",
    "radeberger",
    "oettinger",
    "paulaner",
    "veltins",
    "erdinger",
    "holsten",
    "jaegermeister",
    "henkell",
    "rotkaeppchen",
    "diageo",
    "pernod",
    "campari",
    "carlsberg",
    "heineken",
    "anheuser",
    "ab inbev",
]

BECHTLE_NAME_KEYWORDS = [
    "bechtle",
    "solidline",
    "solidpro",
    "planet software",
    "steuteq",
]

BECHTLE_DOMAINS = {
    "bechtle.com",
    "bechtle.de",
    "bechtle-direct.de",
    "bechtle-logistik.de",
    "arp.com",
    "solidline.de",
}

TOO_BIG_NAME_KEYWORDS = [
    "siemens",
    "volkswagen",
    "mercedes-benz",
    "mercedes benz",
    "bmw group",
    "bayerische motoren",
    "deutsche telekom",
    "telekom deutschland",
    "robert bosch",
    "bosch gmbh",
    "schwarz gruppe",
    "schwarz group",
    "schwarz dienstleistung",
    "lidl stiftung",
    "kaufland",
    "deutsche bahn",
    "db ag",
    "basf se",
    "bayer ag",
    "sap se",
    "deutsche post",
    "dhl group",
    "e.on se",
    "eon se",
    "uniper",
    "rwe ag",
    "enbw",
    "lufthansa",
    "allianz se",
    "munich re",
    "münchener rück",
    "deutsche bank",
    "commerzbank",
    "adidas",
    "henkel",
    "fresenius",
    "merck kgaa",
    "boehringer",
    "thyssenkrupp",
    "daimler truck",
    "porsche ag",
    "infineon",
    "siemens energy",
    "siemens healthineers",
    "würth",
    "wuerth",
    "metro ag",
    "edeka",
    "rewe group",
    "rewe-zentral",
    "aldi ",
    "ceconomy",
    "mediamarkt",
    "otto group",
    "tui ag",
    "hapag-lloyd",
    "heidelberg materials",
    "continental ag",
    "schaeffler",
    "zf friedrichshafen",
    "phoenix pharma",
]


def _normalize(text: str) -> str:
    text = (text or "").lower()
    text = text.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("ß", "ss")
    return re.sub(r"\s+", " ", text).strip()


def _hits(haystack: str, keywords: Iterable[str]) -> Optional[str]:
    blob = _normalize(haystack)
    if not blob:
        return None
    for kw in keywords:
        needle = _normalize(kw)
        if needle and needle in blob:
            return kw
    return None


def finance_hit(name: str = "", industry: str = "", extra: str = "") -> Optional[str]:
    return _hits(" ".join([name, industry, extra]), FINANCE_NAME_KEYWORDS)


def defense_hit(name: str = "", industry: str = "", extra: str = "") -> Optional[str]:
    return _hits(" ".join([name, industry, extra]), DEFENSE_NAME_KEYWORDS)


def alcohol_hit(name: str = "", industry: str = "", extra: str = "") -> Optional[str]:
    blob = _normalize(" ".join([name, industry, extra]))
    if not blob:
        return None
    for kw in ALCOHOL_KEYWORDS:
        needle = re.escape(_normalize(kw))
        if not needle:
            continue
        whole_word = kw.endswith(" ")
        pattern = r"(?<![a-z0-9])" + needle + (r"(?![a-z0-9])" if whole_word else "")
        if re.search(pattern, blob):
            return kw.strip()
    return None


def bechtle_hit(name: str = "", domain: str = "") -> Optional[str]:
    domain = (domain or "").lower().strip()
    if domain in BECHTLE_DOMAINS or any(
        domain.endswith("." + d) or domain == d for d in BECHTLE_DOMAINS
    ):
        return f"domain:{domain}"
    return _hits(name, BECHTLE_NAME_KEYWORDS)


def too_big_group_hit(name: str = "", legal_name: str = "") -> Optional[str]:
    return _hits(" ".join([name, legal_name]), TOO_BIG_NAME_KEYWORDS)
