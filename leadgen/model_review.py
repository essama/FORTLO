"""Ask Claude to write the ICP, persona angles, and outreach.

Scores, evidence counts, and disqualifiers stay in code. The model only
writes prose from that evidence.
"""

from __future__ import annotations

import json
import os
import re
from typing import Protocol

import anthropic
import requests

DEFAULT_MODEL = "claude-opus-5-5"
PROVIDER = "Anthropic"
MAX_TOKENS = 16000
# A refused request is re-run server-side on a model chosen by refusal category.
BETAS = ["server-side-fallback-2026-07-01"]
WEB_SEARCH_TOOL = {"type": "web_search_20260209", "name": "web_search", "max_uses": 5}
MAX_CONTINUATIONS = 4  # web search turns can pause; resend to let them finish

SYSTEM = (
    "You write the customer-profile review for a B2B consultancy. "
    "Use only the JSON facts in the user message. "
    "Do not invent employee counts, revenue, funding stage, sales-cycle length, "
    "lost deals, named people, or disqualifiers. "
    "If lost deals are zero, say that no disqualifier can be set. "
    "The narrative must include the win_count number. "
    "Return one JSON object and no markdown. "
    "Keys: icp_summary (one sentence), narrative (string), personas (object keyed by persona type with "
    "cares_about, message_angle), outreach (object keyed by account name, at most 90 words each, "
    "German if the account is in Germany)."
)


class ModelClient(Protocol):
    model: str

    def complete(self, user_payload: dict, system: str = ...) -> str:
        """Return the model text."""


class ClaudeClient:
    """Claude Messages API client (official anthropic SDK)."""

    def __init__(self, api_key: str, model: str = DEFAULT_MODEL, timeout: float = 300,
                 effort: str = "medium", client=None):
        self.model = model
        self.effort = effort
        self.client = client or anthropic.Anthropic(api_key=api_key, timeout=timeout)

    def complete(self, user_payload: dict, system: str = SYSTEM) -> str:
        response = self._create(system, user_payload)
        return _output_text(response)

    def web_search(self, user_payload: dict, system: str) -> tuple[str, set[str]]:
        """Same call with Claude's server-side web search. Returns (text, URLs the search returned or cited)."""
        response = self._create(system, user_payload, tools=[WEB_SEARCH_TOOL])
        return _output_text(response), _cited_urls(response)

    def _create(self, system: str, user_payload: dict, tools: list | None = None):
        messages = [{"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)}]
        extra = {"tools": tools} if tools else {}
        for _ in range(MAX_CONTINUATIONS + 1):
            response = self.client.beta.messages.create(
                model=self.model,
                max_tokens=MAX_TOKENS,
                system=system,
                messages=messages,
                output_config={"effort": self.effort},
                betas=BETAS,
                fallbacks="default",
                **extra,
            )
            if response.stop_reason != "pause_turn":
                break
            messages = messages + [{"role": "assistant", "content": response.content}]
        if response.stop_reason == "refusal":
            raise ValueError("model refused the request")
        return response


def client_from_env() -> ClaudeClient | None:
    _load_env()
    key = (os.environ.get("ANTHROPIC_API_KEY") or "").strip()
    if not key:
        return None
    model = (os.environ.get("LEADGEN_MODEL") or DEFAULT_MODEL).strip()
    timeout = float(os.environ.get("LEADGEN_MODEL_TIMEOUT") or 300)
    effort = (os.environ.get("LEADGEN_EFFORT") or "medium").strip()
    return ClaudeClient(key, model=model, timeout=timeout, effort=effort)


def apply_review(result: dict, client: ModelClient | None, *, required: bool = True) -> dict:
    """Replace prose fields when a model client is available. Leave scores unchanged."""
    if client is None:
        result["model"] = {
            "used": False,
            "provider": PROVIDER,
            "model": os.environ.get("LEADGEN_MODEL") or DEFAULT_MODEL,
            "status": "skipped",
            "detail": "Set ANTHROPIC_API_KEY to classify blank industries and write the ICP, personas, and outreach with Claude. Scores stay calculated in code.",
        }
        if required:
            result["gaps"].append(result["model"]["detail"])
        return result

    payload = _payload(result)
    try:
        raw = client.complete(payload)
        parsed = _parse_json(raw)
    except (anthropic.APIError, requests.RequestException, ValueError, json.JSONDecodeError, KeyError) as exc:
        result["model"] = {
            "used": False,
            "provider": PROVIDER,
            "model": getattr(client, "model", DEFAULT_MODEL),
            "status": "error",
            "detail": f"Model call failed ({exc.__class__.__name__}). The rule-written text is still shown.",
        }
        result["gaps"].append(result["model"]["detail"])
        return result

    notes = _merge(result, parsed)
    result["model"] = {
        "used": True,
        "provider": PROVIDER,
        "model": getattr(client, "model", DEFAULT_MODEL),
        "status": "ok",
        "detail": "Prose written by the model from the evidence. Scores were not taken from the model."
        + ((" " + " ".join(notes)) if notes else ""),
    }
    return result


def _load_env() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    load_dotenv(os.path.join(root, ".env"))


def _output_text(response) -> str:
    text = "\n".join(
        block.text for block in response.content if block.type == "text" and block.text
    ).strip()
    if not text:
        raise ValueError("response had no output text")
    return text


def _cited_urls(response) -> set[str]:
    urls: set[str] = set()
    for block in response.content:
        if block.type == "web_search_tool_result" and isinstance(block.content, list):
            for result in block.content:
                if getattr(result, "url", None):
                    urls.add(result.url)
        elif block.type == "text":
            for citation in block.citations or []:
                if getattr(citation, "url", None):
                    urls.add(citation.url)
    return urls


def _parse_json(raw: str) -> dict:
    text = raw.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[-1]
        if text.endswith("```"):
            text = text[: text.rfind("```")]
    parsed = json.loads(text.strip())
    if not isinstance(parsed, dict):
        raise ValueError("model JSON was not an object")
    return parsed


def _payload(result: dict) -> dict:
    icp = result["icp"]
    leads = [
        {
            "account": row["account"],
            "score": row["score"],
            "matched_traits": row["matched_traits"],
            "missing_data": row["missing_data"],
            "recommended_persona": row["recommended_persona"],
        }
        for row in result.get("leads") or []
        if row["decision"] == "pursue"
    ][:8]
    return {
        "win_count": icp["win_count"],
        "partial": icp["partial"],
        "missing_fields": icp["missing_fields"],
        "summary_line": icp.get("summary_line") or "",
        "size_band": icp.get("size_band") or {},
        "channel": icp.get("channel") or [],
        "disqualifiers": icp["disqualifiers"],
        "disqualifier_note": icp["disqualifier_note"],
        "summary_quote": icp.get("summary_quote") or "",
        "wins": icp["wins"],
        "evidence": icp["evidence"],
        "triggers": icp["triggers"],
        "relationships": [
            {
                "recruiting_company": row["recruiting_company"],
                "end_customer": row["end_customer"],
                "confirmed": row["confirmed"],
                "confidence": row["confidence"],
            }
            for row in (result["relationships"]["confirmed"] + result["relationships"]["suggested"])
        ],
        "personas": [
            {
                "type": persona["type"],
                "tied_accounts": persona["tied_accounts"],
                "title_patterns": persona["title_patterns"],
                "department": persona["department"],
                "named_people": [person["name"] for person in persona["named_people"]],
                "named_gap": persona["named_gap"],
            }
            for persona in result["personas"]
        ],
        "leads": leads,
    }


def _merge(result: dict, parsed: dict) -> list[str]:
    notes = []
    icp = result["icp"]
    narrative = parsed.get("narrative")
    has_count = isinstance(narrative, str) and re.search(r"(?<!\d)" + str(icp["win_count"]) + r"(?!\d)", narrative)
    if has_count:
        lower = narrative.lower()
        if icp["disqualifiers"] or "lost" in lower or "disqualif" in lower:
            icp["narrative"] = narrative.strip()
        else:
            notes.append("The model narrative left out the empty lost-deal sample, so the rule narrative was kept.")
    else:
        notes.append("The model narrative did not include the win count, so the rule narrative was kept.")

    summary = parsed.get("icp_summary")
    if isinstance(summary, str) and 20 <= len(summary.strip()) <= 400:
        icp["summary_line"] = summary.strip()

    # Objections stay rule-written: there are no lost deals, so anything the model adds is invented.
    by_type = parsed.get("personas") if isinstance(parsed.get("personas"), dict) else {}
    for persona in result["personas"]:
        incoming = by_type.get(persona["type"])
        if not isinstance(incoming, dict):
            continue
        for field in ("cares_about", "message_angle"):
            value = incoming.get(field)
            if isinstance(value, str) and value.strip():
                persona[field] = value.strip()

    outreach = parsed.get("outreach") if isinstance(parsed.get("outreach"), dict) else {}
    allowed = {
        row["account"]
        for row in result.get("leads") or []
        if row["decision"] == "pursue"
    }
    for row in result.get("leads") or []:
        text = outreach.get(row["account"])
        if row["account"] in allowed and isinstance(text, str) and text.strip():
            row["outreach"] = text.strip()
            row["outreach_by"] = "model"
    return notes
