"""Two pages: the ICP flow (/) and plain-English company search (/search).

Uses the standard library so the page runs without the CRM app's web framework.
"""

from __future__ import annotations

import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from jinja2 import Environment, FileSystemLoader, select_autoescape

from leadgen.pipeline import run

TEMPLATES = Environment(
    loader=FileSystemLoader(str(Path(__file__).resolve().parent / "templates")),
    autoescape=select_autoescape(["html"]),
)
_CACHE: dict | None = None
_SEARCH: dict | None = None
_GATE = None


def render(result: dict | None = None) -> str:
    global _CACHE
    if result is None:
        if _CACHE is None:
            _CACHE = run()
        result = _CACHE
    return TEMPLATES.get_template("index.html").render(r=result)


def refresh() -> dict:
    global _CACHE
    _CACHE = run()
    return _CACHE


def _gate():
    global _GATE
    if _GATE is None:
        from leadgen.gate import LeadGate

        _GATE = LeadGate()
    return _GATE


def _apollo():
    """FORTLO's Apollo search and CRM lookup. Only available inside the FORTLO app folder."""
    from enrich_apollo import api_key
    from pipeline_crm import account_has_contacts
    from refuel_apollo import _org_search, load_config

    cfg = load_config()
    key = api_key()
    return (lambda params: _org_search(key, cfg, params)), account_has_contacts, cfg


def render_search(message: str = "") -> str:
    from leadgen import sourcing

    return TEMPLATES.get_template("search.html").render(
        s=_SEARCH, message=message, queue=sourcing.queue_pending()
    )


def search(text: str) -> dict:
    global _SEARCH
    from leadgen import sourcing

    try:
        search_fn, known_fn, cfg = _apollo()
        st_cfg = (cfg.get("leadgen") or {}).get("search_tier") or {}
        _SEARCH = sourcing.plain_search(
            text, _gate(), search_fn, st_cfg=st_cfg, known_fn=known_fn, big_groups=cfg.get("too_big_groups") or []
        )
    except BaseException as exc:  # api_key() raises SystemExit when APOLLO_API_KEY is missing
        if isinstance(exc, KeyboardInterrupt):
            raise
        _SEARCH = {"request": text, "error": f"{exc.__class__.__name__}: {str(exc)[:300]}", "rows": []}
    return _SEARCH


def queue_selected(domains: list[str]) -> int:
    from leadgen import sourcing

    rows = (_SEARCH or {}).get("rows") or []
    picked = [row["org"] for row in rows if row["domain"] in set(domains)]
    return sourcing.queue_add(picked, "search: " + ((_SEARCH or {}).get("request") or ""))


def render_outreach(message: str = "") -> str:
    import json as _json

    import outreach

    con = outreach.connect()
    try:
        events = [dict(r) for r in con.execute(
            "SELECT * FROM outreach_events WHERE kind != 'auto_reply' ORDER BY received_at DESC LIMIT 60")]
        drafts = [dict(r) for r in con.execute(
            "SELECT * FROM outreach_drafts ORDER BY CASE status WHEN 'draft' THEN 0 WHEN 'approved' THEN 1 ELSE 2 END, "
            "created_at DESC LIMIT 100")]
        for row in drafts:
            try:
                row["fact"] = _json.loads(row.get("facts") or "{}")
            except ValueError:
                row["fact"] = {}
        sequence = [dict(r) for r in con.execute(
            "SELECT * FROM outreach_sequence ORDER BY updated_at DESC LIMIT 100")]
        stats = outreach.summary(con)
    finally:
        con.close()
    return TEMPLATES.get_template("outreach.html").render(
        message=message, events=events, drafts=drafts, sequence=sequence, stats=stats, cfg=outreach.load_cfg()
    )


def decide_draft(form: dict) -> str:
    import outreach

    from leadgen import personalize

    contact_id = (form.get("contact_id") or [""])[0]
    action = (form.get("action") or [""])[0]
    text = (form.get("opener") or [None])[0]
    con = outreach.connect()
    try:
        if action == "approve_all":
            ids = [r[0] for r in con.execute("SELECT contact_id FROM outreach_drafts WHERE status='draft'")]
            for cid in ids:
                personalize.decide(con, cid, "approve")
            return f"Approved {len(ids)} drafts"
        return f"{contact_id[:8]}: " + personalize.decide(con, contact_id, action, text)
    finally:
        con.close()


def serve(port: int = 8010) -> None:
    class Handler(BaseHTTPRequestHandler):
        def _send(self, html: str) -> None:
            body = html.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _redirect(self, location: str) -> None:
            self.send_response(303)
            self.send_header("Location", location)
            self.end_headers()

        def do_GET(self):
            url = urlparse(self.path)
            if url.path == "/search":
                message = (parse_qs(url.query).get("msg") or [""])[0][:200]
                self._send(render_search(message))
            elif url.path == "/outreach":
                message = (parse_qs(url.query).get("msg") or [""])[0][:200]
                try:
                    self._send(render_outreach(message))
                except Exception as exc:  # outreach.py lives in the FORTLO folder only
                    self._send(f"<p>Outreach page unavailable: {exc.__class__.__name__}: {exc}</p>")
            else:
                self._send(render())

        def do_POST(self):
            length = int(self.headers.get("Content-Length") or 0)
            form = parse_qs(self.rfile.read(length).decode("utf-8")) if length else {}
            path = self.path.split("?", 1)[0]
            if path == "/refresh":
                refresh()
                self._redirect("/")
            elif path == "/search":
                text = (form.get("q") or [""])[0].strip()
                if text:
                    search(text)
                self._redirect("/search")
            elif path == "/outreach/decide":
                from urllib.parse import quote_plus

                self._redirect("/outreach?msg=" + quote_plus(decide_draft(form)))
            elif path == "/queue":
                added = queue_selected(form.get("domain") or [])
                self._redirect(f"/search?msg=Queued+{added}+companies+for+the+next+refuel")
            else:
                self._redirect("/")

        def log_message(self, fmt: str, *args) -> None:
            return

    # Inside Docker set LEADGEN_HOST=0.0.0.0 and publish the port on the host's 127.0.0.1 only.
    host = os.environ.get("LEADGEN_HOST") or "127.0.0.1"
    server = ThreadingHTTPServer((host, port), Handler)
    print(f"ICP flow at http://{host}:{port}")
    server.serve_forever()
