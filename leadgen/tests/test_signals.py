"""Phase B buying signals: job postings, cited web research, scoring, refuel wiring. Offline."""

import json
import os
import sys
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path
from unittest import mock

from leadgen import signals
from leadgen.model_review import _cited_urls
from leadgen.score import score_account

TODAY = date.today().isoformat()
OLD = (date.today() - timedelta(days=400)).isoformat()
ICP = {
    "won_sectors": {"manufacturing": ["ebm-papst", "Jungheinrich"]},
    "size_band": {"revenue_min_eur": 1.15e9, "revenue_max_eur": 135e9, "employees_min": 3700},
    "wins": [{"company": "ebm-papst"}, {"company": "Jungheinrich"}],
    "disqualifiers": [],
}


class TempState(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        patcher = mock.patch.dict(os.environ, {"LEADGEN_STATE_DIR": self.tmp.name})
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(self.tmp.cleanup)


class FakeSearchClient:
    model = "fake-grok"

    def __init__(self, text, cited):
        self.text, self.cited, self.calls = text, cited, 0

    def web_search(self, payload, system=""):
        self.calls += 1
        return self.text, set(self.cited)


class HiringTests(TempState):
    POSTS = [
        {"title": "Ausbildung zum Mechatroniker", "posted_at": TODAY, "country": "Germany", "url": "u0"},
        {"title": "SAP PP Developer", "posted_at": TODAY, "city": "Bruchsal", "country": "Germany", "url": "u1"},
        {"title": "Head of Master Data Management", "posted_at": TODAY, "city": "Hamburg", "country": "Germany",
         "url": "u2"},
        {"title": "Leiter Stammdatenmanagement (m/w/d)", "posted_at": OLD, "country": "Germany", "url": "u3"},
        {"title": "Sales Manager", "posted_at": TODAY, "country": "Germany", "url": "u4"},
        {"title": "Global Lead SAP MDG", "posted_at": TODAY, "city": "Madrid", "country": "Spain", "url": "u5"},
        {"title": "SAP MDG Consultant", "posted_at": TODAY, "city": "Madrid", "country": "Spain", "url": "u10"},
        {"title": "SAP SD Analyst", "posted_at": TODAY, "city": "Pune", "country": "India", "url": "u6"},
        {"title": "sap hcm analyst", "posted_at": TODAY, "city": "Berlin", "country": "Germany", "url": "u7"},
        {"title": "Master Data Specialist", "posted_at": TODAY, "country": "Romania", "url": "u8"},
        {"title": "Mitarbeiter Stammdatenpflege", "posted_at": TODAY, "city": "Augsburg", "country": "Germany",
         "url": "u9"},
    ]

    def test_classifies_and_filters_postings(self):
        found = signals.hiring_from_postings(self.POSTS)
        self.assertEqual(sorted(s["type"] for s in found), ["hiring_md_ops", "hiring_mdg", "hiring_mdg", "hiring_sap"])
        # old, junk, sales, non-DACH generic SAP, SAP HCM and clerical master data abroad dropped;
        # lead-level MDG abroad kept, a consultant abroad dropped; clerical master data in DACH kept as the weak hiring_md_ops
        self.assertEqual({s["source_url"] for s in found}, {"u2", "u5", "u1", "u9"})
        self.assertIn("Hamburg, Germany", found[0]["summary"] + found[1]["summary"])

    def test_old_cache_version_is_ignored(self):
        path = signals.store_path()
        path.write_text(json.dumps({"zf.com": {"hiring": [{"type": "hiring_sap", "summary": "sap hcm"}],
                                               "hiring_at": TODAY}}), encoding="utf-8")
        self.assertEqual(signals.cached("zf.com"), [])
        self.assertEqual(signals.hiring("zf.com", "o", lambda _: []), [])

    def test_hiring_is_cached(self):
        calls = []

        def fetch(org_id):
            calls.append(org_id)
            return self.POSTS

        first = signals.hiring("Kion.com", "org1", fetch)
        second = signals.hiring("kion.com", "org1", fetch)
        self.assertEqual(calls, ["org1"])
        self.assertEqual(first, second)
        self.assertEqual(len(signals.cached("kion.com")), 4)

    def test_no_org_id_no_call(self):
        self.assertEqual(signals.hiring("x.de", None, lambda _: self.fail("called")), [])


class ResearchTests(TempState):
    def test_only_cited_sources_survive(self):
        text = json.dumps({
            "uses_sap": "yes",
            "summary": "Phased S/4HANA rollout.",
            "signals": [
                {"type": "s4_program", "summary": "S/4HANA rollout in finance", "date": "2025-04",
                 "source_url": "https://basware.com/kion-s4/"},
                {"type": "mdg_program", "summary": "invented", "source_url": "https://made-up.example/x"},
                {"type": "hiring_mdg", "summary": "model may not claim jobs", "source_url": "https://basware.com/kion-s4"},
                {"type": "bogus", "summary": "x", "source_url": "https://basware.com/kion-s4"},
            ],
        }).replace("Phased", "Phased [[1]](https://basware.com/kion-s4)")
        client = FakeSearchClient(text, ["https://basware.com/kion-s4"])
        out = signals.research("KION", "kion.com", client)
        self.assertFalse(out["cached"])
        self.assertEqual([s["type"] for s in out["signals"]], ["s4_program"])
        self.assertEqual(out["uses_sap"], "yes")
        again = signals.research("KION", "kion.com", client)
        self.assertTrue(again["cached"])
        self.assertEqual(client.calls, 1)

    def test_prose_wrapped_json_is_parsed_and_failures_are_remembered(self):
        text = 'Here is what I found:\n{"uses_sap": "likely", "signals": [], "summary": "Nothing public."}\nDone.'
        out = signals.research("X", "x.de", FakeSearchClient(text, []))
        self.assertEqual(out["uses_sap"], "likely")

        class Broken(FakeSearchClient):
            def web_search(self, payload, system=""):
                self.calls += 1
                return "no json at all", set()

        broken = Broken("", [])
        with self.assertRaises(ValueError):
            signals.research("Y", "y.de", broken)
        again = signals.research("Y", "y.de", broken)  # remembered for RESEARCH_RETRY_DAYS: no second paid call
        self.assertTrue(again["cached"])
        self.assertEqual(broken.calls, 1)

    def test_client_without_web_search_is_skipped(self):
        out = signals.research("X", "x.de", object())
        self.assertEqual(out["signals"], [])

    def test_cited_urls_from_claude_response(self):
        from types import SimpleNamespace as NS
        response = NS(content=[
            NS(type="web_search_tool_result", content=[NS(type="web_search_result", url="https://a"),
                                                       NS(type="web_search_result", url="https://b")]),
            NS(type="web_search_tool_result", content=NS(type="web_search_tool_result_error", error_code="max_uses_exceeded")),
            NS(type="text", text="t", citations=[NS(type="web_search_result_location", url="https://c")]),
            NS(type="text", text="u", citations=None),
        ])
        self.assertEqual(_cited_urls(response), {"https://a", "https://b", "https://c"})


class ScoringTests(unittest.TestCase):
    BASE = {"name": "Werk GmbH", "domain": "werk.de", "industry": "industrial machinery", "country": "Germany",
            "annual_revenue": 1e8, "employee_count": None, "icp_status": "qualified"}  # size: 0 points, no cap

    def test_points_are_capped_and_one_per_type(self):
        sigs = [{"type": "hiring_mdg", "summary": "a"}, {"type": "hiring_mdg", "summary": "b"},
                {"type": "s4_program", "summary": "c"}, {"type": "m_and_a", "summary": "d"}]
        pts, reasons, sap = signals.points(sigs)
        self.assertEqual(pts, 15)  # 15 + 10 + 5 = 30, capped at 15
        self.assertEqual(len(reasons), 3)
        self.assertTrue(sap)
        self.assertEqual(signals.points([{"type": "m_and_a"}])[2], False)

    def test_signals_lift_score_and_count_as_sap_evidence(self):
        plain = score_account(dict(self.BASE), ICP)
        lifted = score_account({**self.BASE, "signals": [
            {"type": "hiring_mdg", "summary": "Open role: Head of MDG", "date": TODAY, "source_url": "https://jobs/1"}]}, ICP)
        # 50 hold -> 95: +20 documented SAP, +10 sector & SAP together, +15 hiring signal
        self.assertEqual(lifted["score"] - plain["score"], 45)
        self.assertEqual(lifted["signal_points"], 15)
        self.assertIn("https://jobs/1", lifted["reason"])
        self.assertEqual(lifted["decision"], "pursue")

    def test_it_services_firms_get_no_signal_points(self):
        firm = {**self.BASE, "name": "SAP Partner GmbH", "industry": "it services"}
        sig = [{"type": "hiring_mdg", "summary": "Senior Consultant SAP MDG", "source_url": "https://jobs/2"}]
        plain = score_account(dict(firm), ICP)
        lifted = score_account({**firm, "signals": sig}, ICP)
        self.assertEqual(lifted["score"], plain["score"])
        self.assertIn("competitor", lifted["reason"])


ROOT = Path(__file__).resolve().parents[2]


class FakeGate:
    pursue_at = 60

    def __init__(self):
        self.client = object()
        self.collected = []

    def icp(self):
        return {"won_sectors": {"manufacturing": []}, "size_band": {}, "wins": []}

    def prefetch(self, raws, limit=200):
        return {"requested": 0, "classified": 0, "errors": []}

    def collect_signals(self, raw, *, fetch_jobs=None, research=False):
        self.collected.append((raw["domain"], raw.get("apollo_org_id"), bool(fetch_jobs), research))
        hit = raw["domain"] == "hiring.de"
        return {"hiring": [{"type": "hiring_mdg", "summary": "Head of MDG"}] if hit else [], "research": [],
                "research_new": research, "research_tried": research, "errors": []}

    def judge(self, raw, fortlo_status=None, exclusion_reason=None):
        seen = any(d == raw["domain"] for d, *_ in self.collected)
        score = 52 + (15 if seen and raw["domain"] == "hiring.de" else 0)
        return {"score": score, "decision": "pursue" if score >= 60 else "hold", "reason": "r", "sector": "",
                "industry": "", "ai_inferred": False, "scored_at": "now", "signal_points": 0, "signals": []}


@unittest.skipUnless((ROOT / "refuel_apollo.py").exists(), "FORTLO refuel module not next to leadgen")
class RefuelSignalTests(TempState):
    def test_hiring_tier_and_signals_flip_hold_to_pursue(self):
        if str(ROOT) not in sys.path:
            sys.path.insert(0, str(ROOT))
        import refuel_apollo as ra

        gate = FakeGate()
        searches, people_calls = [], []

        def org_search(key, cfg, params):
            searches.append(params)
            p = dict(params)
            if p.get("q_organization_name") or p.get("page") != "1":
                return []
            if "q_organization_job_titles[]" in p:
                return [{"id": "acct1", "organization_id": "org1", "name": "Hiring Werk", "primary_domain": "hiring.de"}]
            return [{"id": "org2", "name": "Quiet Werk", "primary_domain": "quiet.de"}]

        def people(key, domain, org_id, cfg, titles=None):
            people_calls.append(domain)
            return [{"id": "p1", "title": "Head of Master Data"}]

        cfg = ra.load_config(str(ROOT / "config.yaml"))
        cfg["refuel_small_de"]["enabled"] = False
        cfg["leadgen"]["search_tier"]["profile_sourcing"] = True
        cfg["leadgen"]["search_tier"]["lookalikes"] = False
        cfg["leadgen"]["search_tier"]["ai_titles"] = False
        cfg["leadgen"]["signals"].update({"enabled": True, "research_per_run": 1})
        with mock.patch.object(ra, "load_config", return_value=cfg), \
             mock.patch.object(ra, "api_key", return_value="k"), \
             mock.patch.object(ra, "_leadgen_gate", return_value=gate), \
             mock.patch.object(ra, "search_bechtle_band_orgs", return_value=[]), \
             mock.patch.object(ra, "_org_search", side_effect=org_search), \
             mock.patch.object(ra, "people_search", side_effect=people), \
             mock.patch.object(ra, "account_has_contacts", return_value=False), \
             mock.patch.object(ra, "_load_state", return_value={}), \
             mock.patch.object(ra, "_save_state"), \
             mock.patch.object(ra, "log"):
            summary = ra.refuel_head_of_erp(100, dry_run=True)

        hiring_search = next(p for p in searches if "q_organization_job_titles[]" in dict(p))
        self.assertIn(("q_organization_job_titles[]", "SAP MDG"), hiring_search)
        # job postings are looked up by the Apollo organization id, not the account id
        self.assertEqual(gate.collected[0][:2], ("hiring.de", "org1"))
        self.assertEqual(people_calls, ["hiring.de"])          # 52 + hiring signal -> pursue; quiet.de stays held
        self.assertEqual(summary["signals"]["researched"], 1)    # research budget of 1 respected
        self.assertEqual(summary["signals"]["with_signals"], 1)
        self.assertEqual([c[3] for c in gate.collected], [True, False])


if __name__ == "__main__":
    unittest.main()
