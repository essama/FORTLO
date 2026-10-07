import hashlib
import json
import os
import sqlite3
import tempfile
import unittest
from collections import Counter
from pathlib import Path
from unittest import mock

from leadgen import ai_enrich
from leadgen.adapter import SqliteCrmAdapter
from leadgen.app import render
from leadgen.crm_sync import write_scores
from leadgen.gate import LeadGate
from leadgen.model_review import ClaudeClient, apply_review
from leadgen.pipeline import run
from leadgen.score import PURSUE_AT, score_account

PROFILE = Path(os.environ.get("LEADGEN_PROFILE") or r"C:\Users\essam\Desktop\Essam Azzam\Essam\CVs\Profil.docx")
CV = Path(os.environ.get("LEADGEN_CV") or r"C:\Users\essam\Downloads\Essam_Azzam_CV.docx")
ROOT = Path(__file__).resolve().parents[1]

WON_ICP = {
    "won_sectors": {"manufacturing": ["ebm-papst", "Jungheinrich"], "pharma & life sciences": ["Novartis", "Takeda"]},
    "size_band": {"revenue_min_eur": 1.15e9, "revenue_max_eur": 135e9, "employees_min": 3700},
    "wins": [{"company": "Bosch (ETAS GmbH)"}, {"company": "Bechtle"}],
    "example_accounts": ["ebm-papst"],
    "disqualifiers": [],
}


@unittest.skipUnless(PROFILE.exists(), "profile docx not available on this machine")
class LeadgenTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.result = run(profile_path=PROFILE, cv_path=CV, lead_limit=30, use_model=False)

    def test_wins_exclude_early_career_and_abis(self):
        companies = [row["company"] for row in self.result["icp"]["wins"]]
        self.assertGreaterEqual(len(companies), 10)
        self.assertIn("ebm-papst", companies)
        self.assertNotIn("SAP", companies)
        self.assertNotIn("IBM", companies)
        self.assertTrue(any("Abis" in row["company_name"] for row in self.result["relationships"]["unresolved_clients"]))
        self.assertNotIn("Abis", " ".join(row["agency"] for row in self.result["icp"]["channel"]))

    def test_confirmed_relationship_and_cv(self):
        confirmed = self.result["relationships"]["confirmed"]
        softup = next(row for row in confirmed if "softup" in row["recruiting_company"].lower() and row["end_customer"] == "ebm-papst")
        self.assertGreaterEqual(softup["confidence"], 0.9)
        self.assertGreater(softup["billing"]["net_eur"], 0)
        if CV.exists():
            self.assertEqual(self.result["cv_check"]["missing_on_cv"], [])

    def test_icp_has_size_band_sectors_and_channel(self):
        icp = self.result["icp"]
        self.assertTrue(icp["partial"])
        self.assertEqual(icp["disqualifiers"], [])
        self.assertIn("Lost deals", icp["narrative"])
        self.assertGreaterEqual(icp["size_band"]["known"], 8)
        self.assertLess(icp["size_band"]["smallest_win_eur"], icp["size_band"]["largest_win_eur"])
        self.assertIn("manufacturing", icp["won_sectors"])
        self.assertIn("pharma & life sciences", icp["won_sectors"])
        self.assertIn(str(icp["win_count"]), icp["summary_line"])
        self.assertTrue(any("softup" in row["agency"].lower() for row in icp["channel"]))

    def test_personas_include_agency_and_named_decision_makers(self):
        types = [row["type"] for row in self.result["personas"]]
        self.assertEqual(types, ["Decision-maker", "Internal champion", "Agency account manager"])
        people = {row["name"] for row in self.result["personas"][0]["named_people"]}
        for name in ("Holger Trompeter", "Manvi Goel", "Jimmy Tan", "Yannic Bahr", "Gernot Hegen", "Franz Kreher", "Eftichia Pilgou"):
            self.assertIn(name, people)
        self.assertNotIn("Aldo Alicka", people)
        agency_people = {row["name"] for row in self.result["personas"][2]["named_people"]}
        self.assertIn("Aldo Alicka", agency_people)
        self.assertIn("Markus Reitberger", agency_people)
        self.assertEqual(self.result["sources"]["crm_write"], "none")
        self.assertNotIn("@", json.dumps(self.result["historical_contacts"]))

    def test_scores_separate_the_accounts(self):
        ranked = self.result["leads"]
        self.assertTrue(ranked)
        pursue = self.result["pursue_count"]
        self.assertLess(pursue, self.result["lead_count"] * 0.2, "most accounts should not be marked pursue")
        biggest_tie = Counter(row["score"] for row in ranked).most_common(1)[0][1]
        self.assertLess(biggest_tie, len(ranked))

    def test_probes(self):
        bank = next(row for row in self.result["probes"] if row["account"] == "Probe Bank AG")
        plant = next(row for row in self.result["probes"] if row["account"] == "Probe Werke GmbH")
        self.assertLess(bank["score"], 40)
        self.assertEqual(bank["decision"], "hold")
        self.assertGreaterEqual(plant["score"], 80)
        self.assertEqual(plant["decision"], "pursue")
        self.assertTrue(plant["outreach"])

    def test_page_renders(self):
        html = render(self.result)
        for text in ("Probe Bank AG", "ebm-papst", "SoftUp", "Agency account manager", "Channel", self.result["icp"]["summary_line"][:30]):
            self.assertIn(text, html)
        self.assertEqual(self.result["model"]["used"], False)


class ScoreTests(unittest.TestCase):
    def test_sap_in_suggested_titles_is_not_evidence(self):
        row = score_account(
            {"name": "Werk AG", "industry": "", "country": "Germany", "suggested_titles_json": '["Head of SAP"]'},
            WON_ICP,
        )
        self.assertNotIn("SAP / S/4 / MDG is documented on the account", row["matched_traits"])
        self.assertEqual(row["decision"], "hold")

    def test_fortlo_size_band_does_not_override_won_range(self):
        # FORTLO says too_big; the wins include groups this size, so leadgen still pursues.
        row = score_account(
            {"name": "Pharma Gruppe AG", "industry": "pharmaceuticals", "country": "Germany",
             "annual_revenue": 25e9, "icp_status": "too_big"},
            WON_ICP,
        )
        self.assertEqual(row["decision"], "pursue")
        far = score_account(
            {"name": "Mega Auto AG", "industry": "automotive", "country": "Germany", "annual_revenue": 450e9},
            WON_ICP,
        )
        self.assertEqual(far["decision"], "hold")

    def test_excluded_sector_and_bank_are_capped(self):
        for account in (
            {"name": "Werk AG", "industry": "manufacturing", "country": "Germany", "annual_revenue": 5e9,
             "icp_status": "excluded_sector", "score_notes": "SAP MDG"},
            {"name": "Geld Bank AG", "industry": "banking", "country": "Germany", "annual_revenue": 5e9, "score_notes": "SAP"},
        ):
            self.assertLessEqual(score_account(account, WON_ICP)["score"], 10)

    def test_ai_sector_counts_less_and_is_tagged(self):
        base = {"name": "Unbekannt GmbH", "industry": "", "country": "Germany", "annual_revenue": 3e9}
        plain = score_account({**base, "industry": "machinery"}, WON_ICP)
        ai = score_account({**base, "ai": {"sector": "manufacturing", "uses_sap": "unknown", "industry": "machinery"}}, WON_ICP)
        self.assertLess(ai["score"], plain["score"])
        self.assertTrue(ai["ai_inferred"])
        self.assertIn("AI-inferred", ai["reason"])

    def test_disqualifier_flag_when_a_rule_exists(self):
        icp = {**WON_ICP, "disqualifiers": [{"field": "industry", "value": "manufacturing", "label": "test rule"}]}
        row = score_account(
            {"name": "Werk AG", "industry": "manufacturing", "country": "Germany", "icp_status": "qualified", "score_notes": "SAP MDG"},
            icp,
        )
        self.assertTrue(row["fails_disqualifier"])
        self.assertLessEqual(row["score"], 25)

    def test_same_end_customer_is_held_back_and_a_sister_company_is_not(self):
        bechtle = score_account({"name": "Bechtle AG", "industry": "it", "country": "Germany", "score_notes": "SAP"}, WON_ICP)
        sister = score_account(
            {"name": "Bosch Engineering GmbH", "industry": "manufacturing", "country": "Germany", "score_notes": "SAP MDG"},
            WON_ICP,
        )
        self.assertLessEqual(bechtle["score"], 15)
        self.assertEqual(bechtle["decision"], "hold")
        self.assertGreaterEqual(sister["score"], PURSUE_AT)
        self.assertIn("Bosch", sister["next_action"])


class FakeModel:
    model = "grok-test"

    def __init__(self, reply):
        self.reply = reply
        self.calls = []

    def complete(self, payload, system=None):
        self.calls.append((payload, system))
        return self.reply(payload) if callable(self.reply) else self.reply


class AiEnrichTests(unittest.TestCase):
    def test_classifies_blank_industries_once_and_caches(self):
        def reply(payload):
            return json.dumps({"companies": [
                {"key": c["key"], "industry": "machinery", "sector": "manufacturing", "uses_sap": "likely", "hq_country": "Germany"}
                for c in payload["companies"]
            ] + [{"key": "not-asked.example", "sector": "manufacturing"}]})

        accounts = [
            {"name": "A GmbH", "domain": "a.example", "industry": ""},
            {"name": "B GmbH", "domain": "b.example", "industry": "chemicals"},
        ]
        with tempfile.TemporaryDirectory() as tmp, mock.patch.dict(os.environ, {"LEADGEN_STATE_DIR": tmp}):
            fake = FakeModel(reply)
            summary = ai_enrich.enrich(accounts, fake)
            self.assertEqual(summary["classified"], 1)
            ai_enrich.enrich(accounts, fake)
            self.assertEqual(len(fake.calls), 1, "cached accounts must not be sent again")
            applied = ai_enrich.apply(accounts)
            self.assertEqual(applied[0]["ai"]["sector"], "manufacturing")
            self.assertNotIn("ai", applied[1])
            self.assertNotIn("not-asked.example", ai_enrich.load_cache())

    def test_bad_model_output_is_reported_not_raised(self):
        with tempfile.TemporaryDirectory() as tmp, mock.patch.dict(os.environ, {"LEADGEN_STATE_DIR": tmp}):
            summary = ai_enrich.enrich([{"name": "A", "domain": "a.example"}], FakeModel("not json"))
            self.assertEqual(summary["classified"], 0)
            self.assertTrue(summary["errors"])

    def test_unknown_sector_values_are_dropped(self):
        self.assertEqual(ai_enrich._clean({"sector": "crypto", "uses_sap": "definitely"}),
                         {"industry": "", "sector": "unknown", "uses_sap": "unknown", "hq_country": "", "note": ""})


class GateTests(unittest.TestCase):
    def test_gate_scores_an_apollo_org_with_cached_icp(self):
        gate = LeadGate(use_model=False)
        gate._icp, gate._built_at = WON_ICP, 10 ** 12
        good = gate.judge({"name": "Maschinen AG", "domain": "m.example", "industry": "machinery",
                           "country": "Germany", "revenue_eur": 3e9, "employees": 9000}, fortlo_status="too_big")
        small = gate.judge({"name": "Klein GmbH", "domain": "k.example", "industry": "machinery",
                            "country": "Germany", "revenue_eur": 40e6, "employees": 250}, fortlo_status="qualified")
        self.assertEqual(good["decision"], "pursue")
        self.assertEqual(small["decision"], "hold")
        self.assertIn("below", small["reason"])


class CrmSyncTests(unittest.TestCase):
    def test_writes_only_leadgen_columns(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "fortlo.db"
            con = sqlite3.connect(db)
            con.execute("CREATE TABLE customers (id TEXT, name TEXT, domain TEXT, icp_status TEXT, in_email_campaign INTEGER)")
            con.execute("INSERT INTO customers VALUES ('c1', 'Werk AG', 'werk.example', 'qualified', 1)")
            con.commit()
            con.close()
            accounts = [{"id": "c1", "name": "Werk AG", "domain": "werk.example"}]
            ranked = [{"account": "Werk AG", "domain": "werk.example", "score": 72, "decision": "pursue", "reason": "r"}]
            self.assertEqual(write_scores(db, ranked, accounts, backup=False)["updated"], 1)
            con = sqlite3.connect(db)
            row = con.execute(
                "SELECT icp_status, in_email_campaign, leadgen_score, leadgen_decision FROM customers"
            ).fetchone()
            con.close()
            self.assertEqual(row, ("qualified", 1, 72, "pursue"))


class SparseTests(unittest.TestCase):
    def test_sparse_profile_does_not_fail(self):
        with tempfile.TemporaryDirectory() as tmp:
            empty = Path(tmp) / "empty.json"
            empty.write_text('{"clients": [], "invoices": []}', encoding="utf-8")
            result = run(
                profile_path=Path(tmp) / "missing.docx",
                cv_path=Path(tmp) / "missing-cv.docx",
                crm_snapshot=empty,
                accounts_snapshot=Path(tmp) / "no-accounts.json",
                use_model=False,
            )
        self.assertEqual(result["icp"]["win_count"], 0)
        self.assertTrue(result["gaps"])
        self.assertEqual(len(result["personas"]), 3)
        self.assertLess(result["probes"][0]["score"], 40)

    def test_sqlite_adapter_does_not_write(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "crm.sqlite"
            con = sqlite3.connect(db_path)
            con.execute("CREATE TABLE clients (id TEXT, company_name TEXT, name TEXT, city TEXT, country TEXT, email TEXT)")
            con.execute("INSERT INTO clients VALUES ('1', 'SoftUp Technologies GmbH', 'Aldo Alicka', 'Munich', 'Germany', 'a@softup.co')")
            con.execute(
                "CREATE TABLE outgoing_invoices (id TEXT, inv_nr TEXT, date TEXT, net_price REAL, client_id TEXT, item TEXT, item_count REAL, price REAL)"
            )
            con.execute("INSERT INTO outgoing_invoices VALUES ('1', '2026-1004', '2026-07-03', 5762, '1', 'SAP MDG', 1, 5762)")
            con.commit()
            con.close()
            digest = hashlib.sha256(db_path.read_bytes()).hexdigest()
            adapter = SqliteCrmAdapter(db_path)
            self.assertEqual(adapter.recruiting_companies()[0]["company_name"], "SoftUp Technologies GmbH")
            self.assertEqual(adapter.invoices()[0]["net_price"], 5762)
            self.assertEqual(hashlib.sha256(db_path.read_bytes()).hexdigest(), digest)


class ModelReviewTests(unittest.TestCase):
    def test_model_prose_replaces_copy_and_leaves_the_score(self):
        result = _tiny_result()
        fake = FakeModel(json.dumps({
            "icp_summary": "Large DACH manufacturers running SAP MDG on S/4HANA.",
            "narrative": "Reviewed 2 wins. Lost deals are absent, so there is no disqualifier.",
            "personas": {"Decision-maker": {"cares_about": "A governed golden record.", "objections": "They hate change.",
                                            "message_angle": "Open with the consolidation delivered."}},
            "outreach": {"Werk AG": "Werk AG matches manufacturing and SAP MDG."},
        }))
        apply_review(result, fake)
        self.assertTrue(result["model"]["used"])
        self.assertIn("Reviewed 2 wins", result["icp"]["narrative"])
        self.assertEqual(result["icp"]["summary_line"], "Large DACH manufacturers running SAP MDG on S/4HANA.")
        self.assertEqual(result["leads"][0]["score"], 90)
        self.assertEqual(result["leads"][0]["outreach_by"], "model")
        self.assertEqual(result["personas"][0]["objections"], "No objection text.", "objections are never model-written")

    def test_win_count_must_appear_as_a_number(self):
        result = _tiny_result()
        kept = result["icp"]["narrative"]
        apply_review(result, FakeModel(json.dumps({"narrative": "Reviewed 12 wins; lost deals none."})))
        self.assertEqual(result["icp"]["narrative"], kept)

    def test_client_sends_the_documented_messages_call(self):
        from types import SimpleNamespace
        captured = {}

        class Messages:
            def create(self, **kwargs):
                captured.update(kwargs)
                text = SimpleNamespace(type="text", text="{\"narrative\": \"ok\"}", citations=None)
                return SimpleNamespace(stop_reason="end_turn", content=[text])

        fake = SimpleNamespace(beta=SimpleNamespace(messages=Messages()))
        client = ClaudeClient("test-key", client=fake)
        self.assertEqual(client.complete({"win_count": 2}, system="classify"), "{\"narrative\": \"ok\"}")
        self.assertEqual(captured["model"], "claude-opus-5-5")
        self.assertEqual(captured["system"], "classify")
        self.assertEqual(json.loads(captured["messages"][0]["content"]), {"win_count": 2})
        self.assertNotIn("tools", captured)


def _tiny_result():
    return {
        "gaps": [],
        "icp": {
            "win_count": 2, "partial": True, "narrative": "Rule narrative for 2 wins. Lost deals: none.",
            "missing_fields": [], "disqualifiers": [], "disqualifier_note": "", "summary_quote": "",
            "wins": [{"company": "ebm-papst"}], "evidence": [], "triggers": [],
        },
        "relationships": {"confirmed": [], "suggested": []},
        "personas": [{
            "type": "Decision-maker", "tied_accounts": ["ebm-papst"], "title_patterns": "", "department": "",
            "named_people": [], "named_gap": "", "cares_about": "rule", "objections": "No objection text.",
            "message_angle": "rule angle",
        }],
        "leads": [{
            "account": "Werk AG", "score": 90, "decision": "pursue", "matched_traits": [], "missing_data": [],
            "recommended_persona": "Decision-maker", "outreach": "rule outreach",
        }],
    }


if __name__ == "__main__":
    unittest.main()
