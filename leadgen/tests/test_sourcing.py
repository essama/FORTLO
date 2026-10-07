"""Phase A sourcing: profile filters, grok lookalikes/titles/plain search, queue, refuel wiring. Offline."""

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from leadgen import sourcing

ICP = {
    "won_sectors": {
        "manufacturing": ["ebm-papst", "Jungheinrich"],
        "pharma & life sciences": ["Novartis", "Takeda"],
        "it services": ["Bechtle"],
    },
    "size_band": {"revenue_min_eur": 1.15e9, "revenue_max_eur": 135e9, "employees_min": 3700},
    "wins": [{"company": "ebm-papst"}, {"company": "Jungheinrich"}, {"company": "Novartis"},
             {"company": "Takeda"}, {"company": "Bechtle"}],
}


class FakeClient:
    model = "fake-grok"

    def __init__(self, *replies):
        self.replies = list(replies)
        self.calls = []

    def complete(self, payload, system=""):
        self.calls.append((payload, system))
        reply = self.replies.pop(0)
        return reply if isinstance(reply, str) else json.dumps(reply)


class FakeGate:
    pursue_at = 60

    def __init__(self, client=None):
        self.client = client

    def icp(self):
        return ICP

    def prefetch(self, raws, limit=200):
        return {"requested": 0, "classified": 0, "errors": []}

    def judge(self, raw, fortlo_status=None, exclusion_reason=None):
        good = "pharma" in (raw.get("name") or "").lower()
        return {"score": 72 if good else 30, "decision": "pursue" if good else "hold", "reason": "test",
                "sector": "", "industry": raw.get("industry") or "", "ai_inferred": False, "scored_at": "now"}


class TempState(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        patcher = mock.patch.dict(os.environ, {"LEADGEN_STATE_DIR": self.tmp.name})
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(self.tmp.cleanup)


class ProfileSpecTests(unittest.TestCase):
    def test_spec_uses_won_sectors_dach_sap_and_band(self):
        spec = sourcing.profile_spec(ICP)
        self.assertEqual(spec["locations"], ["Germany", "Austria", "Switzerland"])
        self.assertIn("pharmaceuticals", spec["keyword_tags"])
        self.assertIn("industrial machinery", spec["keyword_tags"])
        self.assertNotIn("information technology & services", spec["keyword_tags"])  # IT wins are channel
        self.assertEqual(spec["technologies"], ["sap"])
        self.assertEqual(spec["revenue_min_eur"], 1_150_000_000)
        self.assertEqual(spec["employee_ranges"][0], "2001,5000")  # bucket holding the 3,700 floor

    def test_config_overrides(self):
        spec = sourcing.profile_spec(ICP, {"locations": ["Germany"], "technologies": [], "employee_min": 1000})
        self.assertEqual(spec["locations"], ["Germany"])
        self.assertEqual(spec["technologies"], [])
        self.assertEqual(spec["employee_ranges"][0], "501,1000")

    def test_params_with_and_without_technology(self):
        spec = sourcing.profile_spec(ICP)
        with_tech = sourcing.apollo_params(spec)
        without = sourcing.apollo_params(spec, technologies=False)
        self.assertIn(("currently_using_any_of_technology_uids[]", "sap"), with_tech)
        self.assertNotIn(("currently_using_any_of_technology_uids[]", "sap"), without)
        self.assertIn(("q_organization_keyword_tags[]", "pharmaceuticals"), without)
        self.assertIn(("organization_locations[]", "Switzerland"), without)

    def test_spec_hash_changes_with_filters(self):
        a = sourcing.profile_spec(ICP)
        b = sourcing.profile_spec(ICP, {"locations": ["Germany"]})
        self.assertNotEqual(sourcing.spec_hash(a), sourcing.spec_hash(b))
        self.assertEqual(sourcing.spec_hash(a), sourcing.spec_hash(sourcing.profile_spec(ICP)))


class ParseQueryTests(unittest.TestCase):
    def test_model_spec_is_validated(self):
        defaults = sourcing.profile_spec(ICP)
        client = FakeClient({
            "locations": ["Switzerland"],
            "keyword_tags": ["pharmaceuticals", "medical devices"],
            "technologies": ["sap", "DROP TABLE x"],
            "revenue_min_eur": 9e9, "revenue_max_eur": 2e9,
            "employee_min": 5000,
            "titles": ["Head of MDM"],
            "explain": "Swiss pharma and medtech on SAP.",
        })
        spec = sourcing.parse_query("swiss pharma on sap", client, defaults)
        self.assertEqual(spec["locations"], ["Switzerland"])
        self.assertEqual(spec["technologies"], ["sap"])
        self.assertEqual((spec["revenue_min_eur"], spec["revenue_max_eur"]), (2_000_000_000, 9_000_000_000))
        self.assertEqual(spec["employee_ranges"][0], "2001,5000")
        self.assertEqual(spec["titles"], ["Head of MDM"])

    def test_missing_fields_fall_back_to_profile(self):
        defaults = sourcing.profile_spec(ICP)
        spec = sourcing.parse_query("anything", FakeClient("{}"), defaults)
        self.assertEqual(spec["locations"], defaults["locations"])
        self.assertEqual(spec["technologies"], ["sap"])
        self.assertEqual(spec["revenue_min_eur"], defaults["revenue_min_eur"])

    def test_no_client_is_a_clear_error(self):
        with self.assertRaises(RuntimeError):
            sourcing.parse_query("x", None, sourcing.profile_spec(ICP))


class LookalikeTests(TempState):
    def test_pool_dedupes_excludes_wins_and_only_asks_when_low(self):
        client = FakeClient({"companies": [
            {"name": "Roche Deutschland", "domain": "https://www.roche.de/", "country": "Germany",
             "sector": "pharma", "why": "Like Novartis"},
            {"name": "Novartis", "domain": "novartis.com"},                 # a win: dropped
            {"name": "Roche Deutschland", "domain": "roche.de"},            # duplicate: dropped
            {"name": "KION Group", "domain": "kiongroup.com", "why": "Like Jungheinrich"},
        ]})
        rows = sourcing.lookalikes(ICP, client, want=2, ask=10)
        self.assertEqual([r["name"] for r in rows], ["Roche Deutschland", "KION Group"])
        self.assertEqual(rows[0]["domain"], "roche.de")
        self.assertIn("Novartis", client.calls[0][0]["exclude"])
        again = sourcing.lookalikes(ICP, client, want=2)  # pool is enough: no second call
        self.assertEqual(len(client.calls), 1)
        self.assertEqual(len(again), 2)
        sourcing.mark_lookalikes({"Roche Deutschland": "processed"})
        self.assertEqual([r["name"] for r in sourcing.lookalikes(ICP, None, want=5)], ["KION Group"])

    def test_model_error_keeps_pool(self):
        rows = sourcing.lookalikes(ICP, FakeClient("not json"), want=3)
        self.assertEqual(rows, [])
        pool = json.loads(sourcing.lookalike_path().read_text(encoding="utf-8"))
        self.assertIn("last_error", pool)

    def test_resolve_needs_domain_or_exact_name(self):
        orgs = [{"name": "Roche Diagnostics GmbH", "primary_domain": "roche.de"},
                {"name": "Roche Holding", "primary_domain": "roche.com"}]
        found = sourcing.resolve_in_apollo({"name": "Roche Deutschland", "domain": "roche.de"}, lambda p: orgs)
        self.assertEqual(found["primary_domain"], "roche.de")
        invented = sourcing.resolve_in_apollo({"name": "Rochester Pharma", "domain": "rochester-pharma.de"},
                                              lambda p: orgs)
        self.assertIsNone(invented)
        by_name = sourcing.resolve_in_apollo({"name": "Roche Holding", "domain": ""}, lambda p: orgs)
        self.assertEqual(by_name["primary_domain"], "roche.com")


class TitleTests(TempState):
    def test_variants_are_cached_and_deduped(self):
        client = FakeClient({"titles": ["Leiter Stammdatenmanagement", "Head of ERP", "Head of MDM"]})
        titles = sourcing.title_variants(client, ["Head of ERP"])
        self.assertEqual(titles, ["Leiter Stammdatenmanagement", "Head of MDM"])
        self.assertEqual(sourcing.title_variants(client, ["Head of ERP"]), titles)
        self.assertEqual(len(client.calls), 1)
        merged = sourcing.merge_titles(["Head of ERP", "head of mdm"], titles)
        self.assertEqual(merged, ["Head of ERP", "head of mdm", "Leiter Stammdatenmanagement"])

    def test_no_client_no_cache_is_empty(self):
        self.assertEqual(sourcing.title_variants(None, ["Head of ERP"]), [])


class QueueTests(TempState):
    def test_add_pending_done(self):
        added = sourcing.queue_add([
            {"id": "1", "name": "A", "primary_domain": "www.a.de"},
            {"id": "2", "name": "B", "website_url": "https://b.ch/x"},
            {"id": "3", "name": "A again", "primary_domain": "a.de"},
            {"id": "4", "name": "No domain"},
        ], "search: test")
        self.assertEqual(added, 2)
        self.assertEqual([q["primary_domain"] for q in sourcing.queue_pending()], ["a.de", "b.ch"])
        sourcing.queue_done(["a.de"])
        self.assertEqual([q["primary_domain"] for q in sourcing.queue_pending()], ["b.ch"])


class PlainSearchTests(TempState):
    def test_search_scores_and_drops_tech_when_empty(self):
        client = FakeClient({"locations": ["Germany"], "keyword_tags": ["pharmaceuticals"],
                             "technologies": ["sap"], "explain": "German pharma"})
        calls = []

        def search_fn(params):
            calls.append(params)
            if ("currently_using_any_of_technology_uids[]", "sap") in params:
                return []
            return [{"id": "o1", "name": "Big Pharma AG", "primary_domain": "bigpharma.de", "annual_revenue": 5e9},
                    {"id": "o2", "name": "Bolts GmbH", "primary_domain": "bolts.de"}]

        result = sourcing.plain_search("german pharma", FakeGate(client), search_fn,
                                       known_fn=lambda name, domain: domain == "bolts.de")
        self.assertEqual(len(calls), 2)
        self.assertTrue(result["note"])
        self.assertEqual([r["name"] for r in result["rows"]], ["Big Pharma AG", "Bolts GmbH"])
        self.assertEqual(result["rows"][0]["decision"], "pursue")
        self.assertTrue(result["rows"][1]["known"])
        self.assertEqual(result["rows"][0]["org"]["primary_domain"], "bigpharma.de")


class GroupRuleTests(unittest.TestCase):
    GROUPS = ["Siemens", "Bayer", "Merck", "Phoenix Pharma", "Heidelberg Materials"]

    def test_big_group_matches_whole_words(self):
        self.assertEqual(sourcing.big_group("Siemens Healthineers", self.GROUPS), "Siemens")
        self.assertEqual(sourcing.big_group("Merck KGaA, Darmstadt, Germany", self.GROUPS), "Merck")
        self.assertIsNone(sourcing.big_group("Bayerische Landesbank", self.GROUPS))
        self.assertIsNone(sourcing.big_group("Phoenix Contact", self.GROUPS))
        self.assertEqual(sourcing.big_group("Heidelberg Materials AG", self.GROUPS), "Heidelberg Materials")

    def test_group_key_merges_group_entities(self):
        keys = {sourcing.group_key(n) for n in ("Merck KGaA, Darmstadt, Germany", "Merck Group", "Merck Life Science")}
        self.assertEqual(keys, {"merck"})
        self.assertEqual(sourcing.group_key("Syngenta Group"), sourcing.group_key("Syngenta"))
        self.assertNotEqual(sourcing.group_key("ZEISS Group"), sourcing.group_key("Sika"))


class LookalikeBigGroupTests(TempState):
    def test_big_groups_are_excluded_and_band_is_capped(self):
        client = FakeClient({"companies": [
            {"name": "Siemens Energy", "domain": "siemens-energy.com"},
            {"name": "Pfeiffer Vacuum", "domain": "pfeiffer-vacuum.com"},
        ]})
        rows = sourcing.lookalikes(ICP, client, want=5, exclude=["Siemens"], revenue_max_eur=10e9)
        self.assertEqual([r["name"] for r in rows], ["Pfeiffer Vacuum"])
        payload = client.calls[0][0]
        self.assertEqual(payload["revenue_band_eur"][1], 10e9)
        self.assertIn("Siemens", payload["exclude"])


ROOT = Path(__file__).resolve().parents[2]


@unittest.skipUnless((ROOT / "refuel_apollo.py").exists(), "FORTLO refuel module not next to leadgen")
class RefuelWiringTests(TempState):
    def _run(self, orgs_by_page, people_per_org=2, groups=None):
        if str(ROOT) not in sys.path:
            sys.path.insert(0, str(ROOT))
        import refuel_apollo as ra

        gate = FakeGate(FakeClient({"titles": []}, {"companies": []}))
        people_calls = []

        def org_search(key, cfg, params):
            if dict(params).get("q_organization_name"):
                return []
            return orgs_by_page.get(dict(params).get("page"), [])

        def people(key, domain, org_id, cfg, titles=None):
            people_calls.append(domain)
            return [{"id": f"{domain}-{i}", "title": "Head of Master Data"} for i in range(people_per_org)]

        cfg = ra.load_config(str(ROOT / "config.yaml"))
        cfg["refuel_small_de"]["enabled"] = False
        cfg["leadgen"]["search_tier"]["profile_sourcing"] = True
        cfg["leadgen"].setdefault("signals", {})["enabled"] = False  # Phase B is tested in test_signals
        if groups is not None:
            cfg["too_big_groups"] = groups
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
        return summary, people_calls

    def test_big_groups_are_never_pursued(self):
        orgs = {"1": [{"id": "a", "name": "Siemens Healthineers Pharma", "primary_domain": "siemens-healthineers.com"},
                      {"id": "b", "name": "Pfeiffer Pharma", "primary_domain": "pfeiffer.de"}]}
        summary, people_calls = self._run(orgs, groups=["Siemens"])
        self.assertEqual(people_calls, ["pfeiffer.de"])

    def test_group_cap_limits_contacts_across_group_entities(self):
        orgs = {"1": [{"id": "a", "name": "Merck Pharma KGaA", "primary_domain": "merckgroup.com"},
                      {"id": "b", "name": "Merck Pharma Life Science", "primary_domain": "sigmaaldrich.com"},
                      {"id": "c", "name": "Merck Pharma Healthcare", "primary_domain": "merck-healthcare.com"}]}
        summary, people_calls = self._run(orgs, people_per_org=2, groups=[])
        # 2 + 1 contacts fill the group cap of 3; the third Merck entity is skipped before any people search
        self.assertEqual(people_calls, ["merckgroup.com", "sigmaaldrich.com"])
        self.assertEqual(summary["dry_run_candidates"], 3)
        self.assertEqual(summary["group_capped"], 1)

    def test_dry_run_uses_queue_lookalikes_and_profile_search(self):
        if str(ROOT) not in sys.path:
            sys.path.insert(0, str(ROOT))
        import refuel_apollo as ra

        sourcing.queue_add([{"id": "q1", "name": "Queued Pharma AG", "primary_domain": "queuedpharma.de"}], "test")
        client = FakeClient({"titles": ["Leiter Stammdatenmanagement"]},
                            {"companies": [{"name": "Like Pharma SE", "domain": "likepharma.de", "why": "x"},
                                           {"name": "Made Up AG", "domain": "madeup.de"}]})
        gate = FakeGate(client)
        searches, people_cfgs = [], []

        def org_search(key, cfg, params):
            searches.append(params)
            name = dict(params).get("q_organization_name")
            if name == "Like Pharma SE":
                return [{"id": "l1", "name": "Like Pharma SE", "primary_domain": "likepharma.de"}]
            if name or ("page", "1") not in params:
                return []
            return [{"id": "p1", "name": "Profile Pharma GmbH", "primary_domain": "profilepharma.de"}]

        def people(key, domain, org_id, cfg, titles=None):
            people_cfgs.append((domain, cfg.get("apollo_person_locations"), list(titles or [])))
            return []

        cfg = ra.load_config(str(ROOT / "config.yaml"))
        cfg["refuel_small_de"]["enabled"] = False
        cfg["leadgen"]["search_tier"]["profile_sourcing"] = True
        cfg["leadgen"].setdefault("signals", {})["enabled"] = False  # Phase B is tested in test_signals
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

        self.assertEqual(summary["errors"], [])
        domains = [d for d, _, _ in people_cfgs]
        self.assertEqual(domains, ["queuedpharma.de", "likepharma.de", "profilepharma.de"])
        self.assertEqual(people_cfgs[0][1], ["Germany", "Austria", "Switzerland"])
        self.assertIn("Leiter Stammdatenmanagement", people_cfgs[2][2])
        profile_call = next(p for p in searches if ("page", "1") in p and "q_organization_name" not in dict(p))
        self.assertIn(("q_organization_keyword_tags[]", "pharmaceuticals"), profile_call)
        # dry run leaves the queue and the lookalike pool untouched
        self.assertEqual(len(sourcing.queue_pending()), 1)
        self.assertEqual(len(sourcing.lookalikes(ICP, None, want=5)), 2)


if __name__ == "__main__":
    unittest.main()
