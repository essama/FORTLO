"""Phase C outreach: reply classification, stops, follow-up sequence, openers. Offline, temp databases."""

import json
import os
import sqlite3
import sys
import tempfile
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    import outreach
except ImportError:  # leadgen copied without the FORTLO folder
    outreach = None

from leadgen import personalize, signals

SUBJECT = "SAP MDG Support for Beiersdorf"
CFG = {
    "sequence": {"enabled": True, "start_date": "2026-01-01", "max_per_day": 20, "steps": [
        {"after_days": 5, "body": "erste Nachfrage", "optout": "Abmelden"},
        {"after_days": 12, "body": "letzte Nachricht", "optout": "Abmelden"},
    ]},
    "openers": {"enabled": True, "require_approval": True},
}


def msg(sender, subject, preview, mid=None, headers=None):
    return {"id": mid or f"{sender}|{subject}", "from": {"emailAddress": {"address": sender}}, "subject": subject,
            "bodyPreview": preview, "receivedDateTime": "2026-09-24T10:00:00Z",
            "internetMessageHeaders": headers or []}


@unittest.skipIf(outreach is None, "outreach.py not next to leadgen")
class OutreachDb(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = os.path.join(self.tmp.name, "fortlo.db")
        con = sqlite3.connect(self.path)
        con.executescript(
            """
            CREATE TABLE customers (id TEXT PRIMARY KEY, name TEXT, domain TEXT, in_email_campaign INT,
                                    icp_status TEXT, last_client_status TEXT, client_phase TEXT);
            CREATE TABLE contacts (id TEXT PRIMARY KEY, customer_id TEXT, email TEXT, first_name TEXT,
                                   job_title TEXT, created_at TEXT);
            CREATE TABLE email_logs (id TEXT, contact_id TEXT, customer_id TEXT, email_address TEXT,
                                     subject TEXT, sent_at TEXT);
            """
        )
        con.execute("INSERT INTO customers VALUES ('cu1','Beiersdorf','beiersdorf.com',1,'qualified',NULL,'contacted')")
        con.execute("INSERT INTO email_logs VALUES ('l1','c1','cu1','anna@beiersdorf.com',?, '2026-09-20')", (SUBJECT,))
        con.execute("INSERT INTO email_logs VALUES ('l2','c2','cu2','ben@krones.com','SAP MDG Support for Krones','2026-09-20')")
        con.commit()
        con.close()
        self.con = outreach.connect(self.path)
        self.addCleanup(self.con.close)


class ClassifyTests(OutreachDb):
    def setUp(self):
        super().setUp()
        self.emails, self.domains = outreach.contacted(self.con)
        self.subjects = outreach.sent_subjects(self.con)

    def kind(self, m):
        hit = outreach.classify(m, self.emails, self.domains, self.subjects)
        return hit[0] if hit else None

    def test_real_inbox_cases(self):
        self.assertEqual(self.kind(msg("anna@beiersdorf.com", "AW: " + SUBJECT, "Hallo Herr Azzam, wir kommen gern auf Sie zu")), "reply")
        self.assertEqual(self.kind(msg("ben@krones.com", "Automatische Antwort: SAP MDG Support for Krones", "ich bin aktuell abwesend")), "auto_reply")
        self.assertEqual(self.kind(msg("ben@krones.com", "Re: SAP MDG Support for Krones", "I am not in the office before October 7th.")), "auto_reply")
        self.assertEqual(self.kind(msg("ben@krones.com", "Automatische Antwort: SAP MDG Support for Krones", "I am retired. Please contact my successor")), "left")
        self.assertEqual(self.kind(msg("x@krones.com", "Out of office Re: SAP MDG Support for Krones", "This user is no longer with the organisation.")), "left")
        self.assertEqual(self.kind(msg("anna@beiersdorf.com", "AW: " + SUBJECT, "Bitte keine weiteren E-Mails.")), "unsubscribe")
        self.assertEqual(self.kind(msg("postmaster@beiersdorf.com", "Undeliverable: " + SUBJECT, "Delivery has failed to anna@beiersdorf.com")), "bounce")
        self.assertIsNone(self.kind(msg("postmaster@spark.net", "Delivery Status Notification (Delay)", "Delivery incomplete anna@beiersdorf.com")))
        # same company, different thread (e.g. a freelance project): not an outreach reply
        self.assertIsNone(self.kind(msg("karin@beiersdorf.com", "Re: Profile MDG civillent", "09:00 Uhr passt")))
        self.assertIsNone(self.kind(msg("someone@other.de", "Hello", "hi")))
        self.assertEqual(self.kind(msg("ben@krones.com", "Re: x", "y", headers=[{"name": "Auto-Submitted", "value": "auto-replied"}])), None)

    def test_header_auto_submitted(self):
        m = msg("ben@krones.com", "Re: SAP MDG Support for Krones", "Danke", headers=[{"name": "Auto-Submitted", "value": "auto-replied"}])
        self.assertEqual(self.kind(m), "auto_reply")


class StopTests(OutreachDb):
    def enroll(self, email, contact="c1", customer="cu1", when="2026-09-20 08:00:00"):
        row = {"email": email, "contact_id": contact, "customer_id": customer, "first_name": "Anna",
               "organization_name": "Beiersdorf"}
        return outreach.enroll(self.con, CFG, row, SUBJECT, when=datetime.strptime(when, "%Y-%m-%d %H:%M:%S"))

    def test_reply_stops_person_and_company_and_marks_crm(self):
        self.enroll("anna@beiersdorf.com")
        self.enroll("carl@beiersdorf.com", contact="c3")
        self.enroll("ben@krones.com", contact="c2", customer="cu2")
        events = outreach.scan_replies(self.con, [msg("anna@beiersdorf.com", "AW: " + SUBJECT, "Gern, rufen Sie an")])
        outreach.apply_crm_status(self.con, events)
        status = {r["email"]: r["status"] for r in self.con.execute("SELECT * FROM outreach_sequence")}
        self.assertEqual(status, {"anna@beiersdorf.com": "replied", "carl@beiersdorf.com": "stopped", "ben@krones.com": "active"})
        self.assertEqual(outreach.is_blocked(self.con, "new.person@beiersdorf.com"), "someone at this company replied or opted out")
        self.assertIsNone(outreach.is_blocked(self.con, "ben@krones.com"))
        crm = self.con.execute("SELECT client_phase, last_client_status FROM customers WHERE id='cu1'").fetchone()
        self.assertEqual(crm[0], "replied")
        # the same message is never processed twice
        self.assertEqual(outreach.scan_replies(self.con, [msg("anna@beiersdorf.com", "AW: " + SUBJECT, "Gern")]), [])

    def test_auto_reply_does_not_stop(self):
        self.enroll("ben@krones.com", contact="c2", customer="cu2")
        outreach.scan_replies(self.con, [msg("ben@krones.com", "Automatische Antwort: SAP MDG Support for Krones", "abwesend")])
        self.assertIsNone(outreach.is_blocked(self.con, "ben@krones.com"))
        self.assertEqual(self.con.execute("SELECT status FROM outreach_sequence").fetchone()[0], "active")

    def test_bounce_blocks_only_that_address(self):
        outreach.scan_replies(self.con, [msg("postmaster@krones.com", "Undeliverable: x", "failed to ben@krones.com")])
        self.assertTrue(outreach.is_blocked(self.con, "ben@krones.com"))
        self.assertIsNone(outreach.is_blocked(self.con, "eva@krones.com"))


class SequenceTests(OutreachDb):
    def test_enroll_respects_switch_and_start_date(self):
        row = {"email": "a@x.de", "contact_id": "0000-AB", "customer_id": "cu-1"}
        self.assertFalse(outreach.enroll(self.con, {"sequence": {"enabled": False}}, row, "s"))
        self.assertFalse(outreach.enroll(self.con, CFG, row, "s", when=datetime(2025, 12, 31)))
        self.assertTrue(outreach.enroll(self.con, CFG, row, "s", when=datetime(2026, 9, 1)))
        stored = self.con.execute("SELECT contact_id, customer_id FROM outreach_sequence").fetchone()
        self.assertEqual(tuple(stored), ("0000ab", "cu1"))  # uuid hex, no dashes, lower case

    def test_steps_are_due_on_time_and_end(self):
        row = {"email": "anna@beiersdorf.com", "contact_id": "c1", "customer_id": "cu1", "first_name": "Anna",
               "organization_name": "Beiersdorf"}
        outreach.enroll(self.con, CFG, row, SUBJECT, when=datetime(2026, 9, 20, 8))
        self.assertEqual(outreach.due_followups(self.con, CFG, today=date(2026, 9, 24)), [])
        due = outreach.due_followups(self.con, CFG, today=date(2026, 9, 25))
        self.assertEqual([(d["email"], d["next_step"]) for d in due], [("anna@beiersdorf.com", 2)])
        payload = outreach.followup_payload(due[0])
        self.assertEqual(payload["message"]["subject"], "Re: " + SUBJECT)
        body = payload["message"]["body"]["content"]
        self.assertIn("Hallo Anna,", body)
        self.assertIn("erste Nachfrage", body)
        self.assertIn("Abmelden", body)
        outreach.mark_followup(self.con, due[0], 2)
        self.assertEqual(outreach.due_followups(self.con, CFG, today=date(2026, 9, 30)), [])
        due = outreach.due_followups(self.con, CFG, today=date(2026, 10, 2))
        self.assertEqual(due[0]["next_step"], 3)
        outreach.mark_followup(self.con, due[0], 2)
        self.assertEqual(self.con.execute("SELECT status FROM outreach_sequence").fetchone()[0], "done")
        self.assertEqual(outreach.due_followups(self.con, CFG, today=date(2026, 12, 1)), [])


class OpenerTests(OutreachDb):
    def setUp(self):
        super().setUp()
        self.state = tempfile.TemporaryDirectory()
        self.addCleanup(self.state.cleanup)
        patcher = mock.patch.dict(os.environ, {"LEADGEN_STATE_DIR": self.state.name})
        patcher.start()
        self.addCleanup(patcher.stop)
        self.con.execute("INSERT INTO customers VALUES ('cu3','SEW','sew-eurodrive.de',1,'qualified',NULL,NULL)")
        self.con.execute("INSERT INTO customers VALUES ('cu4','Quiet','quiet.de',1,'qualified',NULL,NULL)")
        self.con.execute("INSERT INTO contacts VALUES ('aa11','cu3','eva@sew-eurodrive.de','Eva','Head of IT','2026-10-01')")
        self.con.execute("INSERT INTO contacts VALUES ('bb22','cu4','max@quiet.de','Max','CIO','2026-10-01')")
        self.con.commit()
        signals._save({"sew-eurodrive.de": {"hiring": [{"type": "hiring_mdg", "summary": "Open role: Application Architect Master Data Governance (Bruchsal, Germany)", "date": "2026-10-01", "source_url": "https://jobs/sew"}],
                                            "hiring_at": date.today().isoformat(), "hiring_v": signals.HIRING_VERSION}})

    def test_candidates_need_signals(self):
        items = personalize.candidates(self.con)
        self.assertEqual([i["contact_id"] for i in items], ["aa11"])
        self.assertIn("Application Architect", items[0]["facts"][0]["fact"])

    def test_draft_validation_and_approval_flow(self):
        class Client:
            model = "fake"

            def complete(self, payload, system=""):
                key = payload["items"][0]["key"]
                return json.dumps({"drafts": [{"key": key, "fact_index": 0, "opener":
                    "Ich habe gesehen, dass SEW-Eurodrive in Bruchsal einen Application Architect für Master Data "
                    "Governance sucht, was oft mit einem laufenden SAP-MDG-Vorhaben in Ihrem Haus zusammenhängt."}]})

        result = personalize.draft(self.con, Client(), personalize.candidates(self.con))
        self.assertEqual(result["drafted"], 1)
        self.assertIsNone(outreach.approved_opener(self.con, CFG, "aa11"))  # approval required
        self.assertIsNotNone(outreach.approved_opener(self.con, {"openers": {"enabled": True, "require_approval": False}}, "aa11"))
        self.assertEqual(personalize.decide(self.con, "aa11", "save", "Super Angebot!"), "invalid text (30-320 characters, formal Sie, no URL, no ! or ?)")
        self.assertEqual(personalize.decide(self.con, "aa11", "approve"), "approved")
        self.assertIn("SEW-Eurodrive", outreach.approved_opener(self.con, CFG, "AA11"))
        self.assertIsNone(outreach.approved_opener(self.con, {"openers": {"enabled": False}}, "aa11"))
        outreach.mark_opener_sent(self.con, "aa11")
        self.assertIsNone(outreach.approved_opener(self.con, CFG, "aa11"))
        self.assertEqual(personalize.decide(self.con, "aa11", "approve"), "already sent")

    def test_clean_opener_rules(self):
        self.assertIsNone(personalize.clean_opener("Toll!"))
        self.assertIsNone(personalize.clean_opener("I saw that you are hiring an MDG architect in Bruchsal right now."))
        self.assertIsNone(personalize.clean_opener("Ich habe gesehen, dass Sie suchen: https://jobs/sew ist die Anzeige dazu."))
        self.assertIsNotNone(personalize.clean_opener("Ich habe gesehen, dass Ihr Team einen Architekten für SAP MDG sucht."))


if __name__ == "__main__":
    unittest.main()
