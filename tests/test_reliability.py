import hashlib
import json
import tempfile
import unittest
from contextlib import ExitStack
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import Mock, patch

import requests

import calendar_backup
import export_ics
import fetch_tournaments as f
from collector_http import FetchError, HTTPClient
from validate_publication import validate, verify_remote


class Response:
    def __init__(self, text="valid data", status=200, headers=None, body_error=None):
        self.body = text.encode()
        self.status_code = status
        self.headers = headers or {}
        self.encoding = "utf-8"
        self.closed = False
        self.body_error = body_error

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(str(self.status_code))

    def iter_content(self, chunk_size):
        if self.body_error:
            raise self.body_error
        yield self.body

    def close(self):
        self.closed = True


class HTTPTests(unittest.TestCase):
    def client(self, *responses):
        session = Mock(headers={})
        session.get.side_effect = responses
        return HTTPClient(session=session, sleep=Mock()), session

    def test_temporary_server_failure_recovers_and_reuses_same_page(self):
        failed, success = Response(status=503), Response("restored")
        client, session = self.client(failed, success)
        self.assertEqual(client.get_text("https://a.test/calendar"), "restored")
        self.assertEqual(client.get_text("https://a.test/calendar"), "restored")
        self.assertEqual(session.get.call_count, 2)
        self.assertTrue(failed.closed and success.closed)
        self.assertEqual(client.summary()["a.test"]["retries"], 1)

    def test_read_failure_after_headers_is_retried(self):
        client, _ = self.client(Response(body_error=requests.exceptions.ChunkedEncodingError("truncated")), Response("complete"))
        self.assertEqual(client.get_text("https://a.test"), "complete")

    def test_rate_limit_waits_for_server_delay(self):
        client, _ = self.client(Response(status=429, headers={"Retry-After": "3"}), Response())
        client.get_text("https://a.test")
        client.sleep.assert_called_once_with(3)

    def test_long_retry_after_defers_host_without_repeated_requests(self):
        client, session = self.client(Response(status=429, headers={"Retry-After": "3600"}), Response())
        with self.assertRaises(FetchError):
            client.get_text("https://a.test/calendar")
        with self.assertRaises(FetchError):
            client.get_text("https://a.test/other")
        self.assertEqual(client.get_text("https://b.test/calendar"), "valid data")
        self.assertEqual(session.get.call_count, 2)
        client.sleep.assert_not_called()

    def test_missing_future_page_does_not_disable_current_year(self):
        client, session = self.client(Response(status=404), Response("current season"))
        with self.assertRaises(FetchError):
            client.get_text("https://a.test/2029")
        self.assertEqual(client.get_text("https://a.test/2026"), "current season")
        self.assertEqual(session.get.call_count, 2)

    def test_forbidden_path_is_not_retried_but_another_public_path_can_work(self):
        client, session = self.client(Response(status=403), Response("events"))
        with self.assertRaises(FetchError):
            client.get_text("https://a.test/archive")
        self.assertEqual(client.get_text("https://a.test/events"), "events")
        self.assertEqual(session.get.call_count, 2)

    def test_html_challenge_is_rejected_without_rejecting_normal_cloudflare_footer(self):
        client, _ = self.client(Response("<title>Just a moment...</title>"), Response("<title>Events</title>Powered by Cloudflare"))
        with self.assertRaisesRegex(FetchError, "challenge"):
            client.get_text("https://blocked.test")
        self.assertIn("Events", client.get_text("https://working.test"))

    def test_empty_and_oversize_responses_are_not_accepted(self):
        client, _ = self.client(Response(""), Response(""), Response(""), Response("too large"))
        with self.assertRaises(FetchError):
            client.get_text("https://a.test")
        with self.assertRaisesRegex(FetchError, "size limit"):
            client.get_text("https://b.test", max_bytes=2)

    def test_repeated_timeouts_stop_host_but_leave_other_sources_working(self):
        client, session = self.client(*([requests.Timeout("slow")] * 6), Response("other source"))
        for url in ("https://a.test/one", "https://a.test/two", "https://a.test/three"):
            with self.assertRaises(FetchError):
                client.get_text(url)
        self.assertEqual(client.get_text("https://b.test"), "other source")
        self.assertEqual(session.get.call_count, 7)


class SourceResilienceTests(unittest.TestCase):
    iframe = '<iframe src="https://teamup.com/ksabc123"></iframe>'

    def payload(self, year=2026):
        return {"events": [{"id": str(year), "title": "World Open", "start_dt": f"{year}-11-20T00:00:00Z", "end_dt": f"{year}-11-22T23:59:00Z"}]}

    def test_one_failed_year_does_not_lose_other_wpa_seasons(self):
        with patch.object(f, "http_get", side_effect=[self.iframe, json.dumps(self.payload()), FetchError("503"), json.dumps(self.payload(2028))]):
            events = f.fetch_wpa_current(date(2026, 1, 1), date(2028, 12, 31))
        self.assertEqual({e.start.year for e in events}, {2026, 2028})
        self.assertIn("WPA 2027", events.issues[0])

    def test_one_malformed_record_does_not_lose_valid_records(self):
        payload = self.payload()
        payload["events"].extend([{"id": "bad", "title": "Broken date", "start_dt": "tomorrow"}, None, dict(payload["events"][0], id="bad-location", location=["invalid"])])
        with patch.object(f, "http_get", side_effect=[self.iframe, json.dumps(payload)]):
            events = f.fetch_wpa_current(date(2026, 1, 1), date(2026, 12, 31))
        self.assertEqual(len(events), 1)
        self.assertIn("3 invalid", events.issues[0])

    def test_discovery_outage_uses_previously_verified_official_calendar(self):
        with patch.object(f, "WPA_KNOWN_CALENDARS", {"https://teamup.com/ksabc123"}), patch.object(f, "http_get", side_effect=[FetchError("unavailable"), json.dumps(self.payload())]) as get:
            events = f.fetch_wpa_current(date(2026, 1, 1), date(2026, 12, 31))
        self.assertEqual(len(events), 1)
        self.assertIn("https://teamup.com/ksabc123/events?", get.call_args.args[0])
        self.assertIn("WPA discovery", events.issues[0])

    def test_changed_epbf_layout_is_an_error_not_an_empty_success(self):
        with patch.object(f, "http_get", return_value="<main>New site layout</main>"):
            with self.assertRaisesRegex(ValueError, "structure"):
                f.fetch_epbf(2026, date(2026, 1, 1), date(2026, 12, 31), 0, 0)

    def test_epbf_invalid_dates_are_reported_alongside_valid_events(self):
        html = '<table><tr><th>Date</th><th>Event</th><th>Location</th></tr><tr><td>unknown date</td><td>New Open</td><td>Paris</td></tr><tr><td>2026-11-21</td><td>Valid Open</td><td>Lyon</td></tr></table>'
        with patch.object(f, "http_get", return_value=html):
            events = f.fetch_epbf(2026, date(2026, 1, 1), date(2026, 12, 31), 0, 0)
        self.assertEqual(len(events), 1)
        self.assertIn("1 event date", events.issues[0])

    def test_changed_matchroom_layout_is_reported(self):
        with patch.object(f, "http_get", return_value="<main>No readable events</main>"):
            with self.assertRaisesRegex(ValueError, "structure"):
                f.fetch_matchroom(date(2026, 1, 1), date(2026, 12, 31), 0, 0)


class BackupAndPublicationTests(unittest.TestCase):
    def event(self, organizer="WPA", uid="future", start=None):
        start = start or date.today()
        return f.Tournament("Open " + uid, organizer, start, start + timedelta(days=2), "Paris, France", "Tour", "Official source", "https://example.com/" + uid, uid)

    def test_backup_contains_future_events_and_detects_corruption(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "backup.json"
            event = self.event(start=date.today() + timedelta(days=60))
            calendar_backup.save([f.tournament_record(event)], path)
            self.assertEqual(f.load_previous_tournaments(path), [event])
            data = json.loads(path.read_text())
            data["events"][0]["title"] = "corrupt"
            path.write_text(json.dumps(data))
            with self.assertRaisesRegex(ValueError, "checksum"):
                f.load_previous_tournaments(path)

    def test_complete_outage_leaves_backup_unchanged(self):
        with tempfile.TemporaryDirectory() as tmp, ExitStack() as stack:
            path = Path(tmp) / "backup.json"
            calendar_backup.save([f.tournament_record(self.event("Predator/PBS"))], path)
            original = path.read_bytes()
            stack.enter_context(patch.object(f, "fetch_pbs", side_effect=FetchError("unavailable")))
            stack.enter_context(patch("sys.argv", ["fetch_tournaments.py", "--pbs-only", "--previous", str(path), "--backup", str(path)]))
            stack.enter_context(patch("builtins.print"))
            with self.assertRaisesRegex(RuntimeError, "No fresh events"):
                f.main()
            self.assertEqual(path.read_bytes(), original)

    def build(self, root, empty_future=False):
        backup, archive, state = root / "backup.json", root / "tournaments-archive.json", root / "state.json"
        past = self.event("EPBF", "archive", date(2010, 1, 1))
        calendar_backup.save([f.tournament_record(past)], backup)
        f.save_archive([past], archive, date.today())
        with ExitStack() as stack:
            for name, organizer in {"fetch_pbs": "Predator/PBS", "fetch_wpa_current": "WPA", "fetch_matchroom": "Matchroom", "fetch_epbf": "EPBF"}.items():
                if name == "fetch_epbf" and empty_future:
                    stack.enter_context(patch.object(f, name, side_effect=[[self.event(organizer, organizer)], ValueError("EPBF calendar structure not recognized; no event table")]))
                else:
                    stack.enter_context(patch.object(f, name, return_value=[self.event(organizer, organizer)]))
            stack.enter_context(patch("sys.argv", ["fetch_tournaments.py", "--previous", str(backup), "--backup", str(backup), "--archive", str(archive), "--state", str(state), "--refresh-from", date.today().isoformat(), "--to", f"{date.today().year + int(empty_future)}-12-31", "--out", str(root/'tournaments.xlsx'), "--pbs-out", str(root/'pbs.xlsx'), "--report", str(root/'sync-status.json')]))
            stack.enter_context(patch("builtins.print"))
            f.main()
        with patch("sys.argv", ["export_ics.py", "--xlsx", str(root/'tournaments.xlsx'), "--ics", str(root/'tournaments.ics'), "--ics-conflicts", str(root/'tournaments-conflicts.ics'), "--pbs-ics", str(root/'pbs.ics'), "--state", str(state), "--report", str(root/'sync-status.json'), "--status-text", str(root/'sync-status.txt')]):
            export_ics.main()
        return backup, state

    def test_unannounced_future_season_does_not_create_an_incident(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.build(root, empty_future=True)
            report = json.loads((root / "sync-status.json").read_text())
            self.assertEqual(report["monitoring"]["incidents"], [])
            self.assertEqual(report["collection"][-1]["status"], "not_published")

    def test_pipeline_checks_archives_future_records_and_every_published_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            backup, state = self.build(root)
            manifest = validate(root, backup, state)
            self.assertEqual(manifest["events"]["tournaments.ics"], 5)
            self.assertEqual(len(manifest["files"]), 9)
            path = root / "tournaments.ics"
            path.write_bytes(path.read_bytes().replace(b"END:VCALENDAR", b""))
            with self.assertRaisesRegex(ValueError, "Truncated"):
                validate(root, backup, state)

    def test_missing_archived_event_stops_publication(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            backup, state = self.build(root)
            (root / "tournaments-archive.json").write_text('{"schema_version": 1, "events": []}')
            with self.assertRaisesRegex(ValueError, "Archive"):
                validate(root, backup, state)

    def test_remote_verification_retries_stale_files_only(self):
        current = b"new calendar"
        manifest = {"files": {"all.ics": hashlib.sha256(current).hexdigest(), "pbs.ics": hashlib.sha256(current).hexdigest()}}
        download = Mock(side_effect=[b"old calendar", current, current])
        sleep = Mock()
        verify_remote("https://example.com", manifest, attempts=2, sleep=sleep, download=download)
        self.assertEqual(download.call_count, 3)
        sleep.assert_called_once()

    def test_remote_missing_or_wrong_bytes_cannot_pass(self):
        manifest = {"files": {"all.ics": hashlib.sha256(b"expected").hexdigest()}}
        for result in (b"wrong", requests.ConnectionError("offline")):
            with self.assertRaisesRegex(ValueError, "unavailable or differ"):
                verify_remote("https://example.com", manifest, attempts=1, download=Mock(side_effect=[result]))


if __name__ == "__main__":
    unittest.main()
