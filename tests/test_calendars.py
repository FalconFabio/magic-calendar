import json
import tempfile
import unittest
from datetime import date
from pathlib import Path
from contextlib import ExitStack
from unittest.mock import patch

import pandas as pd
from icalendar import Calendar

import export_ics as exporter
import fetch_tournaments as fetcher


class CalendarRegressionTests(unittest.TestCase):
    def event(self, organizer="Predator/PBS", title="Open", event_id="open", year=2026):
        return fetcher.Tournament(
            title, organizer, date(year, 11, 22), date(year, 11, 29),
            "San Antonio, USA", "PBS", "Official source", "https://example.com/open", event_id,
        )

    def test_epbf_changed_dates_and_simultaneous_youth_stops(self):
        html = """<table><tr><th>Date</th><th>Event</th><th>Loc.</th></tr>
        <tr><td>20-23 mar<br>NEW DATE</td><td>Seniors</td><td>Prague</td></tr>
        <tr><td>2026-11-21</td><td>Youth Tour</td><td>Tallin / Estonia</td></tr>
        <tr><td>2026-11-21</td><td>Youth Tour</td><td>Sofia / Bulgaria</td></tr>
        </table>"""
        with patch.object(fetcher, "http_get", return_value=html):
            events = fetcher.dedup(fetcher.fetch_epbf(2026, date(2026, 1, 1), date(2026, 12, 31), 0, 0))
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].end, date(2026, 3, 23))
        self.assertEqual(events[1].start, events[1].end)
        self.assertNotEqual(events[1].event_id, events[2].event_id)

    def test_matchroom_structured_fields_and_existing_uid(self):
        html = """<a href="/events/mosconi-cup/">
        <strong>November 27 - 30 2026</strong><div>Invitational</div>
        <div class="title">Mosconi Cup</div>
        <div class="venue">Caribe Royale Resort, Orlando, Florida, USA</div></a>"""
        with patch.object(fetcher, "http_get", return_value=html):
            event, = fetcher.fetch_matchroom(date(2026, 1, 1), date(2026, 12, 31), 0, 0)
        self.assertEqual(event.title, "Mosconi Cup")
        self.assertEqual(event.location, "Caribe Royale Resort, Orlando, Florida, USA")
        self.assertEqual(event.tour, "WNT (Invitational)")
        self.assertEqual(event.event_id, fetcher.stable_event_id_from_url("https://matchroompool.com/events/mosconi-cup/"))

    def test_wpa_whole_year_and_inclusive_json_end(self):
        html = '<iframe src="https://teamup.com/ksabc123?showHeader=0"></iframe>'
        payload = {"events": [
            {"id": "123", "title": "January Open", "start_dt": "2026-01-08T00:00:00",
             "end_dt": "2026-01-10T23:59:00", "location": "Taipei, Taiwan", "custom": {"discipline": ["9_ball"]}},
            {"id": "124", "title": ""},
        ]}
        with patch.object(fetcher, "http_get", side_effect=[html, json.dumps(payload)]) as get:
            event, = fetcher.fetch_wpa_current(date(2026, 1, 1), date(2026, 12, 31))
        self.assertIn("startDate=2026-01-01&endDate=2026-12-31", get.call_args.args[0])
        self.assertEqual(event.end, date(2026, 1, 10))
        self.assertEqual(event.discipline, "9-ball")

    def test_empty_export_retains_schema(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "empty.xlsx"
            fetcher.export_excel([], str(path))
            self.assertEqual(list(pd.read_excel(path).columns), fetcher.EXPORT_COLUMNS)

    def test_backup_reuses_only_missing_source_year(self):
        old = [self.event(event_id="pbs1"), self.event(title="Second Open", event_id="pbs2"),
               self.event("WPA", title="Obsolete date", event_id="wpa")]
        fresh = [self.event("WPA", title="Corrected date", event_id="wpa")]
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "previous.xlsx")
            fetcher.export_excel(old, path)
            merged, reused = fetcher.restore_missing_sources(fresh, path, date(2026, 1, 1), date(2026, 12, 31))
        self.assertEqual(len(merged), 3)
        self.assertEqual(len(reused), 2)
        self.assertNotIn("Obsolete date", [t.title for t in merged])

    def test_ics_and_pbs_are_from_same_reconciled_workbook(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "all.xlsx"
            fetcher.export_excel([self.event(), self.event("WPA", event_id="wpa")], str(source))
            argv = ["export_ics.py", "--xlsx", str(source), "--ics", str(root / "all.ics"),
                    "--ics-conflicts", str(root / "conflicts.ics"), "--pbs-ics", str(root / "pbs.ics"),
                    "--pbs-xlsx", str(root / "missing-or-stale.xlsx"), "--uid-domain", "FalconFabio.github.io"]
            with patch("sys.argv", argv):
                exporter.main()
            all_events = Calendar.from_ical((root / "all.ics").read_bytes()).walk("VEVENT")
            self.assertIn(b"REFRESH-INTERVAL;VALUE=DURATION:PT6H", (root / "all.ics").read_bytes())
            pbs_events = Calendar.from_ical((root / "pbs.ics").read_bytes()).walk("VEVENT")
            self.assertEqual(len(all_events), 2)
            self.assertEqual(len(pbs_events), 1)
            self.assertEqual(pbs_events[0]["DTEND"].dt, date(2026, 11, 30))
            self.assertEqual(str(pbs_events[0]["UID"]), "open@FalconFabio.github.io")
            self.assertIn("DTSTAMP", pbs_events[0])

    def test_total_source_outage_does_not_publish(self):
        with patch.object(fetcher, "fetch_pbs", return_value=[]), patch("sys.argv", ["fetch_tournaments.py", "--pbs-only"]):
            with self.assertRaisesRegex(RuntimeError, "No fresh events"):
                fetcher.main()

    def test_current_refresh_preserves_archives(self):
        year = max(2026, date.today().year)
        with tempfile.TemporaryDirectory() as tmp, ExitStack() as stack:
            root = Path(tmp)
            previous = root / "previous.xlsx"
            fetcher.export_excel([self.event("EPBF", event_id="archive", year=year-1)], str(previous))
            providers = {"fetch_pbs": "Predator/PBS", "fetch_wpa_current": "WPA",
                         "fetch_matchroom": "Matchroom", "fetch_epbf": "EPBF"}
            mocked = {}
            for function, organizer in providers.items():
                mocked[function] = stack.enter_context(patch.object(fetcher, function,
                    return_value=[self.event(organizer, event_id=organizer, year=year)]))
            archive = stack.enter_context(patch.object(fetcher, "fetch_wpa_ics"))
            stack.enter_context(patch("sys.argv", ["fetch_tournaments.py",
                "--from", f"{year-1}-01-01", "--to", f"{year}-12-31",
                "--refresh-from", f"{year}-01-01", "--previous", str(previous),
                "--out", str(root / "all.xlsx"), "--pbs-out", str(root / "pbs.xlsx"),
                "--report", str(root / "status.json")]))
            self.assertEqual(fetcher.main(), 0)
            archive.assert_not_called()
            self.assertEqual(mocked["fetch_wpa_current"].call_args.args[0], date(year, 1, 1))
            report = json.loads((root / "status.json").read_text())
            self.assertEqual(report["preserved_archive"], {f"EPBF {year-1}": 1})
            self.assertEqual(report["reused_previous"], {})
            self.assertEqual(len(pd.read_excel(root / "all.xlsx")), 5)

    def test_invalid_dates_are_rejected_before_ics(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "invalid.xlsx")
            pd.DataFrame([{"start_date": "2026-11-29", "end_date": "2026-11-22", "title": "Open"}]).to_excel(path, index=False)
            with self.assertRaises(ValueError):
                exporter.load_rows(path)


if __name__ == "__main__":
    unittest.main()
