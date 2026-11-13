import json
import tempfile
import unittest
from contextlib import ExitStack
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

from icalendar import Calendar

import export_ics as exporter
import fetch_tournaments as f


class HistoryTests(unittest.TestCase):
    today = date(2026, 11, 13)
    refresh = date(2026, 10, 14)

    def event(self, start, event_id="open", organizer="WPA"):
        return f.Tournament("Open", organizer, start, start + timedelta(days=2),
                            "Paris, France", "Tour", "Official source", "https://example.com/open", event_id)

    def merge(self, fresh, previous=(), archived=()):
        return f.reconcile_history(fresh, previous, archived, self.refresh, self.today,
                                   date(2021, 11, 13), date(2029, 11, 13), {"WPA", "EPBF"})

    def test_old_history_survives_rolling_window_and_missing_workbook(self):
        old = self.event(date(2010, 1, 1), "old")
        fresh = self.event(date(2027, 1, 1), "next")
        merged, restored = self.merge([fresh], archived=[old])
        self.assertEqual(restored, [old])
        self.assertIn(old, merged)

    def test_finished_event_removed_by_source_stays_but_future_removal_does_not(self):
        past = self.event(date(2026, 11, 1), "past")
        removed_future = self.event(date(2026, 12, 1), "removed")
        current = self.event(date(2026, 12, 10), "current")
        merged, _ = self.merge([current], previous=[past, removed_future])
        self.assertEqual({t.event_id for t in merged}, {"past", "current"})

    def test_recent_correction_replaces_old_dates_and_frozen_archive_is_unchanged(self):
        recent = self.event(date(2026, 11, 1), "recent")
        corrected = replace(recent, end=date(2026, 11, 5), location="Lyon, France")
        frozen = self.event(date(2026, 2, 1), "frozen")
        overwritten = replace(frozen, location="Incorrect location")
        merged, _ = self.merge([corrected], previous=[recent, overwritten], archived=[recent, frozen])
        self.assertIn(corrected, merged)
        self.assertIn(frozen, merged)
        self.assertNotIn(overwritten, merged)

    def test_annual_url_reuse_keeps_both_editions_with_stable_unique_uids(self):
        old = self.event(date(2025, 1, 1))
        new = self.event(date(2026, 1, 1))
        editions = f.assign_edition_ids(f.dedup([old, new]))
        self.assertEqual([t.event_id for t in editions], ["open", "open-edition-2026"])
        again = f.assign_edition_ids(f.dedup(editions + [new]))
        self.assertEqual([t.event_id for t in again], ["open", "open-edition-2026"])
        self.assertEqual(len(again), 2)

    def test_archive_is_stable_and_contains_only_completed_events(self):
        past = self.event(date(2025, 1, 1))
        ongoing = self.event(self.today - timedelta(days=1), "ongoing")
        future = self.event(date(2027, 1, 1), "future")
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "archive.json"
            f.save_archive([past, ongoing, future], path, self.today)
            before = (path.read_bytes(), path.stat().st_mtime_ns)
            f.save_archive([future, past, ongoing], path, self.today)
            self.assertEqual((path.read_bytes(), path.stat().st_mtime_ns), before)
            self.assertEqual(f.load_archive(path), [past])

    def test_missing_explicit_previous_file_stops_before_fetching(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(f, "fetch_pbs") as fetch:
            with patch("sys.argv", ["fetch_tournaments.py", "--previous", str(Path(tmp) / "missing.xlsx")]):
                with self.assertRaises(FileNotFoundError):
                    f.main()
            fetch.assert_not_called()

    def test_permanent_history_is_in_same_ics_without_fetching_old_seasons(self):
        year = max(2026, date.today().year)
        old = self.event(date(2010, 1, 1), "archive", "EPBF")
        with tempfile.TemporaryDirectory() as tmp, ExitStack() as stack:
            root = Path(tmp)
            archive = root / "archive.json"
            f.save_archive([old], archive, date.today())
            providers = {"fetch_pbs": "Predator/PBS", "fetch_wpa_current": "WPA",
                         "fetch_matchroom": "Matchroom", "fetch_epbf": "EPBF"}
            mocks = {}
            for name, organizer in providers.items():
                mocks[name] = stack.enter_context(patch.object(f, name, return_value=[
                    self.event(date(year, 12, 1), organizer, organizer)]))
            legacy = stack.enter_context(patch.object(f, "fetch_wpa_ics"))
            stack.enter_context(patch("builtins.print"))
            stack.enter_context(patch("sys.argv", ["fetch_tournaments.py", "--archive", str(archive),
                "--refresh-from", f"{year}-01-01", "--from", f"{year-5}-01-01", "--to", f"{year}-12-31",
                "--out", str(root / "all.xlsx"), "--pbs-out", str(root / "pbs.xlsx"), "--report", str(root / "status.json")]))
            self.assertEqual(f.main(), 0)
            legacy.assert_not_called()
            mocks["fetch_epbf"].assert_called_once()
            self.assertEqual(mocks["fetch_wpa_current"].call_args.args[0], date(year, 1, 1))
            exporter.write_ics(exporter.load_rows(str(root / "all.xlsx")), "All tournaments",
                               str(root / "all.ics"), str(root / "conflicts.ics"), "test")
            events = Calendar.from_ical((root / "all.ics").read_bytes()).walk("VEVENT")
            self.assertEqual(len(events), 5)
            self.assertIn(date(2010, 1, 1), [e["DTSTART"].dt for e in events])
            self.assertEqual(json.loads((root / "status.json").read_text())["archive"]["oldest_date"], "2010-01-01")


if __name__ == "__main__":
    unittest.main()
