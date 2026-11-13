import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from icalendar import Calendar

import calendar_quality as q
import export_ics as exporter


class QualityTests(unittest.TestCase):
    now = datetime(2026, 11, 13, 12, tzinfo=timezone.utc)
    refresh = date(2026, 10, 14)

    def row(self, organizer="WPA", uid="wpa", title="World Pool Masters", location="Taipei, Taiwan"):
        return dict(title=title, organizer=organizer, event_id=uid, start=date(2027, 1, 12), end=date(2027, 1, 14),
                    start_iso="2027-01-12", end_iso="2027-01-14", location=location, discipline="", tour="Tour",
                    venue_name="", venue_address="", prize_fund="", source="Official calendar", source_url=f"https://example.com/{uid}")

    def record(self, row):
        return dict(row, start_date=row["start"].isoformat(), end_date=row["end"].isoformat())

    def update(self, state, rows, fresh, previous, errors=()):
        return q.update_monitoring(state, [self.record(r) for r in rows], [self.record(r) for r in fresh],
                                   [self.record(r) for r in previous], self.refresh, self.now, errors)

    def test_duplicate_is_one_event_and_does_not_conflict_with_itself(self):
        rows = [self.row(location="Taipei City (TPE)"), self.row("Matchroom", "direct", title="2027 World Pool Masters", location="Songshan Park, Taipei, Chinese Taipei")]
        with tempfile.TemporaryDirectory() as tmp:
            all_path, conflict_path = Path(tmp) / "all.ics", Path(tmp) / "conflicts.ics"
            merged, conflicts = exporter.write_ics(rows, "Test", str(all_path), str(conflict_path), "test")
            events = Calendar.from_ical(all_path.read_bytes()).walk("VEVENT")
            self.assertEqual(len(events), 1)
            self.assertEqual(conflicts, set())
            self.assertEqual(Calendar.from_ical(conflict_path.read_bytes()).walk("VEVENT"), [])
            self.assertEqual(str(events[0]["UID"]), "direct@test")
            self.assertIn("https://example.com/wpa", str(events[0]["DESCRIPTION"]))
            self.assertIn("https://example.com/direct", str(events[0]["DESCRIPTION"]))
            self.assertEqual({s["event_id"] for s in merged[0]["sources"]}, {"wpa", "direct"})

    def test_distinct_divisions_locations_disciplines_and_editions_are_not_merged(self):
        base = self.row("Matchroom", "direct")
        variants = [self.row(title="World Pool Masters Women"), self.row(location="Paris, France"),
                    self.row(title="World Pool Masters Junior"), self.row(organizer="Matchroom", uid="second")]
        for variant in variants:
            self.assertEqual(len(q.consolidate([base, variant])), 2)
        other_year = dict(self.row(), start=date(2028, 1, 12), end=date(2028, 1, 14))
        self.assertEqual(len(q.consolidate([base, other_year])), 2)
        self.assertFalse(q.locations_compatible("Paris, France", "Paris, USA"))
        self.assertFalse(q.locations_compatible("Paris, France", "Lyon, France"))
        self.assertEqual(len(q.consolidate([dict(base, discipline="8-Ball"), dict(self.row(), discipline="9-Ball")])), 2)

    def test_ambiguous_match_is_not_assigned_to_either_stop(self):
        first = self.row("EPBF", "paris", location="Paris, France")
        second = self.row("EPBF", "lyon", location="Lyon, France")
        unknown = self.row(location="")
        self.assertEqual(len(q.consolidate([first, second, unknown])), 3)

    def test_small_date_disagreement_is_visible_and_organizer_dates_win(self):
        direct = self.row("Matchroom", "direct")
        other = dict(self.row(), end=date(2027, 1, 13))
        merged, = q.consolidate([other, direct])
        self.assertEqual(merged["end"], direct["end"])
        self.assertTrue(merged["date_disagreement"])
        self.assertEqual(len(merged["sources"]), 2)

    def test_uid_survives_source_outage_and_fresher_secondary_data(self):
        direct, other = self.row("Matchroom", "direct"), self.row()
        state = q.load_state(None)
        q.consolidate([direct, other], state)
        state["events"] = {"direct": {"verification": "stale"}, "wpa": {"verification": "current"}}
        merged, = q.consolidate([other, direct], state)
        self.assertEqual(merged["event_id"], "direct")
        self.assertEqual(merged["organizer"], "WPA")

    def test_changed_dates_and_venue_are_logged_once(self):
        old = self.row()
        new = dict(old, start=old["start"] + timedelta(days=2), end=old["end"] + timedelta(days=2), location="Lyon, France")
        state = q.load_state(None)
        report = self.update(state, [new], [new], [old])
        self.assertEqual(len(report["changes"]), 1)
        self.assertEqual(set(report["changes"][0]["fields"]), {"start_date", "end_date", "location"})
        self.assertIn("2027-01-12", q.describe_changes(report["changes"][0]))
        self.assertEqual(self.update(state, [new], [new], [new])["changes"], [])

    def test_missing_is_unconfirmed_not_cancelled_and_alerts_only_once(self):
        missing, current = self.row(uid="missing"), self.row(uid="current", title="Another Championship")
        state = q.load_state(None)
        report = self.update(state, [missing, current], [current], [missing, current])
        self.assertEqual(state["events"]["missing"]["verification"], "missing")
        self.assertEqual(state["events"]["missing"]["status"], "")
        self.assertEqual(len(report["new_alerts"]), 1)
        self.assertEqual(self.update(state, [missing, current], [current], [missing, current])["new_alerts"], [])

    def test_source_incident_is_alerted_once_then_rearms_after_recovery(self):
        row = self.row("Predator/PBS", "pbs")
        state = q.load_state(None)
        errors = ["Predator/PBS: 403 Forbidden", "EPBF 2029: 404 Not Found"]
        report = self.update(state, [row], [], [row], errors)
        self.assertEqual([x["source"] for x in report["new_alerts"]], ["Predator/PBS"])
        self.assertEqual(self.update(state, [row], [], [row], errors)["new_alerts"], [])
        self.assertEqual(len(self.update(state, [row], [row], [row])["resolved"]), 1)
        self.assertEqual(len(self.update(state, [row], [], [row], errors)["new_alerts"]), 1)

    def test_explicit_cancellation_is_exported_and_excluded_from_conflicts(self):
        cancelled = self.row(title="World Pool Masters — CANCELLED")
        other = self.row("EPBF", "other", "Another Championship")
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "all.ics"
            _, conflicts = exporter.write_ics([cancelled, other], "Test", str(path), str(Path(tmp) / "conflicts.ics"), "test")
            events = Calendar.from_ical(path.read_bytes()).walk("VEVENT")
            self.assertEqual([str(e.get("STATUS", "")) for e in events].count("CANCELLED"), 1)
            self.assertEqual(conflicts, set())
        self.assertEqual(q.event_status("World Pool Masters — not cancelled"), "")

    def test_epbf_nearby_reschedule_preserves_uid_but_not_ambiguous_rounds(self):
        old = self.row("EPBF", "old")
        new = dict(old, event_id="date-hash-new", start=date(2027, 1, 19), end=date(2027, 1, 21))
        replacements = q.epbf_id_replacements([self.record(new)], [self.record(old)], self.refresh)
        self.assertEqual(replacements, {"date-hash-new": "old"})
        ambiguous = dict(old, event_id="another")
        self.assertEqual(q.epbf_id_replacements([self.record(new)], [self.record(old), self.record(ambiguous)], self.refresh), {})

    def test_general_titles_are_clean_but_true_overlap_remains_in_conflicts_feed(self):
        rows = [self.row(), self.row("EPBF", "other", "Another Championship")]
        with tempfile.TemporaryDirectory() as tmp:
            all_path, conflicts_path = Path(tmp) / "all.ics", Path(tmp) / "conflicts.ics"
            exporter.write_ics(rows, "Test", str(all_path), str(conflicts_path), "test")
            all_events = Calendar.from_ical(all_path.read_bytes()).walk("VEVENT")
            conflicts = Calendar.from_ical(conflicts_path.read_bytes()).walk("VEVENT")
            self.assertFalse(any(str(e["SUMMARY"]).startswith("⚠") for e in all_events))
            self.assertEqual(len(conflicts), 2)
            self.assertTrue(all(str(e["SUMMARY"]).startswith("⚠") for e in conflicts))


if __name__ == "__main__":
    unittest.main()
