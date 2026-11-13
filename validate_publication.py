"""Validate the complete calendar bundle and check the bytes served by Pages."""
import argparse
import hashlib
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

PUBLIC_FILES = (
    "tournaments.xlsx", "pbs.xlsx", "tournaments.ics", "tournaments-conflicts.ics",
    "pbs.ics", "pbs-conflicts.ics", "sync-status.json", "sync-status.txt", "tournaments-archive.json",
)


def validate(root, backup_path, state_path):
    # Keep remote verification lightweight: these dependencies are only needed
    # while checking the source workbook and generated calendars before upload.
    from icalendar import Calendar
    import calendar_backup
    import calendar_quality as quality
    import export_ics
    import fetch_tournaments as fetcher

    root = Path(root)
    report = json.loads((root / "sync-status.json").read_text())
    generated = datetime.fromisoformat(report["generated_at"])
    age = datetime.now(timezone.utc) - generated
    if age < -timedelta(minutes=5) or age > timedelta(hours=6):
        raise ValueError("Collection report is stale or dated in the future")
    raw = fetcher.load_previous_tournaments(root / "tournaments.xlsx")
    backup = fetcher.tournaments_from_records(calendar_backup.load(backup_path))
    records = [fetcher.tournament_record(t) for t in raw]
    if records != [fetcher.tournament_record(t) for t in backup]:
        raise ValueError("Workbook and permanent backup disagree")
    ids = [t.event_id for t in raw]
    if not ids or any(not uid for uid in ids) or len(ids) != len(set(ids)):
        raise ValueError("Missing or duplicate source identifiers")
    archive = fetcher.load_archive(root / "tournaments-archive.json")
    if {t.event_id: fetcher.tournament_record(t) for t in archive} != {t.event_id: fetcher.tournament_record(t) for t in raw if t.end < generated.date()}:
        raise ValueError("Archive does not contain every completed source record")
    state = quality.load_state(state_path)
    rows = export_ics.load_rows(root / "tournaments.xlsx")
    all_rows = quality.consolidate(rows, state)
    pbs_rows = quality.consolidate([r for r in rows if r["organizer"] == "Predator/PBS"], state)
    if report["calendar"]["events"] != len(all_rows) or report["calendar"]["source_records"] != len(raw):
        raise ValueError("Reported event count differs from the calendar")
    if {t.event_id for t in fetcher.load_previous_tournaments(root / "pbs.xlsx")} != {r["event_id"] for r in pbs_rows}:
        raise ValueError("PBS workbook differs from the source dataset")
    if {s["event_id"] for r in all_rows for s in r["sources"]} != set(ids):
        raise ValueError("Calendar consolidation dropped source records")
    counts = {}
    for name, expected_rows in (("tournaments", all_rows), ("pbs", pbs_rows)):
        conflict_indices = export_ics.build_conflict_set(expected_rows)
        for suffix, expected in (("", expected_rows), ("-conflicts", [r for i, r in enumerate(expected_rows) if i in conflict_indices])):
            path = root / f"{name}{suffix}.ics"
            data = path.read_bytes()
            if not data.rstrip().endswith(b"END:VCALENDAR"):
                raise ValueError(f"Truncated calendar: {path.name}")
            events = Calendar.from_ical(data).walk("VEVENT")
            by_id = {str(e["UID"]).split("@", 1)[0]: e for e in events}
            if len(by_id) != len(events) or set(by_id) != {r["event_id"] for r in expected}:
                raise ValueError(f"Calendar identifiers disagree: {path.name}")
            for row in expected:
                event = by_id[row["event_id"]]
                if event.decoded("DTSTART") != row["start"] or event.decoded("DTEND") != row["end"] + timedelta(days=1):
                    raise ValueError(f"Incorrect calendar dates: {path.name}")
            counts[path.name] = len(events)
    manifest = {"schema_version": 1, "generated_at": report["generated_at"], "events": counts,
                "files": {name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in PUBLIC_FILES}}
    (root / "publication-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def verify_remote(base_url, manifest, attempts=6, delay=10, sleep=time.sleep, download=None):
    # Binary downloads must be hashed before decoding (xlsx is a ZIP archive).
    import requests
    def fetch_bytes(url):
        with requests.get(url, timeout=(5, 20), headers={"Cache-Control": "no-cache"}, stream=True) as response:
            response.raise_for_status()
            body = bytearray()
            for chunk in response.iter_content(65536):
                body.extend(chunk)
                if len(body) > 8_000_000:
                    raise ValueError("Published file exceeds size limit")
            return bytes(body)
    download = download or fetch_bytes
    pending = dict(manifest["files"])
    for attempt in range(attempts):
        for name, expected in list(pending.items()):
            try:
                data = download(base_url.rstrip("/") + "/" + name)
                if hashlib.sha256(data).hexdigest() == expected:
                    del pending[name]
            except (requests.RequestException, ValueError):
                pass
        if not pending:
            return
        if attempt + 1 < attempts:
            sleep(delay)
    raise ValueError("Published files are unavailable or differ from the validated build: " + ", ".join(pending))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default="public")
    parser.add_argument("--backup", default="data/tournaments-backup.json")
    parser.add_argument("--state", default="data/calendar-state.json")
    parser.add_argument("--verify-url")
    parser.add_argument("--manifest", default="publication-manifest.json")
    args = parser.parse_args()
    if args.verify_url:
        verify_remote(args.verify_url, json.loads(Path(args.manifest).read_text()))
        print("All published files match the validated calendar bundle.")
    else:
        result = validate(args.root, args.backup, args.state)
        print("Validated calendars:", result["events"])


if __name__ == "__main__":
    main()
