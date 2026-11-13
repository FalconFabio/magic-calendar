"""Rebuild presentation from verified saved data without collecting any source."""
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import fetch_tournaments as fetcher


def prepare(backup_path, published_workbook, published_report, output_dir):
    saved = fetcher.load_previous_tournaments(backup_path)
    published = fetcher.load_previous_tournaments(published_workbook)
    if [fetcher.tournament_record(t) for t in saved] != [fetcher.tournament_record(t) for t in published]:
        raise ValueError("Published data and backup disagree; refusing to reformat a different collection")
    report = json.loads(Path(published_report).read_text(encoding="utf-8"))
    if report["calendar"]["source_records"] != len(saved):
        raise ValueError("Published report and backup disagree")
    # Retain the real collection time and source verification timestamps.
    report["publication_mode"] = "formatting_only"
    report["reformatted_at"] = datetime.now(timezone.utc).isoformat()
    monitoring = report.get("monitoring", {})
    for key in ("new_alerts", "resolved", "changes"):
        monitoring[key] = []
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    fetcher.export_excel(saved, output / "tournaments.xlsx")
    fetcher.export_excel([t for t in saved if t.organizer == "Predator/PBS"], output / "pbs.xlsx")
    (output / "sync-status.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--backup", default="data/tournaments-backup.json")
    parser.add_argument("--published-workbook", required=True)
    parser.add_argument("--published-report", required=True)
    parser.add_argument("--output-dir", default=".")
    args = parser.parse_args()
    prepare(args.backup, args.published_workbook, args.published_report, args.output_dir)


if __name__ == "__main__":
    main()
