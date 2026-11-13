#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import hashlib
import json
from html import escape
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict
from urllib.parse import urlparse

import pandas as pd
from icalendar import Calendar, Event, vDuration
import calendar_quality as quality


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--xlsx", required=True, help="Input tournaments.xlsx")
    p.add_argument("--ics", required=True, help="Output tournaments.ics")
    p.add_argument("--ics-conflicts", required=True, help="Output tournaments-conflicts.ics")
    p.add_argument("--pbs-xlsx", default="pbs.xlsx", help="Input PBS-only xlsx (optional)")
    p.add_argument("--pbs-ics", default=None, help="Output PBS-only ics (optional)")
    p.add_argument("--pbs-ics-conflicts", default=None, help="Output PBS-only conflicts ics (optional)")
    p.add_argument("--calname", default="US Pool – Tournaments", help="Calendar display name")
    p.add_argument("--pbs-calname", default=None, help="PBS calendar display name (optional)")
    p.add_argument("--uid-domain", default="uspool.local", help="UID suffix domain")
    p.add_argument("--state", help="Persistent source verification and change history")
    p.add_argument("--report", help="Collection report to enrich with calendar statistics")
    p.add_argument("--status-text", help="Readable French synchronization report")
    return p.parse_args()


def to_date(s: str) -> date:
    return datetime.fromisoformat(str(s)).date()


def stable_uid_fallback(organizer: str, start: str, end: str, title: str, uid_domain: str) -> str:
    raw = f"{organizer}|{start}|{end}|{title}".encode("utf-8")
    h = hashlib.sha1(raw).hexdigest()
    return f"{h}@{uid_domain}"


def build_conflict_set(rows: List[Dict]) -> set:
    """
    Conflict if inclusive overlap on days.
    O(n log n) sweep using a heap.
    """
    indexed = list(enumerate(rows))
    indexed.sort(key=lambda x: (x[1]["start"], x[1]["end"], x[1]["title"]))

    import heapq
    heap = []  # (end_date, idx)
    conflict = set()

    for idx_i, ev in indexed:
        if ev.get("status") == "CANCELLED":
            continue
        start = ev["start"]
        end = ev["end"]

        # remove events that end strictly before start (inclusive overlap => < start is safe)
        while heap and heap[0][0] < start:
            heapq.heappop(heap)

        if heap:
            conflict.add(idx_i)
            for _, other_idx in heap:
                conflict.add(other_idx)

        heapq.heappush(heap, (end, idx_i))

    return conflict


def make_calendar(calname: str) -> Calendar:
    cal = Calendar()
    cal.add("prodid", "-//US Pool Calendar//github//")
    cal.add("version", "2.0")
    cal.add("calscale", "GREGORIAN")
    cal.add("x-wr-calname", calname)
    cal.add("refresh-interval", vDuration(timedelta(hours=6)), parameters={"VALUE": "DURATION"})
    cal.add("x-published-ttl", "PT6H")
    return cal


def readable_date(value):
    months = ("janvier", "février", "mars", "avril", "mai", "juin", "juillet", "août", "septembre", "octobre", "novembre", "décembre")
    day = date.fromisoformat(str(value)[:10])
    return f"{day.day} {months[day.month - 1]} {day.year}"


def readable_range(start, end):
    return readable_date(start) if str(start)[:10] == str(end)[:10] else f"{readable_date(start)} → {readable_date(end)}"


def event_notes(row):
    """Render the same sections as readable plain text and optional rich text."""
    sections = []
    monitoring = row.get("monitoring", {})
    verification = monitoring.get("verification")
    notices = []
    if row.get("status") == "CANCELLED":
        notices.append(("Annulé", "Annulation explicitement indiquée par la source retenue."))
    elif row.get("status") == "TENTATIVE":
        notices.append(("À confirmer", "Événement provisoire ou reporté ; dates à confirmer."))
    if verification == "stale":
        notices.append(("À reconfirmer", "Source temporairement indisponible. Dernières données conservées."))
    elif verification == "missing":
        notices.append(("À reconfirmer", "Absent de la dernière collecte. Annulation non confirmée."))
    if row.get("date_disagreement"):
        notices.append(("Dates différentes", "Les dates affichées suivent la source retenue. Voir le détail des sources ci-dessous."))
    if notices:
        sections.append(("⚠ À SAVOIR", [[notice] for notice in notices]))

    details = [(label, row[key]) for label, key in (
        ("Discipline", "discipline"), ("Organisateur", "organizer"),
        ("Circuit", "tour"), ("Dotation", "prize_fund"),
    ) if row.get(key)]
    if details:
        sections.append(("🎱 TOURNOI", [details]))
    place = [("", row.get("location") or "Lieu à confirmer.")]
    for label, key in (("Salle", "venue_name"), ("Adresse", "venue_address")):
        if row.get(key) and row[key] != row.get("location"):
            place.append((label, row[key]))
    sections.append(("📍 LIEU", [place]))

    follow_up = []
    if verification == "archived":
        follow_up.append([("", "Tournoi passé, conservé en archive.")])
    if monitoring.get("last_verified"):
        follow_up.append([("Dernière vérification", readable_date(monitoring["last_verified"]))])
    change = monitoring.get("change")
    if change:
        before, after = change["before"], change["after"]
        changes = [("Dernière modification", readable_date(change["at"]))]
        if (before["start_date"], before["end_date"]) != (after["start_date"], after["end_date"]):
            changes += [("Anciennes dates", readable_range(before["start_date"], before["end_date"])),
                        ("Nouvelles dates", readable_range(after["start_date"], after["end_date"]))]
        if before["location"] != after["location"]:
            changes += [("Ancien lieu", before["location"] or "Non précisé"),
                        ("Nouveau lieu", after["location"] or "Non précisé")]
        if before["title"] != after["title"]:
            changes += [("Ancien nom", before["title"]), ("Nouveau nom", after["title"])]
        follow_up.append(changes)
    if follow_up:
        sections.append(("🔄 SUIVI", follow_up))

    sources = row.get("sources") or [row]
    references = []
    for source in sources:
        reference = [("", source.get("source") or source.get("organizer") or "Page officielle")]
        if len(sources) > 1:
            reference.insert(0, ("", source["organizer"]))
            reference.append(("Dates", readable_range(source["start"], source["end"])))
            checked = source.get("monitoring", {}).get("last_verified")
            if checked:
                reference.append(("Vérifié le", readable_date(checked)))
        if source.get("source_url"):
            reference.append(("", source["source_url"]))
        references.append(reference)
    sections.append(("🔗 SOURCES" if len(sources) > 1 else "🔗 SOURCE", references))

    plain, rich = [], []
    for heading, paragraphs in sections:
        plain_paragraphs, html_paragraphs = [], []
        for paragraph in paragraphs:
            plain_lines, html_lines = [], []
            for label, value in paragraph:
                value = str(value)
                plain_lines.append(f"{label} : {value}" if label else value)
                html_value = escape(value).replace("\n", "<br>")
                try:
                    parsed = urlparse(value) if not label and value.startswith(("https://", "http://")) else None
                except ValueError:
                    parsed = None
                if parsed and parsed.netloc:
                    html_value = f'<a href="{escape(value, quote=True)}">Consulter la source</a>'
                html_lines.append(f"<strong>{escape(label)} :</strong> {html_value}" if label else html_value)
            plain_paragraphs.append("\n".join(plain_lines))
            html_paragraphs.append("<br>".join(html_lines))
        plain.append(heading + "\n" + "\n\n".join(plain_paragraphs))
        rich.append(f"<p><strong>{escape(heading)}</strong><br>" + "<br><br>".join(html_paragraphs) + "</p>")
    return "\n\n".join(plain), "".join(rich)


def add_event(cal: Calendar, row: Dict, uid_domain: str, mark_conflict: bool):
    ev = Event()
    ev.add("dtstamp", datetime.now(timezone.utc))

    start_d: date = row["start"]
    end_d: date = row["end"]

    ev.add("dtstart", start_d)
    ev.add("dtend", end_d + timedelta(days=1))  # all-day, exclusive DTEND

    title = row["title"]
    if mark_conflict:
        title = f"⚠ {title}"
        ev.add("categories", "CONFLICT")
    ev.add("summary", title)
    if row.get("status"):
        ev.add("status", row["status"])

    if row.get("location"):
        ev.add("location", row["location"])

    # Stable UID: prefer event_id (hash of URL from fetcher)
    if row.get("event_id"):
        ev.add("uid", f"{row['event_id']}@{uid_domain}")
    else:
        ev.add("uid", stable_uid_fallback(row["organizer"], row["start_iso"], row["end_iso"], row["title"], uid_domain))

    plain_notes, rich_notes = event_notes(row)
    ev.add("description", plain_notes)
    ev.add("x-alt-desc", rich_notes, parameters={"FMTTYPE": "text/html"})

    if row.get("source_url"):
        ev.add("url", row["source_url"])

    cal.add_component(ev)


def load_rows(xlsx_path: str) -> List[Dict]:
    df = pd.read_excel(xlsx_path)

    def get_opt(r, col: str) -> str:
        v = r.get(col)
        return "" if (v is None or pd.isna(v)) else str(v).strip()

    rows: List[Dict] = []
    for _, r in df.iterrows():
        start = to_date(r["start_date"])
        end = to_date(r["end_date"])
        if end < start or not get_opt(r, "title"):
            raise ValueError(f"Invalid event: {r.to_dict()}")
        rows.append(
            {
                "start": start,
                "end": end,
                "start_iso": str(r["start_date"]),
                "end_iso": str(r["end_date"]),
                "title": get_opt(r, "title"),
                "organizer": get_opt(r, "organizer"),
                "tour": get_opt(r, "tour"),
                "location": get_opt(r, "location") or None,
                "venue_name": get_opt(r, "venue_name"),
                "venue_address": get_opt(r, "venue_address"),
                "prize_fund": get_opt(r, "prize_fund"),
                "discipline": get_opt(r, "discipline"),
                "event_id": get_opt(r, "event_id"),
                "source": get_opt(r, "source"),
                "source_url": get_opt(r, "source_url"),
            }
        )
    return rows


def _default_conflicts_path(path: str) -> str:
    if path.endswith(".ics"):
        return path[:-4] + "-conflicts.ics"
    return path + "-conflicts.ics"


def write_ics(rows: List[Dict], calname: str, ics_path: str, conflicts_path: str, uid_domain: str, state=None):
    rows = quality.consolidate(rows, state)
    conflict_set = build_conflict_set(rows)

    cal_all = make_calendar(calname)
    cal_conf = make_calendar(calname + " (Conflicts)")

    for i, row in enumerate(rows):
        add_event(cal_all, row, uid_domain, mark_conflict=False)
        if i in conflict_set:
            add_event(cal_conf, row, uid_domain, mark_conflict=True)

    with open(ics_path, "wb") as f:
        f.write(cal_all.to_ical())

    with open(conflicts_path, "wb") as f:
        f.write(cal_conf.to_ical())
    return rows, conflict_set


def main():
    args = parse_args()
    rows = load_rows(args.xlsx)
    state = quality.load_state(args.state)

    published, conflicts = write_ics(rows, args.calname, args.ics, args.ics_conflicts, args.uid_domain, state)

    if args.pbs_ics:
        # Always derive both feeds from the same reconciled dataset.
        pbs_rows = [row for row in rows if row["organizer"] == "Predator/PBS"]
        pbs_calname = args.pbs_calname or (args.calname + " (PBS)")
        pbs_conflicts = args.pbs_ics_conflicts or _default_conflicts_path(args.pbs_ics)
        write_ics(pbs_rows, pbs_calname, args.pbs_ics, pbs_conflicts, args.uid_domain, state)
    if args.state:
        quality.save_state(state, args.state)
    if args.report:
        report = json.loads(Path(args.report).read_text(encoding="utf-8"))
        report["calendar"] = {
            "source_records": len(rows), "events": len(published),
            "duplicates_removed": len(rows) - len(published), "overlapping_events": len(conflicts),
            "by_year": dict(sorted(Counter(str(r["start"].year) for r in published).items())),
            "date_disagreements": sum(r["date_disagreement"] for r in published),
            "merged": [{"title": r["title"], "event_id": r["event_id"],
                        "source_ids": [s["event_id"] for s in r["sources"]]} for r in published if len(r["sources"]) > 1],
        }
        Path(args.report).write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        if args.status_text:
            quality.write_status_text(report, args.status_text)


if __name__ == "__main__":
    main()
