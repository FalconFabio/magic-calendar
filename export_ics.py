#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import hashlib
from datetime import date, datetime, timedelta
from typing import List, Dict

import pandas as pd
from icalendar import Calendar, Event


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
    return cal


def add_event(cal: Calendar, row: Dict, uid_domain: str, mark_conflict: bool):
    ev = Event()

    start_d: date = row["start"]
    end_d: date = row["end"]

    ev.add("dtstart", start_d)
    ev.add("dtend", end_d + timedelta(days=1))  # all-day, exclusive DTEND

    title = row["title"]
    if mark_conflict:
        title = f"⚠ {title}"
        ev.add("categories", "CONFLICT")
    ev.add("summary", title)

    if row.get("location"):
        ev.add("location", row["location"])

    # Stable UID: prefer event_id (hash of URL from fetcher)
    if row.get("event_id"):
        ev.add("uid", f"{row['event_id']}@{uid_domain}")
    else:
        ev.add("uid", stable_uid_fallback(row["organizer"], row["start_iso"], row["end_iso"], row["title"], uid_domain))

    desc_lines = []

    # extras
    for label, key in [
        ("Venue", "venue_name"),
        ("Address", "venue_address"),
        ("Prize fund", "prize_fund"),
        ("Discipline", "discipline"),
    ]:
        v = row.get(key)
        if v:
            desc_lines.append(f"{label}: {v}")

    desc_lines.extend(
        [
            f"Organizer: {row.get('organizer','')}",
            f"Tour: {row.get('tour','')}",
            f"Source: {row.get('source','')}",
            f"URL: {row.get('source_url','')}",
        ]
    )

    ev.add("description", "\n".join([l for l in desc_lines if l and str(l).strip()]))

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


def write_ics(rows: List[Dict], calname: str, ics_path: str, conflicts_path: str, uid_domain: str):
    conflict_set = build_conflict_set(rows)

    cal_all = make_calendar(calname)
    cal_conf = make_calendar(calname + " (Conflicts)")

    for i, row in enumerate(rows):
        add_event(cal_all, row, uid_domain, mark_conflict=(i in conflict_set))
        if i in conflict_set:
            add_event(cal_conf, row, uid_domain, mark_conflict=True)

    with open(ics_path, "wb") as f:
        f.write(cal_all.to_ical())

    with open(conflicts_path, "wb") as f:
        f.write(cal_conf.to_ical())


def main():
    args = parse_args()
    rows = load_rows(args.xlsx)

    write_ics(rows, args.calname, args.ics, args.ics_conflicts, args.uid_domain)

    if args.pbs_ics:
        pbs_rows = load_rows(args.pbs_xlsx)
        pbs_calname = args.pbs_calname or (args.calname + " (PBS)")
        pbs_conflicts = args.pbs_ics_conflicts or _default_conflicts_path(args.pbs_ics)
        write_ics(pbs_rows, pbs_calname, args.pbs_ics, pbs_conflicts, args.uid_domain)


if __name__ == "__main__":
    main()
