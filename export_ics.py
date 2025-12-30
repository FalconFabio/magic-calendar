#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import hashlib
from datetime import date, datetime, timedelta
from typing import List, Dict, Tuple

import pandas as pd
from icalendar import Calendar, Event


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--xlsx", required=True, help="Input tournaments.xlsx")
    p.add_argument("--ics", required=True, help="Output tournaments.ics")
    p.add_argument("--ics-conflicts", required=True, help="Output tournaments-conflicts.ics")
    p.add_argument("--calname", default="US Pool – Tournaments", help="Calendar display name")
    p.add_argument("--uid-domain", default="uspool.local", help="UID suffix domain")
    return p.parse_args()


def stable_uid(organizer: str, start: str, end: str, title: str, uid_domain: str) -> str:
    raw = f"{organizer}|{start}|{end}|{title}".encode("utf-8")
    h = hashlib.sha1(raw).hexdigest()
    return f"{h}@{uid_domain}"


def to_date(s: str) -> date:
    # start_date/end_date are ISO yyyy-mm-dd
    return datetime.fromisoformat(str(s)).date()


def build_conflict_set(rows: List[Dict]) -> set:
    """
    Returns a set of indices that are in conflict with at least one other event.
    Overlap is inclusive on days (same day counts).
    """
    # sort by start
    indexed = list(enumerate(rows))
    indexed.sort(key=lambda x: (x[1]["start"], x[1]["end"], x[1]["title"]))

    conflict = set()
    for i in range(len(indexed)):
        idx_i, a = indexed[i]
        for j in range(i + 1, len(indexed)):
            idx_j, b = indexed[j]
            # if next event starts after a ends, break (because sorted by start)
            if b["start"] > a["end"]:
                break
            # overlap
            if a["start"] <= b["end"] and b["start"] <= a["end"]:
                conflict.add(idx_i)
                conflict.add(idx_j)
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

    # all-day, DTEND is exclusive -> end + 1 day
    ev.add("dtstart", start_d)
    ev.add("dtend", end_d + timedelta(days=1))

    title = row["title"]
    if mark_conflict:
        title = f"⚠ {title}"
        ev.add("categories", "CONFLICT")

    ev.add("summary", title)

    if row.get("location"):
        ev.add("location", row["location"])

    # Stable UID to avoid duplicates on refresh
    ev.add("uid", stable_uid(row["organizer"], row["start_iso"], row["end_iso"], row["title"], uid_domain))

    # Useful description
    desc_lines = [
        f"Organizer: {row.get('organizer','')}",
        f"Tour: {row.get('tour','')}",
        f"Source: {row.get('source','')}",
        f"URL: {row.get('source_url','')}",
    ]
    ev.add("description", "\n".join([l for l in desc_lines if l.strip()]))

    # Also set URL if present
    if row.get("source_url"):
        ev.add("url", row["source_url"])

    cal.add_component(ev)


def main():
    args = parse_args()
    df = pd.read_excel(args.xlsx)

    # Normalize rows
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
                "title": str(r["title"]),
                "organizer": str(r["organizer"]),
                "tour": "" if pd.isna(r.get("tour")) else str(r.get("tour")),
                "location": None if pd.isna(r.get("location")) else str(r.get("location")).strip(),
                "source": "" if pd.isna(r.get("source")) else str(r.get("source")),
                "source_url": "" if pd.isna(r.get("source_url")) else str(r.get("source_url")),
            }
        )

    conflict_set = build_conflict_set(rows)

    cal_all = make_calendar(args.calname)
    cal_conf = make_calendar(args.calname + " (Conflicts)")

    for i, row in enumerate(rows):
        add_event(cal_all, row, args.uid_domain, mark_conflict=(i in conflict_set))
        if i in conflict_set:
            add_event(cal_conf, row, args.uid_domain, mark_conflict=True)

    with open(args.ics, "wb") as f:
        f.write(cal_all.to_ical())

    with open(args.ics_conflicts, "wb") as f:
        f.write(cal_conf.to_ical())


if __name__ == "__main__":
    main()
