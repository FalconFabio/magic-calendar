#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import time
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from functools import lru_cache
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from icalendar import Calendar
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =========================================================
# Sources
# =========================================================
WPA_FEEDS = {
    "WPA_ALL": "https://wpapool.com/?mec-ical-feed=1",
    "WPA_HEYBALL": "https://wpapool.com/?mec-ical-feed=1&mec_categories=100",
    "WPA_MATCHROOM": "https://wpapool.com/?mec-ical-feed=1&mec_categories=63",
    "WPA_MEMBER_EVENT": "https://wpapool.com/?mec-ical-feed=1&mec_categories=60",
    "WPA_PREDATOR": "https://wpapool.com/?mec-ical-feed=1&mec_categories=61",
    "WPA_JUNIOR": "https://wpapool.com/?mec-ical-feed=1&mec_categories=59",
    "WPA_RANKING_MEN": "https://wpapool.com/?mec-ical-feed=1&mec_categories=58",
    "WPA_RANKING_MEN_WOMEN": "https://wpapool.com/?mec-ical-feed=1&mec_categories=50",
    "WPA_RANKING_WOMEN": "https://wpapool.com/?mec-ical-feed=1&mec_categories=62",
}

EPBF_CALENDAR_YEAR_URL = "https://www.epbf.com/calendar/{year}/"
MATCHROOM_SCHEDULE_URL = "https://matchroompool.com/schedule/"

PBS_EVENTS_ARCHIVE_URL = "https://probilliardseries.com/events-archive/"
PBS_BASES = ["https://probilliardseries.com", "https://www.probilliardseries.com"]

DEFAULT_TIMEOUT = 25

# =========================================================
# Model
# =========================================================
@dataclass(frozen=True)
class Tournament:
    title: str
    organizer: str
    start: date
    end: date  # inclusive
    location: Optional[str]
    tour: Optional[str]
    source: str
    source_url: str

    # PBS extras (optional)
    event_id: Optional[str] = None
    venue_name: Optional[str] = None
    venue_address: Optional[str] = None
    prize_fund: Optional[str] = None
    discipline: Optional[str] = None

    @property
    def start_iso(self) -> str:
        return self.start.isoformat()

    @property
    def end_iso(self) -> str:
        return self.end.isoformat()


# =========================================================
# HTTP
# =========================================================
def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9,fr-FR;q=0.8,fr;q=0.7",
        }
    )

    retries = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=0.8,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://", HTTPAdapter(max_retries=retries))
    return s


SESSION = build_session()


def http_get(url: str) -> str:
    r = SESSION.get(url, timeout=DEFAULT_TIMEOUT)
    r.raise_for_status()
    return r.text


# =========================================================
# Utils
# =========================================================
def norm_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def slug(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-zA-Z0-9]+", "-", s).strip("-").lower()


def clean_title(s: str) -> str:
    t = norm_spaces(s)
    t = re.sub(r"(?i)\bshow poster\b", "", t).strip()
    t = norm_spaces(t)
    return t


def ics_dt_to_date(x) -> date:
    return x.date() if isinstance(x, datetime) else x


def stable_event_id_from_url(url: str) -> str:
    return hashlib.sha1(url.encode("utf-8")).hexdigest()


def abs_url(href: str, base: str) -> str:
    h = (href or "").strip()
    if not h:
        return ""
    if h.startswith(("http://", "https://")):
        return h
    if h.startswith("/"):
        return base.rstrip("/") + h
    return base.rstrip("/") + "/" + h


def in_window(start_d: date, end_d: date, from_d: date, to_d: date) -> bool:
    return (end_d >= from_d) and (start_d <= to_d)


def add_years(d: date, years: int) -> date:
    try:
        return d.replace(year=d.year + years)
    except ValueError:
        return d.replace(month=2, day=28, year=d.year + years)


def strip_leading_year(s: str) -> str:
    """
    "2026 Las Vegas, USA" -> "Las Vegas, USA"
    """
    if not s:
        return s
    m = re.match(r"^(20\d{2})\s+(.+)$", s.strip())
    return m.group(2).strip() if m else s.strip()


# =========================================================
# Location rules
# =========================================================
BAD_LOCATION_RE = re.compile(
    r"(?i)\b("
    r"ical|outlook|export|subscribe|add to|google|calendar|share|print|download|"
    r"tickets?|prize fund|more info|read more|countdown"
    r")\b"
)

MONTH_WORDS = [
    "january","february","march","april","may","june","july","august","september","october","november","december",
    "jan","feb","mar","apr","jun","jul","aug","sep","sept","oct","nov","dec"
]

def looks_like_date_text(s: str) -> bool:
    """
    Detects things like:
      "January 12 - January 15, 2022"
      "Feb 24 - 27, 2026"
      "24 Feb - 27 Feb, 2026"
    """
    if not s:
        return False
    low = s.lower()
    if " - " not in low and "-" not in low:
        return False
    if not re.search(r"\b20\d{2}\b", low):
        return False
    # needs at least one day number
    if not re.search(r"\b\d{1,2}\b", low):
        return False
    if not any(re.search(rf"\b{m}\b", low) for m in MONTH_WORDS):
        return False
    return True


def is_bad_location(loc: Optional[str]) -> bool:
    if not loc:
        return True
    s = norm_spaces(loc)
    if not s:
        return True
    if looks_like_date_text(s):
        return True
    if re.fullmatch(r"\d{1,6}", s):
        return True
    if BAD_LOCATION_RE.search(s):
        return True
    if len(s) > 140:
        return True
    if s.startswith(("+", "•")):
        return True
    return False


def normalize_location(loc: Optional[str]) -> Optional[str]:
    if not loc:
        return None
    s = norm_spaces(loc)
    s = s.replace("Tukey", "Turkey")

    if " / " in s:
        left, right = [p.strip() for p in s.split(" / ", 1)]
        if left.upper() == "TBA" and right.upper() == "TBA":
            return None
        if left.upper() == "TBA":
            return None if right.upper() == "TBA" else right
        if right.upper() == "TBA":
            return None if left.upper() == "TBA" else left
        s = f"{left}, {right}"

    if s.upper() == "TBA":
        return None

    s = strip_leading_year(s)

    return None if is_bad_location(s) else s


def location_precision(loc: Optional[str]) -> int:
    if not loc:
        return 0
    return 2 if "," in norm_spaces(loc) else 1


# =========================================================
# JSON-LD extraction
# =========================================================
def _iter_jsonld_objects(soup: BeautifulSoup) -> Iterable[Any]:
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = script.string or script.get_text(strip=True)
        if not raw:
            continue
        raw = raw.strip()
        try:
            yield json.loads(raw)
        except Exception:
            continue


def _walk(obj: Any) -> Iterable[Any]:
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _walk(v)
    elif isinstance(obj, list):
        for it in obj:
            yield from _walk(it)


def _stringify_country(x: Any) -> str:
    if isinstance(x, str):
        return norm_spaces(x)
    if isinstance(x, dict):
        name = x.get("name") or x.get("@name") or x.get("addressCountry")
        if isinstance(name, str):
            return norm_spaces(name)
    return ""


def extract_location_from_jsonld(soup: BeautifulSoup) -> Optional[str]:
    for root in _iter_jsonld_objects(soup):
        for d in _walk(root):
            if not isinstance(d, dict):
                continue
            t = d.get("@type") or d.get("type")
            if isinstance(t, list):
                t = " ".join([str(x) for x in t])
            if not t or "Event" not in str(t):
                continue

            loc = d.get("location")
            loc_list = loc if isinstance(loc, list) else [loc]
            for place in loc_list:
                if not isinstance(place, dict):
                    continue
                addr = place.get("address")

                locality = region = country = ""
                if isinstance(addr, dict):
                    locality = norm_spaces(str(addr.get("addressLocality", "") or ""))
                    region = norm_spaces(str(addr.get("addressRegion", "") or ""))
                    country = _stringify_country(addr.get("addressCountry"))
                elif isinstance(addr, str):
                    parts = [p.strip() for p in addr.split(",") if p.strip()]
                    if len(parts) >= 2:
                        locality = parts[0]
                        country = parts[-1]
                        if len(parts) >= 3:
                            region = parts[-2]

                parts_out: List[str] = []
                if locality:
                    parts_out.append(locality)
                if region and region not in parts_out:
                    parts_out.append(region)
                if country and country not in parts_out:
                    parts_out.append(country)

                cand = normalize_location(", ".join(parts_out))
                if cand:
                    return cand
    return None


# =========================================================
# Generic page location extraction (WPA/Matchroom/EPBF detail)
# =========================================================
@lru_cache(maxsize=1024)
def fetch_location_from_page(url: str) -> Optional[str]:
    try:
        html = http_get(url)
    except Exception:
        return None

    soup = BeautifulSoup(html, "lxml")
    loc = extract_location_from_jsonld(soup)
    if loc:
        return loc

    selectors = [
        ".mec-single-event .mec-event-location",
        ".mec-single-event-location",
        ".mec-event-meta-item-location",
        ".mec-event-meta .mec-event-location",
        ".mec-event-location",
        "i.mec-sl-location",
        "i.mec-fa-map-marker",
        "i.fa-map-marker",
    ]

    for sel in selectors:
        el = soup.select_one(sel)
        if not el:
            continue

        if el.name == "i":
            parent = el.parent
            if parent:
                cand = norm_spaces(parent.get_text(" ", strip=True))
            else:
                continue
        else:
            cand = norm_spaces(el.get_text(" ", strip=True))

        cand = re.sub(r"(?i)^(location|venue)\s*[:\-]?\s*", "", cand).strip()
        cand = normalize_location(cand)
        if cand:
            return cand

    text = soup.get_text("\n")
    m = re.search(r"(?im)^\s*(location|venue)\s*[:\-]\s*(.+?)\s*$", text)
    if m:
        return normalize_location(m.group(2))

    return None


# =========================================================
# WPA detail URL extraction from ICS
# =========================================================
URL_RE = re.compile(r"https?://[^\s)>\"]+")


def extract_detail_url_from_ical(comp) -> Optional[str]:
    for k in ("url", "URL"):
        v = comp.get(k)
        if v:
            vv = norm_spaces(str(v))
            if vv.startswith(("http://", "https://")):
                return vv

    for k in ("uid", "UID"):
        v = comp.get(k)
        if v:
            vv = norm_spaces(str(v))
            if vv.startswith(("http://", "https://")):
                return vv

    desc = comp.get("description") or ""
    m = URL_RE.search(str(desc))
    if m:
        return m.group(0)

    try:
        for _, val in comp.property_items():
            s = str(val)
            m2 = URL_RE.search(s)
            if m2:
                u = m2.group(0)
                if u.startswith(("http://", "https://")):
                    return u
    except Exception:
        pass

    return None


def parse_wpa_location_from_description(desc: Optional[str]) -> Optional[str]:
    if not desc:
        return None
    txt = str(desc).replace("\r", "\n")
    m = re.search(r"(?im)^\s*location\s*:\s*(.+?)\s*$", txt)
    if not m:
        return None
    return normalize_location(m.group(1))


# =========================================================
# Date parsing
# =========================================================
MONTH_ABBR = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
}
MONTH_FULL = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
}


def parse_epbf_date_range(raw: str, year: int) -> Tuple[date, date]:
    s = norm_spaces(raw.lower().replace("–", "-"))
    m = re.match(r"^(\d{1,2})-(\d{1,2}) ([a-z]{3,4})$", s)
    if m:
        d1, d2, mon = int(m.group(1)), int(m.group(2)), m.group(3)
        return date(year, MONTH_ABBR[mon], d1), date(year, MONTH_ABBR[mon], d2)
    m = re.match(r"^(\d{1,2}) ([a-z]{3,4}) - (\d{1,2}) ([a-z]{3,4})$", s)
    if m:
        d1, mon1 = int(m.group(1)), m.group(2)
        d2, mon2 = int(m.group(3)), m.group(4)
        return date(year, MONTH_ABBR[mon1], d1), date(year, MONTH_ABBR[mon2], d2)
    m = re.match(r"^(\d{1,2}) ([a-z]{3,4}) - (\d{1,2})-(\d{1,2}) ([a-z]{3,4})$", s)
    if m:
        d1, mon1 = int(m.group(1)), m.group(2)
        d3, mon2 = int(m.group(4)), m.group(5)
        return date(year, MONTH_ABBR[mon1], d1), date(year, MONTH_ABBR[mon2], d3)
    raise ValueError(f"Unrecognized EPBF date format: {raw!r}")


def parse_matchroom_date_range(raw: str) -> Tuple[date, date, int]:
    s = norm_spaces(raw.replace("–", "-").replace("—", "-"))
    m = re.match(r"^([A-Za-z]+) (\d{1,2}) - (\d{1,2}) (\d{4})$", s)
    if m:
        mon, d1, d2, y = m.group(1).lower(), int(m.group(2)), int(m.group(3)), int(m.group(4))
        return date(y, MONTH_FULL[mon], d1), date(y, MONTH_FULL[mon], d2), y
    m = re.match(r"^([A-Za-z]+) (\d{1,2}) - ([A-Za-z]+) (\d{1,2}) (\d{4})$", s)
    if m:
        mon1, d1 = m.group(1).lower(), int(m.group(2))
        mon2, d2 = m.group(3).lower(), int(m.group(4))
        y = int(m.group(5))
        return date(y, MONTH_FULL[mon1], d1), date(y, MONTH_FULL[mon2], d2), y
    raise ValueError(f"Unrecognized Matchroom date format: {raw!r}")


# PBS date parsing supports:
# - "24 Feb - 27 Feb, 2026" (day first)
# - "February 24 - February 27, 2026" OR "Feb 24 - 27, 2026" (month first)
PBS_MONTH = r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec|January|February|March|April|May|June|July|August|September|October|November|December)"
PBS_RANGE_RE_A = re.compile(
    rf"^(?P<d1>\d{{1,2}})\s+(?P<m1>{PBS_MONTH})\s*-\s*(?P<d2>\d{{1,2}})\s+(?P<m2>{PBS_MONTH})\s*,?\s*(?P<y>20\d{{2}})$",
    re.IGNORECASE,
)
PBS_RANGE_RE_B = re.compile(
    rf"^(?P<m1>{PBS_MONTH})\s+(?P<d1>\d{{1,2}})\s*-\s*(?:(?P<m2>{PBS_MONTH})\s+)?(?P<d2>\d{{1,2}})\s*,?\s*(?P<y>20\d{{2}})$",
    re.IGNORECASE,
)


def _pbs_month_to_int(m: str) -> int:
    mm = (m or "").strip().lower()
    if mm == "sept":
        mm = "sep"
    if mm in MONTH_ABBR:
        return MONTH_ABBR[mm]
    return MONTH_FULL[mm]


def parse_pbs_date_range(raw: str) -> Optional[Tuple[date, date]]:
    s = norm_spaces((raw or "").replace("–", "-").replace("—", "-"))
    if not s:
        return None

    m = PBS_RANGE_RE_A.match(s)
    if m:
        y = int(m.group("y"))
        d1 = int(m.group("d1"))
        d2 = int(m.group("d2"))
        m1 = _pbs_month_to_int(m.group("m1"))
        m2 = _pbs_month_to_int(m.group("m2"))
        start = date(y, m1, d1)
        end = date(y, m2, d2)
        return None if end < start else (start, end)

    m = PBS_RANGE_RE_B.match(s)
    if m:
        y = int(m.group("y"))
        d1 = int(m.group("d1"))
        d2 = int(m.group("d2"))
        m1 = _pbs_month_to_int(m.group("m1"))
        m2 = _pbs_month_to_int(m.group("m2") or m.group("m1"))
        start = date(y, m1, d1)
        end = date(y, m2, d2)
        if end < start:
            try_end = date(y, m1, d2)
            if try_end >= start:
                end = try_end
            else:
                return None
        return start, end

    return None


# =========================================================
# Fetchers: WPA
# =========================================================
def fetch_wpa_ics(from_d: date, to_d: date, enrich_limit: int, sleep_s: float) -> List[Tournament]:
    out: List[Tournament] = []
    enrich_count = 0

    for label, feed_url in WPA_FEEDS.items():
        try:
            ics_text = http_get(feed_url)
            cal = Calendar.from_ical(ics_text)

            for comp in cal.walk():
                if comp.name != "VEVENT":
                    continue

                summary = clean_title(norm_spaces(str(comp.get("summary", ""))))
                if not summary:
                    continue

                start_d = ics_dt_to_date(comp.get("dtstart").dt)
                dtend = comp.get("dtend").dt if comp.get("dtend") else None
                end_d = start_d if dtend is None else (ics_dt_to_date(dtend) - timedelta(days=1))

                if not in_window(start_d, end_d, from_d, to_d):
                    continue

                detail_url = extract_detail_url_from_ical(comp)

                loc = normalize_location(norm_spaces(str(comp.get("location", ""))))
                if not loc:
                    loc = parse_wpa_location_from_description(comp.get("description"))

                if not loc and detail_url and enrich_count < enrich_limit:
                    cand = fetch_location_from_page(detail_url)
                    loc = normalize_location(cand)
                    enrich_count += 1
                    if sleep_s > 0:
                        time.sleep(sleep_s)

                ev_id = stable_event_id_from_url(detail_url or f"{feed_url}#{summary}#{start_d.isoformat()}")

                out.append(
                    Tournament(
                        title=summary,
                        organizer="WPA",
                        start=start_d,
                        end=end_d,
                        location=loc,
                        tour=label,
                        source="WPA iCal feed",
                        source_url=feed_url,
                        event_id=ev_id,
                    )
                )

        except Exception as e:
            print(f"[WARN] WPA feed failed {label}: {e}", file=sys.stderr)

    return out


# =========================================================
# Fetchers: Matchroom (inchangé, conservé)
# =========================================================
MATCHROOM_STOPWORDS = {
    "wnt", "open", "championship", "championships", "cup", "legends", "pool",
    "premier", "league", "ranking", "major", "non-ranking", "blue", "ribbon",
    "world", "international", "masters", "classic", "tour", "series", "women", "men",
}


def _extract_tail_place_words(segment: str, max_words: int = 3) -> str:
    seg = norm_spaces(segment)
    seg = re.sub(r"[^\wÀ-ÿ'\- ]+$", "", seg).strip()
    words = [w for w in seg.split(" ") if w]
    picked: List[str] = []
    for w in reversed(words):
        wl = w.lower().strip(".,()")
        if re.fullmatch(r"\d{1,4}", wl):
            continue
        if wl in MATCHROOM_STOPWORDS:
            break
        picked.append(w.strip(".,()"))
        if len(picked) >= max_words:
            break
    return norm_spaces(" ".join(reversed(picked)))


def parse_location_from_matchroom_title(title: str) -> Optional[str]:
    t = norm_spaces(title)
    t = re.split(r"(?i)\bprize fund\b", t)[0].strip()
    if "," not in t:
        return None

    parts = [p.strip() for p in t.split(",") if p.strip()]
    if len(parts) < 2:
        return None

    if len(parts) == 2:
        country_or_region = parts[-1]
        city_seg = parts[-2]
        city = _extract_tail_place_words(city_seg, max_words=3)
        return normalize_location(f"{city}, {country_or_region}")

    country = parts[-1]
    region = parts[-2]
    city_seg = parts[-3]
    city = _extract_tail_place_words(city_seg, max_words=3)
    return normalize_location(f"{city}, {region}, {country}")


def matchroom_location_is_suspicious(loc: Optional[str]) -> bool:
    if not loc:
        return True
    s = norm_spaces(loc)
    if re.search(r"(?i)\b(wnt|open|championship|cup|legends|pool|premier|league)\b", s):
        if not re.search(r"(?i)\b(arena|hotel|resort|centre|center|club|hall)\b", s):
            return True
    if len(s) > 80:
        return True
    return False


def fetch_matchroom(from_d: date, to_d: date, enrich_limit: int, sleep_s: float) -> List[Tournament]:
    html = http_get(MATCHROOM_SCHEDULE_URL)
    soup = BeautifulSoup(html, "lxml")

    out: List[Tournament] = []
    enrich_count = 0

    for a in soup.find_all("a"):
        txt = norm_spaces(a.get_text(" ", strip=True))
        if not txt or not re.search(r"\b20\d{2}\b", txt):
            continue
        if not re.match(
            r"^(January|February|March|April|May|June|July|August|September|October|November|December)\b",
            txt,
        ):
            continue

        m = re.match(r"^(.+?\b20\d{2}\b)\s+(.*)$", txt)
        if not m:
            continue

        date_part, rest = m.group(1).strip(), m.group(2).strip()
        try:
            start_d, end_d, _y = parse_matchroom_date_range(date_part)
        except Exception:
            continue

        if not in_window(start_d, end_d, from_d, to_d):
            continue

        event_type = None
        m2 = re.match(r"^(Ranking|Major|Non-Ranking|Junior|Blue Ribbon)\s+(.*)$", rest)
        if m2:
            event_type = m2.group(1)
            title = clean_title(m2.group(2).strip())
        else:
            title = clean_title(rest)

        href = a.get("href") or MATCHROOM_SCHEDULE_URL
        if not href.startswith("http"):
            href = f"https://matchroompool.com{href}"

        loc: Optional[str] = None

        if enrich_count < enrich_limit:
            cand = fetch_location_from_page(href)
            cand = normalize_location(cand)
            enrich_count += 1
            if sleep_s > 0:
                time.sleep(sleep_s)
            if cand and not matchroom_location_is_suspicious(cand):
                loc = cand

        if not loc:
            cand2 = parse_location_from_matchroom_title(title)
            cand2 = normalize_location(cand2)
            if cand2 and not matchroom_location_is_suspicious(cand2):
                loc = cand2

        out.append(
            Tournament(
                title=title,
                organizer="Matchroom",
                start=start_d,
                end=end_d,
                location=loc,
                tour=f"WNT ({event_type})" if event_type else "WNT",
                source="Matchroom schedule",
                source_url=href,
                event_id=stable_event_id_from_url(href),
            )
        )

    return out


# =========================================================
# Fetchers: EPBF
# =========================================================
def find_epbf_table_columns(table: BeautifulSoup) -> Optional[Dict[str, int]]:
    header_tr = table.find("tr")
    if not header_tr:
        return None
    headers = [norm_spaces(th.get_text(" ", strip=True)).lower() for th in header_tr.find_all(["th", "td"])]
    if not headers:
        return None

    def idx_of(*names: str) -> Optional[int]:
        for i, h in enumerate(headers):
            for n in names:
                if n in h:
                    return i
        return None

    i_date = idx_of("date")
    i_title = idx_of("tournament", "event", "competition", "name")
    i_loc = idx_of("place", "location", "country", "city")

    if i_date is None or i_title is None:
        return None

    return {"date": i_date, "title": i_title, "loc": i_loc if i_loc is not None else 2}


@lru_cache(maxsize=256)
def fetch_epbf_location_from_link(url: str) -> Optional[str]:
    if re.search(r"(?i)\.(pdf|jpg|jpeg|png|webp)$", url):
        return None
    return normalize_location(fetch_location_from_page(url))


def fetch_epbf(year: int, from_d: date, to_d: date, enrich_limit: int, sleep_s: float) -> List[Tournament]:
    url = EPBF_CALENDAR_YEAR_URL.format(year=year)
    html = http_get(url)
    soup = BeautifulSoup(html, "lxml")

    out: List[Tournament] = []
    tables = soup.find_all("table")
    if not tables:
        return out

    enrich_count = 0

    for table in tables:
        colmap = find_epbf_table_columns(table)
        if not colmap:
            continue

        rows = table.find_all("tr")
        for tr in rows[1:]:
            tds = tr.find_all(["td", "th"])
            if not tds or len(tds) <= colmap["title"]:
                continue

            raw_date = norm_spaces(tds[colmap["date"]].get_text(" ", strip=True)) if len(tds) > colmap["date"] else ""
            if not raw_date:
                continue

            title_cell = tds[colmap["title"]]
            title = clean_title(title_cell.get_text(" ", strip=True))
            if not title:
                continue

            try:
                start_d, end_d = parse_epbf_date_range(raw_date, year=year)
            except Exception:
                continue

            if not in_window(start_d, end_d, from_d, to_d):
                continue

            loc_raw = ""
            if colmap["loc"] is not None and len(tds) > colmap["loc"]:
                loc_raw = norm_spaces(tds[colmap["loc"]].get_text(" ", strip=True))
            loc = normalize_location(loc_raw)

            link_url = None
            for a in title_cell.find_all("a"):
                href = (a.get("href") or "").strip()
                if not href:
                    continue
                if href.startswith("/"):
                    href = f"https://www.epbf.com{href}"
                if re.search(r"(?i)\.(pdf|jpg|jpeg|png|webp)$", href):
                    continue
                if "epbf.com" in href:
                    link_url = href
                    break

            if (location_precision(loc) <= 1) and link_url and enrich_count < enrich_limit:
                cand = fetch_epbf_location_from_link(link_url)
                if cand and location_precision(cand) > location_precision(loc):
                    loc = cand
                enrich_count += 1
                if sleep_s > 0:
                    time.sleep(sleep_s)

            out.append(
                Tournament(
                    title=title,
                    organizer="EPBF",
                    start=start_d,
                    end=end_d,
                    location=loc,
                    tour="EPBF Calendar",
                    source="EPBF calendar table",
                    source_url=url,
                    event_id=stable_event_id_from_url(f"{url}#{title}#{start_d.isoformat()}"),
                )
            )

    return out


# =========================================================
# PBS: archive-driven crawling + location fix + stop fallback
# =========================================================
PBS_TYPES = {
    "US PBS Open",
    "PBS Open",
    "World Championship",
    "Invitational",
    "Qualifier",
    "Junior",
}

PBS_BLOCK_MARKERS = [
    "cloudflare",
    "checking your browser",
    "attention required",
    "access denied",
    "request blocked",
    "captcha",
    "cf-challenge",
    "cf-browser-verification",
]

PBS_URL_RE = re.compile(r"https?://(?:www\.)?probilliardseries\.com/event/[^\"'\s<>]+", re.IGNORECASE)


def _pbs_dump_debug(name: str, url: str, html: str) -> None:
    try:
        stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
        short = slug(url)[:40] or "page"
        fn = f"pbs_debug_{name}_{short}_{stamp}.html"
        with open(fn, "w", encoding="utf-8") as f:
            f.write(html or "")
        print(f"[WARN] PBS debug dumped -> {fn} ({len(html or '')} chars)", file=sys.stderr)
    except Exception:
        pass


def pbs_http_get(url: str) -> str:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,fr-FR;q=0.8,fr;q=0.7",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Referer": "https://probilliardseries.com/",
    }

    r = SESSION.get(url, headers=headers, timeout=60, allow_redirects=True)
    txt = r.text or ""

    low = txt.lower()
    suspicious = False
    if "/event/" not in txt:
        suspicious = True
    if any(m in low for m in PBS_BLOCK_MARKERS):
        suspicious = True

    if suspicious:
        _pbs_dump_debug("suspicious", url, txt)
        print(f"[WARN] PBS suspicious HTML from {url}. First 200 chars: {txt[:200]!r}", file=sys.stderr)

    r.raise_for_status()
    return txt


def _pbs_host_ok(host: str) -> bool:
    h = (host or "").lower()
    return h in {"probilliardseries.com", "www.probilliardseries.com"}


def is_pbs_tournament_url(url: str) -> bool:
    """
    Tournament page:
      /event/<stop>/<tournament>/
    """
    try:
        p = urlparse(url)
        if not _pbs_host_ok(p.netloc):
            return False
        parts = [x for x in (p.path or "").strip("/").split("/") if x]
        return len(parts) == 3 and parts[0] == "event" and bool(parts[1]) and bool(parts[2])
    except Exception:
        return False


def normalize_pbs_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    u = re.sub(r"^http://", "https://", u, flags=re.IGNORECASE)
    u = u.split("#", 1)[0]
    return u


def stop_url_from_tournament_url(u: str) -> Optional[str]:
    try:
        p = urlparse(u)
        parts = [x for x in (p.path or "").strip("/").split("/") if x]
        if len(parts) == 3 and parts[0] == "event":
            return f"{p.scheme}://{p.netloc}/event/{parts[1]}/"
    except Exception:
        pass
    return None


def year_hint_from_url(u: str) -> Optional[int]:
    m = re.search(r"\b(20\d{2})\b", u)
    return int(m.group(1)) if m else None


def collect_pbs_tournament_urls_from_archive() -> List[str]:
    html = ""
    last_err: Optional[Exception] = None

    for base in PBS_BASES:
        url = base.rstrip("/") + "/events-archive/"
        try:
            html = pbs_http_get(url)
            break
        except Exception as e:
            last_err = e
            continue

    if not html:
        raise RuntimeError(f"PBS archive fetch failed: {last_err}")

    urls: List[str] = []
    seen = set()

    try:
        soup = BeautifulSoup(html, "lxml")
        for a in soup.select("a[href*='/event/']"):
            href = (a.get("href") or "").strip()
            u = normalize_pbs_url(abs_url(href, "https://probilliardseries.com"))
            if u and is_pbs_tournament_url(u) and u not in seen:
                seen.add(u)
                urls.append(u)
    except Exception:
        pass

    if not urls:
        for m in PBS_URL_RE.finditer(html):
            u = normalize_pbs_url(m.group(0))
            if u and is_pbs_tournament_url(u) and u not in seen:
                seen.add(u)
                urls.append(u)

    urls.sort()
    print(f"[INFO] PBS archive: found {len(urls)} tournament urls", file=sys.stderr)

    if not urls:
        _pbs_dump_debug("archive_no_tournaments", PBS_EVENTS_ARCHIVE_URL, html)

    return urls


def parse_pbs_tournament_page_details(html: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "lxml")
    main = soup.find("main") or soup

    h1 = main.find(["h1", "h2"])
    title = clean_title(h1.get_text(" ", strip=True)) if h1 else ""

    tokens = [norm_spaces(t) for t in main.stripped_strings if norm_spaces(t)]

    # category
    cat = None
    for tok in tokens:
        if tok in PBS_TYPES:
            cat = tok
            break

    # date range
    start_d = end_d = None
    for tok in tokens[:260]:
        rng = parse_pbs_date_range(tok)
        if rng:
            start_d, end_d = rng
            break

    # location: JSON-LD first
    loc = extract_location_from_jsonld(soup)
    loc = normalize_location(loc) if loc else None

    # location fallback: pick a sensible token, but NEVER a date
    if not loc:
        for tok in tokens[:140]:
            if looks_like_date_text(tok):
                continue
            # require comma for city/country style
            if "," not in tok:
                continue
            cand = normalize_location(tok)
            if cand:
                loc = cand
                break

    # venue: "VENUE" section pattern
    venue_name = None
    venue_addr = None
    for i, tok in enumerate(tokens):
        if tok.upper() == "VENUE":
            tail = tokens[i + 1 : i + 70]
            for x in tail:
                if x.upper() in {"OFFICIAL EQUIPMENT", "HOTEL OPTIONS", "PRO BILLIARD TV", "EVENT INFO", "PLAYERS"}:
                    break
                if looks_like_date_text(x):
                    continue
                if not venue_name and len(x) >= 3 and not any(k in x.lower() for k in ["image", "watch", "show more"]):
                    venue_name = x
                    continue
                if venue_name and not venue_addr and any(ch.isdigit() for ch in x) and "," in x:
                    venue_addr = x
                    break
            break

    # prize fund
    prize = None
    for tok in tokens[:260]:
        m = re.search(r"(?i)\b([\d,]{1,})\s+Prize Fund\b", tok)
        if m:
            prize = m.group(1)
            break

    # discipline
    discipline = None
    disc_re = re.compile(r"(?i)\b(\d{1,2}\s*-\s*Ball|One Pocket|Straight Pool|Bank Pool|3-Cushion)\b")
    for tok in tokens[:260]:
        m = disc_re.search(tok)
        if m:
            discipline = norm_spaces(m.group(1)).replace(" - ", "-")
            break

    return {
        "title": title,
        "category": cat,
        "start": start_d,
        "end": end_d,
        "location": loc,
        "venue_name": venue_name,
        "venue_address": venue_addr,
        "prize_fund": prize,
        "discipline": discipline,
    }


@lru_cache(maxsize=128)
def fetch_pbs_stop_details(stop_url: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Fetch stop page and extract:
      - stop_location (city,country)
      - venue_name
      - venue_address
    """
    try:
        html = pbs_http_get(stop_url)
    except Exception:
        return None, None, None

    soup = BeautifulSoup(html, "lxml")
    main = soup.find("main") or soup
    tokens = [norm_spaces(t) for t in main.stripped_strings if norm_spaces(t)]

    # stop location: find first "X, Y" that is NOT a date
    stop_loc = None
    for tok in tokens[:120]:
        if looks_like_date_text(tok):
            continue
        if "," not in tok:
            continue
        cand = normalize_location(tok)
        if cand and len(cand) <= 80:
            stop_loc = cand
            break

    # venue details section
    venue_name = None
    venue_addr = None
    for i, tok in enumerate(tokens):
        if tok.upper() == "VENUE DETAILS":
            tail = tokens[i + 1 : i + 60]
            for x in tail:
                if x.upper() in {"RELATED NEWS", "OFFICIAL EQUIPMENT"}:
                    break
                if looks_like_date_text(x):
                    continue
                if not venue_name and len(x) >= 3 and not any(k in x.lower() for k in ["image", "watch", "show more"]):
                    venue_name = x
                    continue
                if venue_name and not venue_addr and any(ch.isdigit() for ch in x) and "," in x:
                    venue_addr = x
                    break
            break

    return stop_loc, venue_name, venue_addr


def fetch_pbs(from_d: date, to_d: date, stop_enrich_limit: int, sleep_s: float) -> List[Tournament]:
    """
    Dynamic PBS:
      - Get ALL tournament URLs from events-archive
      - Parse each tournament page
      - If location missing/weak, fill from stop page
      - If venue missing, fill from stop page
    """
    out: List[Tournament] = []

    try:
        tournament_urls = collect_pbs_tournament_urls_from_archive()
    except Exception as e:
        print(f"[WARN] PBS archive failed: {e}", file=sys.stderr)
        return out

    enrich_used = 0

    for u in tournament_urls:
        yh = year_hint_from_url(u)
        if yh is not None and (yh < from_d.year - 1 or yh > to_d.year + 1):
            continue

        try:
            html = pbs_http_get(u)
        except Exception as e:
            print(f"[WARN] PBS tournament fetch failed: {u}: {e}", file=sys.stderr)
            continue

        det = parse_pbs_tournament_page_details(html)

        title = (det.get("title") or "").strip()
        start_d = det.get("start")
        end_d = det.get("end")

        if not title or start_d is None or end_d is None:
            _pbs_dump_debug("tournament_parse_failed", u, html)
            continue

        if not in_window(start_d, end_d, from_d, to_d):
            continue

        cat = det.get("category")
        tour = f"PBS ({cat})" if cat else "PBS"

        loc = det.get("location")
        venue_name = det.get("venue_name")
        venue_addr = det.get("venue_address")

        # If location is missing (or very weak), enrich from stop page
        # (Note: after our fix, date strings won't pass normalize_location, so loc should be None rather than wrong.)
        needs_stop = (location_precision(loc) < 2) or (not venue_name) or (not venue_addr)

        if needs_stop and enrich_used < stop_enrich_limit:
            stop_u = stop_url_from_tournament_url(u)
            if stop_u:
                stop_loc, stop_vn, stop_va = fetch_pbs_stop_details(stop_u)
                if location_precision(stop_loc) > location_precision(loc):
                    loc = stop_loc
                if not venue_name and stop_vn:
                    venue_name = stop_vn
                if not venue_addr and stop_va:
                    venue_addr = stop_va

                enrich_used += 1
                if sleep_s > 0:
                    time.sleep(sleep_s)

        out.append(
            Tournament(
                title=title,
                organizer="Predator/PBS",
                start=start_d,
                end=end_d,
                location=loc,
                tour=tour,
                source="PBS events-archive (tournament pages)",
                source_url=u,
                event_id=stable_event_id_from_url(u),
                venue_name=venue_name,
                venue_address=venue_addr,
                prize_fund=det.get("prize_fund"),
                discipline=det.get("discipline"),
            )
        )

    return out


# =========================================================
# Post-processing: dedup + cross-fill locations
# =========================================================
def choose_better(a: Tournament, b: Tournament) -> Tournament:
    pa = location_precision(a.location)
    pb = location_precision(b.location)

    if pb > pa:
        return b
    if pa > pb:
        return a

    def pick(field: str) -> Optional[str]:
        va = getattr(a, field)
        vb = getattr(b, field)
        return va or vb

    return Tournament(
        title=a.title,
        organizer=a.organizer,
        start=a.start,
        end=a.end,
        location=a.location or b.location,
        tour=a.tour or b.tour,
        source=a.source or b.source,
        source_url=a.source_url or b.source_url,
        event_id=a.event_id or b.event_id,
        venue_name=pick("venue_name"),
        venue_address=pick("venue_address"),
        prize_fund=pick("prize_fund"),
        discipline=pick("discipline"),
    )


def dedup(tournaments: List[Tournament]) -> List[Tournament]:
    seen: Dict[str, Tournament] = {}
    for t in tournaments:
        key = (t.event_id or "").strip() or f"{slug(t.title)}|{t.start_iso}|{t.organizer}"
        if key not in seen:
            seen[key] = t
        else:
            seen[key] = choose_better(seen[key], t)
    return list(seen.values())


def title_tokens(s: str) -> set[str]:
    t = re.sub(r"\b20\d{2}\b", " ", s)
    toks = [x for x in slug(t).split("-") if len(x) >= 3]
    return set(toks)


def jaccard(a: str, b: str) -> float:
    sa = title_tokens(a)
    sb = title_tokens(b)
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)


def cross_fill_locations(tournaments: List[Tournament]) -> List[Tournament]:
    by_start: Dict[date, List[Tournament]] = {}
    for t in tournaments:
        by_start.setdefault(t.start, []).append(t)

    updated: List[Tournament] = []
    for t in tournaments:
        if location_precision(t.location) == 2:
            updated.append(t)
            continue

        cands = by_start.get(t.start, [])
        best_loc: Optional[str] = None
        best_score = 0.0

        for c in cands:
            if c is t:
                continue
            if location_precision(c.location) != 2:
                continue
            if abs((c.end - t.end).days) > 1:
                continue
            sim = jaccard(t.title, c.title)
            if sim < 0.28:
                continue
            if sim > best_score:
                best_score = sim
                best_loc = c.location

        if best_loc and (best_loc != t.location):
            updated.append(Tournament(**{**t.__dict__, "location": best_loc}))
        else:
            updated.append(t)

    return updated


# =========================================================
# Export
# =========================================================
def export_excel(tournaments: List[Tournament], out_path: str) -> None:
    rows = []
    for t in tournaments:
        rows.append(
            {
                "start_date": t.start_iso,
                "end_date": t.end_iso,
                "title": t.title,
                "organizer": t.organizer,
                "tour": t.tour,
                "location": t.location,
                "venue_name": t.venue_name,
                "venue_address": t.venue_address,
                "prize_fund": t.prize_fund,
                "discipline": t.discipline,
                "event_id": t.event_id,
                "source": t.source,
                "source_url": t.source_url,
            }
        )
    df = pd.DataFrame(rows).sort_values(["start_date", "organizer", "title"])
    df.to_excel(out_path, index=False)


def print_missing_locations(tournaments: List[Tournament], limit: int = 30) -> None:
    missing = [t for t in tournaments if not t.location]
    if not missing:
        print("✅ No missing locations.")
        return
    print(f"⚠️ Missing locations: {len(missing)} (showing {min(limit, len(missing))})")
    for t in missing[:limit]:
        print(f"  - {t.start_iso} {t.organizer} | {t.title} | {t.source_url}")


# =========================================================
# CLI
# =========================================================
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()

    p.add_argument("--from", dest="from_date", default=None, help="YYYY-MM-DD (earliest date to keep)")
    p.add_argument("--to", dest="to_date", default=None, help="YYYY-MM-DD (latest date to keep)")

    p.add_argument("--years-past", type=int, default=5, help="Years back from today (default window)")
    p.add_argument("--years-future", type=int, default=3, help="Years forward from today (default window)")

    p.add_argument("--out", default="tournaments.xlsx", help="Output xlsx")

    p.add_argument("--wpa-enrich-limit", type=int, default=200, help="Max WPA event pages fetched for location")
    p.add_argument("--matchroom-enrich-limit", type=int, default=200, help="Max Matchroom pages fetched for location")
    p.add_argument("--epbf-enrich-limit", type=int, default=200, help="Max EPBF internal pages fetched for location")

    # PBS stop enrichment budget (location/venue fallback)
    p.add_argument("--pbs-enrich-limit", type=int, default=200, help="Max PBS stop pages fetched for enrichment")
    p.add_argument("--sleep", type=float, default=0.0, help="Optional sleep between page fetches (seconds)")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    today = date.today()

    if args.from_date:
        from_d = date.fromisoformat(args.from_date)
    else:
        from_d = add_years(today, -max(0, int(args.years_past)))

    if args.to_date:
        to_d = date.fromisoformat(args.to_date)
    else:
        to_d = add_years(today, max(0, int(args.years_future)))

    if to_d < from_d:
        raise SystemExit("--to must be >= --from (or adjust --years-past/--years-future)")

    all_t: List[Tournament] = []

    # PBS FIRST (priority)
    all_t.extend(fetch_pbs(from_d, to_d, stop_enrich_limit=max(0, args.pbs_enrich_limit), sleep_s=max(0.0, args.sleep)))

    # WPA / Matchroom
    all_t.extend(fetch_wpa_ics(from_d, to_d, enrich_limit=max(0, args.wpa_enrich_limit), sleep_s=max(0.0, args.sleep)))
    all_t.extend(fetch_matchroom(from_d, to_d, enrich_limit=max(0, args.matchroom_enrich_limit), sleep_s=max(0.0, args.sleep)))

    # EPBF years in window
    for y in range(from_d.year, to_d.year + 1):
        try:
            all_t.extend(fetch_epbf(y, from_d, to_d, enrich_limit=max(0, args.epbf_enrich_limit), sleep_s=max(0.0, args.sleep)))
        except Exception as e:
            print(f"[WARN] EPBF {y} failed: {e}", file=sys.stderr)

    all_t = dedup(all_t)
    all_t = cross_fill_locations(all_t)
    all_t = sorted(all_t, key=lambda x: (x.start, x.organizer, x.title))

    export_excel(all_t, args.out)

    print(f"Window: {from_d.isoformat()} → {to_d.isoformat()}")
    print(f"Fetched {len(all_t)} events. First 25:")
    for t in all_t[:25]:
        loc = f" @ {t.location}" if t.location else ""
        print(f"- {t.start_iso} → {t.end_iso} | {t.organizer} | {t.title}{loc}")

    print_missing_locations(all_t, limit=40)
    print(f"\nWrote: {args.out}")

    pbs_count = sum(1 for t in all_t if t.organizer == "Predator/PBS")
    print(f"[INFO] PBS rows: {pbs_count}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
