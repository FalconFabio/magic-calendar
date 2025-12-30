#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import json
import re
import sys
import time
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from functools import lru_cache
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup
from icalendar import Calendar
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# -------------------------
# Sources
# -------------------------
WPA_FEEDS = {
    # All + catégories officielles (WPA calendar page exposes these)
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

PBS_FALLBACK_URLS = [
    "https://77billiards.com/2025/12/10/predator-pro-billiard-series-reveals-stacked-2026-schedule/",
    "https://alison-chang.com/us-pro-billiard-series-announces-2026-season-schedule-across-four-major-cities/",
]

DEFAULT_TIMEOUT = 25


# -------------------------
# Model
# -------------------------
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

    @property
    def start_iso(self) -> str:
        return self.start.isoformat()

    @property
    def end_iso(self) -> str:
        return self.end.isoformat()


# -------------------------
# HTTP
# -------------------------
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


# -------------------------
# Utils
# -------------------------
def norm_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def slug(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-zA-Z0-9]+", "-", s).strip("-").lower()


def ics_dt_to_date(x) -> date:
    return x.date() if isinstance(x, datetime) else x


def is_upcoming_or_ongoing(start_d: date, end_d: date, from_d: date) -> bool:
    return (start_d >= from_d) or (start_d < from_d <= end_d)


def clean_title(s: str) -> str:
    t = norm_spaces(s)
    t = re.sub(r"(?i)\bshow poster\b", "", t).strip()
    t = norm_spaces(t)
    return t


# -------------------------
# Location rules
# -------------------------
BAD_LOCATION_RE = re.compile(
    r"(?i)\b("
    r"ical|outlook|export|subscribe|add to|google|calendar|share|print|download|"
    r"tickets?|prize fund|more info|read more|countdown"
    r")\b"
)


def is_bad_location(loc: Optional[str]) -> bool:
    if not loc:
        return True
    s = norm_spaces(loc)
    if not s:
        return True
    # "00" / "000" etc.
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

    # EPBF often uses "City / Country"
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

    return None if is_bad_location(s) else s


def location_precision(loc: Optional[str]) -> int:
    """
    0 = none
    1 = country-only (no comma)
    2 = city+... (has comma)
    """
    if not loc:
        return 0
    s = norm_spaces(loc)
    return 2 if "," in s else 1


# -------------------------
# JSON-LD extraction (Event -> locality/region/country)
# -------------------------
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
    """
    Returns "City, Region, Country" or "City, Country" when available.
    Avoids returning event name.
    """
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


# -------------------------
# Generic page location extraction (WPA/Matchroom/EPBF detail)
# -------------------------
@lru_cache(maxsize=1024)
def fetch_location_from_page(url: str) -> Optional[str]:
    try:
        html = http_get(url)
    except Exception:
        return None

    soup = BeautifulSoup(html, "lxml")

    # 1) JSON-LD (best)
    loc = extract_location_from_jsonld(soup)
    if loc:
        return loc

    # 2) MEC / common selectors
    selectors = [
        ".mec-single-event .mec-event-location",
        ".mec-single-event-location",
        ".mec-event-meta-item-location",
        ".mec-event-meta .mec-event-location",
        ".mec-event-location",
        # icon-based blocks (often MEC)
        "i.mec-sl-location",
        "i.mec-fa-map-marker",
        "i.fa-map-marker",
    ]

    for sel in selectors:
        el = soup.select_one(sel)
        if not el:
            continue

        # if it's an icon, read its parent container
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

    # 3) Text label fallback
    text = soup.get_text("\n")
    m = re.search(r"(?im)^\s*(location|venue)\s*[:\-]\s*(.+?)\s*$", text)
    if m:
        return normalize_location(m.group(2))

    return None


# -------------------------
# Matchroom schedule fallback (STRICT, no title-as-location)
# -------------------------
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
    """
    Ex:
      "Chinese Taipei Open Taipei City, Taiwan Prize Fund: ..." -> "Taipei City, Taiwan"
      "WNT Legends Manila, Philippines" -> "Manila, Philippines"
      "2026 UK Open ... Brentwood, Essex, UK" -> "Brentwood, Essex, UK"
    """
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

    # 3+ segments: city, region, country
    country = parts[-1]
    region = parts[-2]
    city_seg = parts[-3]
    city = _extract_tail_place_words(city_seg, max_words=3)
    return normalize_location(f"{city}, {region}, {country}")


def matchroom_location_is_suspicious(loc: Optional[str]) -> bool:
    if not loc:
        return True
    s = norm_spaces(loc)
    # If it contains obvious event keywords, it's probably title-ish, except venue words
    if re.search(r"(?i)\b(wnt|open|championship|cup|legends|pool|premier|league)\b", s):
        if not re.search(r"(?i)\b(arena|hotel|resort|centre|center|club|hall)\b", s):
            return True
    if len(s) > 80:
        return True
    return False


# -------------------------
# WPA detail URL extraction from ICS
# -------------------------
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

    # last resort: scan all properties
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


# -------------------------
# Date parsing (EPBF / Matchroom)
# -------------------------
MONTH_ABBR = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12
}
MONTH_FULL = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12
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


# -------------------------
# Fetchers
# -------------------------
def fetch_wpa_ics(from_d: date, enrich_limit: int, sleep_s: float) -> List[Tournament]:
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

                if not is_upcoming_or_ongoing(start_d, end_d, from_d):
                    continue

                detail_url = extract_detail_url_from_ical(comp)

                loc = normalize_location(norm_spaces(str(comp.get("location", ""))))
                if not loc:
                    loc = parse_wpa_location_from_description(comp.get("description"))

                # Enrich from event page (only if still missing)
                if not loc and detail_url and enrich_count < enrich_limit:
                    cand = fetch_location_from_page(detail_url)
                    loc = normalize_location(cand)
                    enrich_count += 1
                    if sleep_s > 0:
                        time.sleep(sleep_s)

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
                    )
                )

        except Exception as e:
            print(f"[WARN] WPA feed failed {label}: {e}", file=sys.stderr)

    return out


def fetch_matchroom(from_d: date, enrich_limit: int, sleep_s: float) -> List[Tournament]:
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

        if not is_upcoming_or_ongoing(start_d, end_d, from_d):
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

        # 1) Try event page (best) if under limit
        if enrich_count < enrich_limit:
            cand = fetch_location_from_page(href)
            cand = normalize_location(cand)
            enrich_count += 1
            if sleep_s > 0:
                time.sleep(sleep_s)
            if cand and not matchroom_location_is_suspicious(cand):
                loc = cand

        # 2) Fallback: strict parsing from title tail
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
            )
        )

    return out


def find_epbf_table_columns(table: BeautifulSoup) -> Optional[Dict[str, int]]:
    """
    Try to map columns by header names.
    Common EPBF headers: Date | Tournament | Place (or Location)
    """
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

    # location might be missing from headers on some tables
    return {"date": i_date, "title": i_title, "loc": i_loc if i_loc is not None else 2}


@lru_cache(maxsize=256)
def fetch_epbf_location_from_link(url: str) -> Optional[str]:
    # skip posters
    if re.search(r"(?i)\.(pdf|jpg|jpeg|png|webp)$", url):
        return None
    return normalize_location(fetch_location_from_page(url))


def fetch_epbf(year: int, from_d: date, enrich_limit: int, sleep_s: float) -> List[Tournament]:
    url = EPBF_CALENDAR_YEAR_URL.format(year=year)
    html = http_get(url)
    soup = BeautifulSoup(html, "lxml")

    out: List[Tournament] = []
    enrich_count = 0

    tables = soup.find_all("table")
    if not tables:
        return out

    for table in tables:
        colmap = find_epbf_table_columns(table)
        if not colmap:
            continue

        rows = table.find_all("tr")
        for tr in rows[1:]:  # skip header
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

            if not is_upcoming_or_ongoing(start_d, end_d, from_d):
                continue

            loc_raw = ""
            if colmap["loc"] is not None and len(tds) > colmap["loc"]:
                loc_raw = norm_spaces(tds[colmap["loc"]].get_text(" ", strip=True))
            loc = normalize_location(loc_raw)

            # pick a link to enrich (HTML page preferred, not poster)
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

            # Enrich if missing OR country-only (low precision) and we have a link
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
                )
            )

    return out


def fetch_pbs_fallback(from_d: date) -> List[Tournament]:
    out: List[Tournament] = []

    def norm_dash(s: str) -> str:
        return (
            (s or "")
            .replace("\u2013", "-")
            .replace("\u2014", "-")
            .replace("–", "-")
            .replace("—", "-")
        )

    date_rx = re.compile(
        r"^(?P<mon1>Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\s+"
        r"(?P<d1>\d{1,2})\s*-\s*"
        r"(?:(?P<mon2>Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\s+)?"
        r"(?P<d2>\d{1,2})$",
        re.IGNORECASE,
    )

    def mon_to_int(mon: str) -> int:
        m = (mon or "").strip().lower()
        if m == "sept":
            m = "sep"
        return MONTH_ABBR[m]

    for url in PBS_FALLBACK_URLS:
        try:
            html = http_get(url)
        except Exception as e:
            print(f"[WARN] PBS fallback URL failed: {url}: {e}", file=sys.stderr)
            continue

        soup = BeautifulSoup(html, "lxml")
        text = soup.get_text("\n")

        lines: List[str] = []
        for raw in text.splitlines():
            s = norm_dash(raw.strip())
            if not s:
                continue
            if s.lower().startswith(("total:", "share", "related", "leave a comment")):
                continue
            lines.append(s)

        assumed_year = 2026
        prev_location: Optional[str] = None

        for s in lines:
            m = date_rx.match(s)
            if m:
                if not prev_location:
                    continue
                start_d = date(assumed_year, mon_to_int(m.group("mon1")), int(m.group("d1")))
                mon2 = mon_to_int(m.group("mon2") or m.group("mon1"))
                end_d = date(assumed_year, mon2, int(m.group("d2")))

                if not is_upcoming_or_ongoing(start_d, end_d, from_d):
                    continue

                out.append(
                    Tournament(
                        title=f"Predator Pro Billiard Series — {prev_location}",
                        organizer="Predator/PBS",
                        start=start_d,
                        end=end_d,
                        location=normalize_location(prev_location),
                        tour=f"PBS {assumed_year} (fallback)",
                        source="Fallback article scrape",
                        source_url=url,
                    )
                )
            else:
                prev_location = norm_spaces(s)

    return out


# -------------------------
# Post-processing: dedup + cross-fill locations
# -------------------------
def choose_better(a: Tournament, b: Tournament) -> Tournament:
    """
    Prefer the one with higher precision location; otherwise keep a,
    but merge missing location if b has it.
    """
    pa = location_precision(a.location)
    pb = location_precision(b.location)

    if pb > pa:
        return b
    if pa > pb:
        return a

    # same precision: merge missing if any
    if not a.location and b.location:
        return Tournament(**{**a.__dict__, "location": b.location})
    return a


def dedup(tournaments: List[Tournament]) -> List[Tournament]:
    seen: Dict[str, Tournament] = {}
    for t in tournaments:
        key = f"{slug(t.title)}|{t.start_iso}"
        if key not in seen:
            seen[key] = t
        else:
            seen[key] = choose_better(seen[key], t)
    return list(seen.values())


def title_tokens(s: str) -> set[str]:
    # remove years and short tokens
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
    """
    If an event has no location or only country-level, try to fill it from another
    event with very similar dates and similar title where the location is more precise.
    """
    by_start: Dict[date, List[Tournament]] = {}
    for t in tournaments:
        by_start.setdefault(t.start, []).append(t)

    updated: List[Tournament] = []
    for t in tournaments:
        if location_precision(t.location) == 2:
            updated.append(t)
            continue

        # candidates same start date (and close end date)
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


# -------------------------
# Export / Debug
# -------------------------
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
                "source": t.source,
                "source_url": t.source_url,
            }
        )
    df = pd.DataFrame(rows).sort_values(["start_date", "title"])
    df.to_excel(out_path, index=False)


def print_missing_locations(tournaments: List[Tournament], limit: int = 30) -> None:
    missing = [t for t in tournaments if not t.location]
    if not missing:
        print("✅ No missing locations.")
        return
    print(f"⚠️ Missing locations: {len(missing)} (showing {min(limit, len(missing))})")
    for t in missing[:limit]:
        print(f"  - {t.start_iso} {t.organizer} | {t.title} | {t.source_url}")


# -------------------------
# CLI
# -------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--from", dest="from_date", default=None, help="YYYY-MM-DD (default=today)")
    p.add_argument("--years", type=int, default=2, help="EPBF years to fetch (current + next by default)")
    p.add_argument("--out", default="tournaments.xlsx", help="Output xlsx")
    p.add_argument("--wpa-enrich-limit", type=int, default=250, help="Max WPA event pages fetched for location")
    p.add_argument("--matchroom-enrich-limit", type=int, default=250, help="Max Matchroom event pages fetched for location")
    p.add_argument("--epbf-enrich-limit", type=int, default=250, help="Max EPBF internal pages fetched for location")
    p.add_argument("--sleep", type=float, default=0.0, help="Optional sleep between page fetches (seconds)")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    from_d = date.today() if not args.from_date else date.fromisoformat(args.from_date)
    years = max(1, int(args.years))

    all_t: List[Tournament] = []

    all_t.extend(fetch_wpa_ics(from_d, enrich_limit=max(0, args.wpa_enrich_limit), sleep_s=max(0.0, args.sleep)))
    all_t.extend(fetch_matchroom(from_d, enrich_limit=max(0, args.matchroom_enrich_limit), sleep_s=max(0.0, args.sleep)))
    all_t.extend(fetch_pbs_fallback(from_d))

    for y in range(from_d.year, from_d.year + years):
        try:
            all_t.extend(fetch_epbf(y, from_d, enrich_limit=max(0, args.epbf_enrich_limit), sleep_s=max(0.0, args.sleep)))
        except Exception as e:
            print(f"[WARN] EPBF {y} failed: {e}", file=sys.stderr)

    # Dedup then cross-fill then sort
    all_t = dedup(all_t)
    all_t = cross_fill_locations(all_t)
    all_t = sorted(all_t, key=lambda x: (x.start, x.title))

    export_excel(all_t, args.out)

    print(f"From: {from_d.isoformat()}")
    print(f"Fetched {len(all_t)} events. First 25:")
    for t in all_t[:25]:
        loc = f" @ {t.location}" if t.location else ""
        print(f"- {t.start_iso} → {t.end_iso} | {t.organizer} | {t.title}{loc}")

    print_missing_locations(all_t, limit=40)
    print(f"\nWrote: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
