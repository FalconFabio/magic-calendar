"""Bounded GET requests with retry, response validation and per-run caching."""
import re
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse

import requests


class FetchError(RuntimeError):
    pass


class HTTPClient:
    def __init__(self, session=None, sleep=time.sleep, clock=time.monotonic):
        self.session = session or requests.Session()
        self.session.headers.update({
            "User-Agent": "MagicCalendar/1.0 (+https://falconfabio.github.io/magic-calendar/)",
            "Accept-Language": "en-US,en;q=0.9,fr;q=0.8",
        })
        self.sleep, self.clock = sleep, clock
        self.cache = {}
        self.failures = Counter()
        self.blocked = set()
        self.stats = defaultdict(Counter)

    @staticmethod
    def retry_delay(value, attempt):
        try:
            delay = float(value)
        except (TypeError, ValueError):
            try:
                delay = (parsedate_to_datetime(value) - datetime.now(timezone.utc)).total_seconds()
            except (TypeError, ValueError, OverflowError):
                delay = 2 ** attempt
        return max(0, delay)

    def get_text(self, url, headers=None, cache=True, max_bytes=8_000_000):
        host = urlparse(url).netloc
        key = (url, tuple(sorted((headers or {}).items())))
        stats = self.stats[host]
        if cache and key in self.cache:
            stats["cache_hits"] += 1
            return self.cache[key]
        if host in self.blocked or self.failures[host] >= 2:
            stats["skipped"] += 1
            raise FetchError(f"{host}: requests paused for this run after repeated failures")
        deadline = self.clock() + 90
        last_error = None
        for attempt in range(3):
            response = None
            retry_after = None
            retryable = True
            try:
                remaining = deadline - self.clock()
                if remaining <= 0:
                    raise FetchError("request time budget exceeded")
                stats["attempts"] += 1
                response = self.session.get(url, headers=headers, timeout=(min(5, remaining), min(20, remaining)), stream=True)
                code = response.status_code
                if code >= 400:
                    retryable = code in {408, 429, 500, 502, 503, 504}
                    retry_after = response.headers.get("Retry-After")
                    if code in {401, 403}:
                        self.failures[host] += 1
                    response.raise_for_status()
                body = bytearray()
                for chunk in response.iter_content(chunk_size=65536):
                    body.extend(chunk)
                    if len(body) > max_bytes:
                        retryable = False
                        raise FetchError("response exceeds size limit")
                    if self.clock() > deadline:
                        raise FetchError("request time budget exceeded")
                encoding = response.encoding or "utf-8"
                text = bytes(body).decode(encoding, errors="replace")
                if not text.strip():
                    raise FetchError("empty response")
                # A Cloudflare footer alone is not a challenge page.
                title = re.search(r"<title[^>]*>(.*?)</title>", text[:100000], re.I | re.S)
                if (title and re.search(r"just a moment|access denied|attention required|security verification", title[1], re.I)) or "cf-chl-opt" in text:
                    self.blocked.add(host)
                    retryable = False
                    raise FetchError("access challenge returned instead of source data")
                stats["successes"] += 1
                self.failures[host] = 0
                if cache:
                    self.cache[key] = text
                return text
            except (requests.RequestException, FetchError) as exc:
                last_error = exc
                # A missing future-year page does not disable other years.
                if not retryable:
                    break
                delay = self.retry_delay(retry_after, attempt)
                if delay > 15 or self.clock() + delay >= deadline:
                    # Respect a long Retry-After by deferring the host to the next run.
                    self.blocked.add(host)
                    break
                if attempt < 2:
                    stats["retries"] += 1
                    if response is not None:
                        response.close()
                        response = None
                    self.sleep(delay)
            finally:
                if response is not None:
                    response.close()
        stats["failures"] += 1
        if retryable:
            self.failures[host] += 1
        raise FetchError(f"{url}: {last_error}") from last_error

    def summary(self):
        return {host: dict(values) for host, values in sorted(self.stats.items())}
