"""Checksummed, reviewable backup of all source records, including future events."""
import hashlib
import json
from pathlib import Path


def digest(records):
    content = json.dumps(records, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def load(path):
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    records = data.get("events")
    if data.get("schema_version") != 1 or not isinstance(records, list) or not records:
        raise ValueError("Invalid or empty tournament backup")
    if data.get("sha256") != digest(records):
        raise ValueError("Tournament backup checksum mismatch")
    return records


def save(records, path):
    if not records:
        raise ValueError("Refusing to save an empty tournament backup")
    data = {"schema_version": 1, "sha256": digest(records), "events": records}
    content = json.dumps(data, ensure_ascii=False, indent=2) + "\n"
    target = Path(path)
    if not target.exists() or target.read_text(encoding="utf-8") != content:
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_suffix(".json.tmp")
        temporary.write_text(content, encoding="utf-8")
        temporary.replace(target)
