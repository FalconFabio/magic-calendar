"""Conservative event matching, source checks, and persistent change tracking."""
import json
import re
import unicodedata
from datetime import date
from pathlib import Path


PRIORITY = {"Predator/PBS": 0, "Matchroom": 1, "EPBF": 2, "WPA": 3}
STATUS_WORDS = r"\b(cancelled|canceled|annule|annulee|postponed|reporte|reportee)\b"
COUNTRIES = set("usa united states america uk gbr united kingdom england scotland france spain esp italy ita sweden swe austria aut germany ger poland pol china chn taiwan chinese indonesia ina malaysia mas vietnam vie thailand tha philippines phi romania rom czech republic cze estonia est belgium bel netherlands ned australia aus norway nor japan jpn canada can turkey turkiye finland fin portugal por luxembourg lux denmark den bosnia herzegovina bahrain qatar uae arab emirates singapore sin slovenia slo albania alb latvia lat city".split())
COUNTRY_ALIASES = {
    "US": ("usa", "united states", "united states of america"), "GB": ("uk", "gbr", "united kingdom", "england", "scotland"),
    "FR": ("france", "fra"), "ES": ("spain", "esp"), "TW": ("taiwan", "chinese taipei", "tpe"),
    "CN": ("china", "chn"), "ID": ("indonesia", "ina"), "MY": ("malaysia", "mas"),
    "VN": ("vietnam", "vie"), "TH": ("thailand", "tha"), "PH": ("philippines", "phi"),
    "IT": ("italy", "ita"), "SE": ("sweden", "swe"), "AT": ("austria", "aut"),
    "DE": ("germany", "ger"), "PL": ("poland", "pol"), "BE": ("belgium", "bel"),
    "EE": ("estonia", "est"), "RO": ("romania", "rom"), "CZ": ("czech republic", "cze"),
    "AU": ("australia", "aus"), "JP": ("japan", "jpn"), "CA": ("canada", "can"),
}


def normalized(text):
    text = unicodedata.normalize("NFKD", text or "").encode("ascii", "ignore").decode().lower()
    return " ".join(re.findall(r"[a-z0-9]+", text))


def title_key(text):
    text = re.sub(STATUS_WORDS, "", normalized(text))
    return re.sub(r"\s+", " ", re.sub(r"\b20\d{2}\b", "", text)).strip()


def event_status(title):
    text = normalized(title)
    if re.search(r"\b(not cancelled|not canceled|non annule|non annulee)\b", text):
        return ""
    if re.search(r"\b(cancelled|canceled|annule|annulee)\b", text):
        return "CANCELLED"
    if re.search(r"\b(postponed|reporte|reportee|tba|tbc|tbd)\b", text):
        return "TENTATIVE"
    return ""


def location_tokens(text):
    words = normalized(text).split()
    aliases = {"fl": "florida", "tx": "texas", "ny": "new york"}
    words = " ".join(aliases.get(w, w) for w in words).split()
    return set(words) - COUNTRIES - {"tpe", "fra"}


def locations_compatible(a, b):
    if not a or not b:
        return True
    if normalized(a) == normalized(b):
        return True
    def country(text):
        text = " " + normalized(text)
        return next((code for code, aliases in COUNTRY_ALIASES.items() if any(text.endswith(" " + alias) for alias in aliases)), None)
    left_country, right_country = country(a), country(b)
    if left_country and right_country and left_country != right_country:
        return False
    left, right = location_tokens(a), location_tokens(b)
    # Country alone is insufficient to decide that two venues are the same.
    return bool(left and right and (left <= right or right <= left))


def same_event(a, b):
    key = title_key(a["title"])
    if a["organizer"] == b["organizer"] or len(key.split()) < 2 or key != title_key(b["title"]):
        return False
    if a["start"].year != b["start"].year:
        return False
    if abs((a["start"] - b["start"]).days) > 1 or abs((a["end"] - b["end"]).days) > 1:
        return False
    if a["end"] < b["start"] or b["end"] < a["start"]:
        return False
    if a.get("discipline") and b.get("discipline") and normalized(a["discipline"]) != normalized(b["discipline"]):
        return False
    if not a.get("location") or not b.get("location"):
        return a["start"] == b["start"] and a["end"] == b["end"]
    return locations_compatible(a["location"], b["location"])


def load_state(path):
    if not path or not Path(path).exists():
        return {"schema_version": 1, "events": {}, "canonical": {}, "changes": [], "incidents": []}
    state = json.loads(Path(path).read_text(encoding="utf-8"))
    if state.get("schema_version") != 1 or not isinstance(state.get("events"), dict):
        raise ValueError("Invalid calendar monitoring state")
    return state


def epbf_id_replacements(fresh, previous, refresh_from):
    """EPBF hashes its dates into IDs; retain the UID for an unambiguous nearby move."""
    def key(row):
        return (title_key(row["title"]), normalized(row.get("location")), str(row["start_date"])[:4])
    old_groups, new_groups = {}, {}
    previous_ids = {r["event_id"] for r in previous}
    for row in previous:
        if row["organizer"] == "EPBF" and date.fromisoformat(row["end_date"]) >= refresh_from:
            old_groups.setdefault(key(row), []).append(row)
    for row in fresh:
        if row["organizer"] == "EPBF":
            new_groups.setdefault(key(row), []).append(row)
    replacements = {}
    for match, new in new_groups.items():
        old = old_groups.get(match, [])
        if len(new) != 1 or len(old) != 1 or not match[1] or new[0]["event_id"] in previous_ids:
            continue
        gap = abs((date.fromisoformat(new[0]["start_date"]) - date.fromisoformat(old[0]["start_date"])).days)
        if gap <= 14:
            replacements[new[0]["event_id"]] = old[0]["event_id"]
    return replacements


def save_state(state, path):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temp = target.with_suffix(".json.tmp")
    temp.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temp.replace(target)


def consolidate(rows, state=None):
    state = state or {"events": {}, "canonical": {}}
    groups = []
    for row in sorted(rows, key=lambda r: (PRIORITY.get(r["organizer"], 9), r["event_id"])):
        candidates = [g for g in groups if all(same_event(row, other) for other in g)]
        if len(candidates) == 1:
            candidates[0].append(row)
        else:
            groups.append([row])
    merged, aliases = [], {}
    for group in groups:
        # Keep a surviving subscriber UID, even if another source is fresher today.
        ids = {r["event_id"] for r in group}
        existing = [state.get("canonical", {}).get(r["event_id"]) for r in group]
        canonical = next((uid for uid in existing if uid in ids), group[0]["event_id"])
        def preference(row):
            verification = state.get("events", {}).get(row["event_id"], {}).get("verification")
            return (verification in {"stale", "missing"}, PRIORITY.get(row["organizer"], 9))
        primary = min(group, key=preference)
        row = dict(primary)
        row["event_id"] = canonical
        row["sources"] = [dict(r, monitoring=state.get("events", {}).get(r["event_id"], {})) for r in group]
        row["status"] = event_status(primary["title"])
        row["monitoring"] = state.get("events", {}).get(primary["event_id"], {})
        row["date_disagreement"] = len({(r["start"], r["end"]) for r in group}) > 1
        # Enrichment comes only from a verified matching event, never a loose title match.
        for field in ("location", "venue_name", "venue_address", "prize_fund", "discipline"):
            if not row.get(field):
                row[field] = next((r[field] for r in group if r.get(field)), "")
        merged.append(row)
        aliases.update({uid: canonical for uid in ids})
    state.setdefault("canonical", {}).update(aliases)
    return sorted(merged, key=lambda r: (r["start"], r["organizer"], r["title"]))


def snapshot(row):
    return {k: row.get(k) or "" for k in ("title", "start_date", "end_date", "location")}


def source_issues(errors, previous, today):
    issues = []
    for error in errors:
        label = error.split(":", 1)[0]
        match = re.fullmatch(r"EPBF (\d{4})", label)
        if match and int(match.group(1)) > today.year:
            announced = any(r["organizer"] == "EPBF" and str(r["start_date"]).startswith(match.group(1)) for r in previous)
            if not announced and "404" in error:
                continue  # A calendar for an unannounced future season may not exist yet.
        if label not in {issue["source"] for issue in issues}:
            issues.append({"source": label, "message": f"{label} : collecte indisponible ou incomplète ; données précédentes conservées.", "detail": error})
    return issues


def update_monitoring(state, records, fresh, previous, refresh_from, now, errors):
    today = now.date()
    stamp = now.isoformat()
    fresh_ids = {r["event_id"] for r in fresh}
    previous_by_id = {r["event_id"]: r for r in previous}
    fresh_sources = {(r["organizer"], str(r["start_date"])[:4]) for r in fresh}
    changes = []
    for row in records:
        uid = row["event_id"]
        old = state["events"].get(uid, {})
        previous_snapshot = old.get("snapshot") or (snapshot(previous_by_id[uid]) if uid in previous_by_id else None)
        current = snapshot(row)
        end = date.fromisoformat(str(row["end_date"])[:10])
        is_fresh = uid in fresh_ids and end >= refresh_from
        if is_fresh:
            verification = "current"
        elif end < today:
            verification = "archived"
        elif (row["organizer"], str(row["start_date"])[:4]) in fresh_sources:
            verification = "missing"
        else:
            verification = "stale"
        status = event_status(row["title"])
        if verification == "missing" and status == "CANCELLED":
            verification = "previous"
        entry = dict(old, snapshot=current, verification=verification, status=status)
        if is_fresh:
            entry["last_verified"] = stamp
        if is_fresh and previous_snapshot and previous_snapshot != current:
            fields = [k for k in current if current[k] != previous_snapshot[k]]
            changes.append({"event_id": uid, "title": row["title"], "at": stamp, "kind": "changed",
                            "fields": fields, "before": previous_snapshot, "after": current})
            entry["last_changed"] = stamp
            entry["change"] = {"at": stamp, "before": previous_snapshot, "after": current}
        elif verification == "missing" and old.get("verification") != "missing":
            changes.append({"event_id": uid, "title": row["title"], "at": stamp, "kind": "missing"})
        state["events"][uid] = entry
    incidents = source_issues(errors, previous, today)
    missing = [e for e in state["events"].values() if e.get("verification") == "missing"]
    if missing:
        incidents.append({"source": "missing-events", "message": f"{len(missing)} événement(s) à reconfirmer : absents de la dernière collecte."})
    before = {i["source"] for i in state.get("incidents", [])}
    new_alerts = [i for i in incidents if i["source"] not in before and i["source"] != "missing-events"]
    new_alerts += [{"source": f"missing:{c['event_id']}", "message": f"{c['title']} : absent de la dernière collecte, à reconfirmer."}
                   for c in changes if c["kind"] == "missing"]
    resolved = [i for i in state.get("incidents", []) if i["source"] not in {x["source"] for x in incidents}]
    state["incidents"] = incidents
    state["changes"] = (state.get("changes", []) + changes)[-200:]
    return {"incidents": incidents, "new_alerts": new_alerts, "resolved": resolved,
            "changes": changes, "recent_changes": state["changes"][-20:]}


def describe_changes(change):
    if change["kind"] == "missing":
        return f"{change['title']} : absent de la dernière collecte, à reconfirmer."
    before, after = change["before"], change["after"]
    details = []
    if before["start_date"] != after["start_date"] or before["end_date"] != after["end_date"]:
        details.append(f"dates {before['start_date']} / {before['end_date']} → {after['start_date']} / {after['end_date']}")
    if before["location"] != after["location"]:
        details.append(f"lieu {before['location'] or 'non précisé'} → {after['location'] or 'non précisé'}")
    if before["title"] != after["title"]:
        details.append(f"intitulé : {after['title']}")
    return f"{change['title']} : " + "; ".join(details)


def write_status_text(report, path):
    calendar = report.get("calendar", {})
    monitoring = report.get("monitoring", {})
    lines = ["Calendrier des tournois — état de la synchronisation", "",
             f"Dernière collecte : {report['generated_at']} (UTC)",
             f"Événements affichés : {calendar.get('events', '?')}",
             f"Doublons regroupés : {calendar.get('duplicates_removed', 0)}",
             f"Entrées sources archivées : {report.get('archive', {}).get('completed_events', 0)}", "", "État des sources"]
    lines += [i["message"] for i in monitoring.get("incidents", [])] or ["Aucun incident détecté."]
    if report.get("publication_mode") == "formatting_only":
        lines += ["", "Mise en forme actualisée à partir des données sauvegardées, sans nouvelle collecte."]
    if report.get("collection"):
        lines += ["", "Collecte par source"]
        for c in report["collection"]:
            if c.get("status") == "not_published":
                lines.append(f"{c['source']} : saison pas encore publiée.")
            else:
                lines.append(f"{c['source']} : {c['events']} entrées récupérées en {c['seconds']} s" + (" — contrôle nécessaire" if c["issues"] else ""))
    lines += ["", "Derniers changements"]
    lines += [f"{c['at'][:10]} — {describe_changes(c)}" for c in reversed(monitoring.get("recent_changes", []))] or ["Aucun changement détecté depuis l'activation du suivi."]
    lines += ["", "Une disparition d'un site n'est pas considérée comme une annulation.",
              "Les archives complètes sont conservées. Les doublons sont regroupés uniquement dans l'affichage du calendrier."]
    Path(path).write_text("\n".join(lines) + "\n", encoding="utf-8")
