#!/usr/bin/env python3
"""
ctm_daily_executive_summary.py

Fetches yesterday's CTM call activities, aggregates operational and AI-derived
metrics by agent and source, generates an email-safe HTML executive summary,
and posts it to a Make.com webhook for Gmail delivery.

Usage:
    python3 ctm_daily_executive_summary.py [options]

See README.md or --help for full option reference.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import html
import json
import logging
import os
import re
import time
from collections import Counter, defaultdict
from pathlib import Path
from statistics import mean
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urlparse

import requests

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_BASE_URL = "https://api.calltrackingmetrics.com/api/v1"
DEFAULT_TIME_DURATION = "yesterday"
DEFAULT_TIMEZONE = "America/Chicago"
DEFAULT_MAX_DETAIL_ROWS = 75
DEFAULT_SLEEP_S = 0.1
DEFAULT_PER_PAGE = 100

# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------


def _load_env_file() -> dict[str, str]:
    """Load key:value pairs from an ENV.txt file next to this script."""
    env_path = Path(__file__).with_name("ENV.txt")
    if not env_path.exists():
        return {}
    data: dict[str, str] = {}
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or ":" not in line:
            continue
        key, _, val = line.partition(":")
        val = val.strip().strip('"').strip("'")
        if val.startswith("{") and val.endswith("}"):
            val = val[1:-1].strip()
        data[key.strip()] = val
    return data


def _resolve_auth_header() -> str:
    """
    Resolve the CTM Basic Auth header value from the environment.
    Checks CTM_API_KEY, CTM_AUTH, CTM_BASIC_AUTH in that order,
    then falls back to ENV.txt.
    Raises SystemExit if no credential is found.
    """
    env = _load_env_file()
    for key in ("CTM_API_KEY", "CTM_AUTH", "CTM_BASIC_AUTH"):
        val = os.environ.get(key) or env.get(key)
        if val:
            val = val.strip()
            return val if val.lower().startswith("basic ") else f"Basic {val}"
    raise SystemExit(
        "No CTM credentials found. Set CTM_API_KEY (or CTM_AUTH / CTM_BASIC_AUTH) "
        "as an environment variable or in ENV.txt."
    )


def _resolve_config() -> dict[str, str]:
    """Merge env vars and ENV.txt into a single config dict."""
    env = _load_env_file()
    keys = (
        "CTM_ACCOUNT_ID",
        "CTM_WEBHOOK_URL",
        "CTM_LOGO_URL",
        "CTM_ONVERTED_FIELD",
        "CTM_SCORE_FIELD",
    )
    return {k: os.environ.get(k) or env.get(k, "") for k in keys}


# ---------------------------------------------------------------------------
# Safe value helpers
# ---------------------------------------------------------------------------


def safe_dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def safe_list(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


def safe_str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def safe_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def normalize_wait_seconds(wait_time: Any) -> float:
    value = safe_float(wait_time)
    if value is None:
        return 0.0
    # CTM sometimes returns milliseconds
    return value / 1000.0 if value > 1000 else value


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------


def fmt_num(value: Any, digits: int = 1) -> str:
    if value is None:
        return "—"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def fmt_pct(value: Any, digits: int = 1) -> str:
    if value is None:
        return "—"
    return f"{float(value):.{digits}f}%"


def fmt_sec(value: Any) -> str:
    if value is None:
        return "—"
    seconds = float(value)
    if seconds <= 0:
        return "0s"
    minutes = int(seconds // 60)
    remainder = int(round(seconds % 60))
    return f"{remainder}s" if minutes == 0 else f"{minutes}m {remainder:02d}s"


def esc(value: Any) -> str:
    return "" if value is None else html.escape(str(value))


def split_multi_value(value: Any) -> List[str]:
    if value is None:
        return []
    parts = value if isinstance(value, list) else re.split(r"[;\n|]+", str(value))
    return [
        item for part in parts
        if (item := safe_str(part)) and item.lower() not in {"none", "null", "n/a", "na"}
    ]


def top_items(counter: Any, limit: int = 5) -> str:
    if not counter:
        return "—"
    if hasattr(counter, "most_common"):
        items = counter.most_common(limit)
    elif isinstance(counter, dict):
        items = sorted(counter.items(), key=lambda kv: kv[1], reverse=True)[:limit]
    else:
        return "—"
    return ", ".join(f"{name} ({count})" for name, count in items)


def path_from_url(url_or_path: str) -> str:
    if not url_or_path:
        return ""
    if url_or_path.startswith("http"):
        parsed = urlparse(url_or_path)
        return parsed.path + (f"?{parsed.query}" if parsed.query else "")
    return url_or_path


# ---------------------------------------------------------------------------
# CTM API client
# ---------------------------------------------------------------------------


class CTMClient:
    def __init__(
        self,
        account_id: str,
        base_url: str,
        auth_header: str,
        timeout: int = 60,
    ) -> None:
        self.account_id = account_id
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": auth_header,
                "Accept": "application/json",
                "User-Agent": "ctm-daily-executive-summary/1.0",
            }
        )

    def get(
        self,
        path_or_url: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Any:
        path = path_from_url(path_or_url)
        if path.startswith("/api/v1"):
            url = f"{self.base_url}{path[len('/api/v1'):]}"
        elif path.startswith("/"):
            url = f"{self.base_url}{path}"
        else:
            url = f"{self.base_url}/{path}"

        for attempt in range(3):
            try:
                resp = self.session.get(url, params=params or {}, timeout=self.timeout)
            except requests.RequestException as exc:
                if attempt < 2:
                    log.warning("Request failed (%s) — retrying in 2 s", exc)
                    time.sleep(2)
                    continue
                raise

            if resp.status_code == 429:
                log.warning("Rate limited — sleeping 5 s")
                time.sleep(5)
                continue

            if resp.status_code == 401:
                raise SystemExit(
                    "CTM authentication failed (401). Check your CTM_API_KEY."
                )

            resp.raise_for_status()
            return resp.json()

        raise SystemExit("CTM API request failed after 3 attempts.")

    def fetch_calls(
        self,
        time_duration: str = DEFAULT_TIME_DURATION,
        per_page: int = DEFAULT_PER_PAGE,
    ) -> List[Dict[str, Any]]:
        calls: List[Dict[str, Any]] = []
        next_path: str = f"/accounts/{self.account_id}/calls"
        params: Optional[Dict[str, Any]] = {
            "per_page": per_page,
            "time_duration": time_duration,
        }

        while next_path:
            log.info("Fetching calls: %s", next_path)
            payload = self.get(next_path, params=params)
            batch = payload.get("calls", []) if isinstance(payload, dict) else []
            calls.extend(c for c in batch if isinstance(c, dict))
            log.info("  -> %d calls so far", len(calls))

            next_page = payload.get("next_page") if isinstance(payload, dict) else None
            next_path = path_from_url(next_page) if next_page else ""
            params = None  # params are embedded in next_page URL

        return calls

    def fetch_call_detail(self, call_id: Any) -> Dict[str, Any]:
        return self.get(f"/accounts/{self.account_id}/calls/{call_id}")


# ---------------------------------------------------------------------------
# Call hydration
# ---------------------------------------------------------------------------


def merge_call(base_call: Dict[str, Any], detail_call: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(base_call)
    for key, value in detail_call.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            nested = dict(merged[key])
            nested.update(value)
            merged[key] = nested
        else:
            merged[key] = value
    return merged


def hydrate_calls(
    client: CTMClient,
    calls: List[Dict[str, Any]],
    hydrate_details: bool,
    sleep_s: float,
) -> List[Dict[str, Any]]:
    if not hydrate_details:
        return calls

    hydrated: List[Dict[str, Any]] = []
    for index, call in enumerate(calls, start=1):
        call_id = call.get("id")
        needs_detail = not (
            call.get("custom_fields") is not None
            and call.get("summary") is not None
            and call.get("agent") is not None
        )

        if not call_id or not needs_detail:
            hydrated.append(call)
            continue

        try:
            detail = client.fetch_call_detail(call_id)
            hydrated.append(merge_call(call, detail))
        except Exception as exc:
            log.warning("Failed to fetch detail for call %s: %s", call_id, exc)
            hydrated.append(call)

        if sleep_s:
            time.sleep(sleep_s)
        if index % 25 == 0:
            log.info("Hydrated %d / %d calls", index, len(calls))

    return hydrated


# ---------------------------------------------------------------------------
# Data normalization
# ---------------------------------------------------------------------------


def normalize_yes_no(value: Any) -> str:
    text = safe_str(value).lower()
    if text in {"yes", "true", "scheduled", "booked"}:
        return "Yes"
    if text in {"no", "false", "not scheduled"}:
        return "No"
    return safe_str(value, "Unknown")


def extract_call_record(
    call: Dict[str, Any],
    converted_field: str = "did_the_caller_schedule",
    score_field: str = "cumulative_score_percentage",
) -> Dict[str, Any]:
    cf = safe_dict(call.get("custom_fields"))
    agent = safe_dict(call.get("agent"))
    missed_questions = split_multi_value(cf.get("missed_questions"))
    score = safe_float(cf.get(score_field))

    return {
        "id": call.get("id"),
        "sid": safe_str(call.get("sid")),
        "called_at": safe_str(call.get("called_at")),
        "unix_time": safe_int(call.get("unix_time")),
        "source": safe_str(call.get("source"), "Unknown"),
        "direction": safe_str(call.get("direction"), "Unknown"),
        "status": safe_str(call.get("status") or call.get("call_status"), "Unknown"),
        "dial_status": safe_str(call.get("dial_status"), "Unknown"),
        "agent_name": safe_str(
            agent.get("name") or agent.get("email") or agent.get("id"), "Unassigned"
        ),
        "agent_email": safe_str(agent.get("email")),
        "duration": safe_int(call.get("duration")),
        "talk_time": safe_int(call.get("talk_time")),
        "ring_time": safe_int(call.get("ring_time")),
        "hold_time": safe_int(call.get("hold_time")),
        "wait_time_s": normalize_wait_seconds(call.get("wait_time")),
        "is_new_caller": bool(call.get("is_new_caller")),
        "summary": safe_str(call.get("summary")),
        "service_type": safe_str(cf.get("service_type"), "Unknown"),
        "call_outcome": safe_str(cf.get("call_outcome"), "Unknown"),
        "did_schedule": normalize_yes_no(cf.get(converted_field)),
        "objection": safe_str(cf.get("objections_reasons_not_scheduled"), "None"),
        "missed_questions": missed_questions,
        "missed_questions_text": "; ".join(missed_questions) if missed_questions else "None",
        "score": score,
        "rating": safe_float(cf.get("agent_star_rating")),
        "explanation_of_outcome": safe_str(cf.get("explanation_of_outcome")),
        "call_type": safe_str(cf.get("call_type")),
        "caller_name": safe_str(
            call.get("name") or safe_dict(call.get("caller")).get("name")
        ),
        "city": safe_str(call.get("city")),
        "state": safe_str(call.get("state")),
        "tracking_label": safe_str(call.get("tracking_label")),
    }


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------


def build_overview(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    inbound = [r for r in records if r["direction"].lower() == "inbound"]
    answered = sum(
        1 for r in inbound
        if r["dial_status"].lower() == "answered" or r["status"].lower() == "answered"
    )
    scheduled = sum(1 for r in inbound if r["did_schedule"] == "Yes")
    avg_score_values = [r["score"] for r in records if r["score"] is not None]

    return {
        "total_calls": len(records),
        "answered_calls": answered,
        "answered_rate": (answered / len(inbound) * 100) if inbound else 0.0,
        "scheduled_calls": scheduled,
        "scheduled_rate": (scheduled / len(inbound) * 100) if inbound else 0.0,
        "inbound_calls": len(inbound),
        "outbound_calls": sum(1 for r in records if r["direction"].lower() == "outbound"),
        "summarized_calls": sum(1 for r in records if r["summary"]),
        "new_callers": sum(1 for r in records if r["is_new_caller"]),
        "unique_agents": len({r["agent_name"] for r in records}),
        "unique_sources": len({r["source"] for r in records}),
        "avg_score": mean(avg_score_values) if avg_score_values else None,
        "avg_talk_time": mean([r["talk_time"] for r in records]) if records else 0.0,
        "avg_ring_time": mean([r["ring_time"] for r in records]) if records else 0.0,
        "avg_hold_time": mean([r["hold_time"] for r in records]) if records else 0.0,
        "avg_wait_time": mean([r["wait_time_s"] for r in records]) if records else 0.0,
        "top_outcomes": Counter(
            r["call_outcome"] for r in records
            if r["call_outcome"] and r["call_outcome"] != "Unknown"
        ),
        "top_sources": Counter(
            r["source"] for r in records
            if r["source"] and r["source"] != "Unknown"
        ),
        "top_agents": Counter(
            r["agent_name"] for r in records
            if r["agent_name"] and r["agent_name"] != "Unassigned"
        ),
    }


def build_agent_breakdown(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for record in records:
        grouped[record["agent_name"]].append(record)

    rows: List[Dict[str, Any]] = []
    for agent_name, calls in grouped.items():
        inbound_calls = [c for c in calls if c["direction"].lower() == "inbound"]
        answered = sum(
            1 for c in inbound_calls
            if c["dial_status"].lower() == "answered" or c["status"].lower() == "answered"
        )
        scheduled = sum(1 for c in inbound_calls if c["did_schedule"] == "Yes")
        scores = [c["score"] for c in calls if c["score"] is not None]

        source_counter: Counter = Counter(c["source"] for c in calls)
        outcome_counter: Counter = Counter(
            c["call_outcome"] for c in calls if c["call_outcome"] != "Unknown"
        )
        service_counter: Counter = Counter(
            c["service_type"] for c in calls if c["service_type"] != "Unknown"
        )
        objection_counter: Counter = Counter(
            c["objection"] for c in calls
            if c["objection"] not in {"", "None", "Unknown"}
        )
        missed_counter: Counter = Counter()
        for call in calls:
            missed_counter.update(call["missed_questions"])

        rows.append(
            {
                "agent_name": agent_name,
                "calls": len(calls),
                "inbound_calls": len(inbound_calls),
                "answered_calls": answered,
                "answered_rate": answered / len(inbound_calls) * 100 if inbound_calls else 0.0,
                "scheduled_calls": scheduled,
                "scheduled_rate": scheduled / len(inbound_calls) * 100 if inbound_calls else 0.0,
                "avg_score": mean(scores) if scores else None,
                "avg_talk_time": mean([c["talk_time"] for c in calls]) if calls else 0.0,
                "avg_ring_time": mean([c["ring_time"] for c in calls]) if calls else 0.0,
                "avg_hold_time": mean([c["hold_time"] for c in calls]) if calls else 0.0,
                "avg_wait_time": mean([c["wait_time_s"] for c in calls]) if calls else 0.0,
                "top_source": source_counter.most_common(1)[0][0] if source_counter else "—",
                "top_outcome": outcome_counter.most_common(1)[0][0] if outcome_counter else "—",
                "top_service": service_counter.most_common(1)[0][0] if service_counter else "—",
                "source_counter": source_counter,
                "outcome_counter": outcome_counter,
                "service_counter": service_counter,
                "objection_counter": objection_counter,
                "missed_counter": missed_counter,
            }
        )

    rows.sort(key=lambda r: (-r["calls"], r["agent_name"].lower()))
    return rows


def build_source_breakdown(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for record in records:
        grouped[record["source"]].append(record)

    rows: List[Dict[str, Any]] = []
    for source, calls in grouped.items():
        inbound_calls = [c for c in calls if c["direction"].lower() == "inbound"]
        answered = sum(
            1 for c in inbound_calls
            if c["dial_status"].lower() == "answered" or c["status"].lower() == "answered"
        )
        scheduled = sum(1 for c in inbound_calls if c["did_schedule"] == "Yes")
        scores = [c["score"] for c in calls if c["score"] is not None]
        agent_counter: Counter = Counter(c["agent_name"] for c in calls)
        outcome_counter: Counter = Counter(
            c["call_outcome"] for c in calls if c["call_outcome"] != "Unknown"
        )

        rows.append(
            {
                "source": source,
                "calls": len(calls),
                "inbound_calls": len(inbound_calls),
                "answered_calls": answered,
                "answered_rate": answered / len(inbound_calls) * 100 if inbound_calls else 0.0,
                "scheduled_calls": scheduled,
                "scheduled_rate": scheduled / len(inbound_calls) * 100 if inbound_calls else 0.0,
                "avg_score": mean(scores) if scores else None,
                "avg_talk_time": mean([c["talk_time"] for c in calls]) if calls else 0.0,
                "top_agent": agent_counter.most_common(1)[0][0] if agent_counter else "—",
                "top_outcome": outcome_counter.most_common(1)[0][0] if outcome_counter else "—",
                "agent_counter": agent_counter,
            }
        )

    rows.sort(key=lambda r: (-r["calls"], r["source"].lower()))
    return rows


def build_agent_source_matrix(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[tuple, List[Dict[str, Any]]] = defaultdict(list)
    for record in records:
        grouped[(record["agent_name"], record["source"])].append(record)

    rows: List[Dict[str, Any]] = []
    for (agent_name, source), calls in grouped.items():
        inbound_calls = [c for c in calls if c["direction"].lower() == "inbound"]
        scheduled = sum(1 for c in inbound_calls if c["did_schedule"] == "Yes")
        scores = [c["score"] for c in calls if c["score"] is not None]
        rows.append(
            {
                "agent_name": agent_name,
                "source": source,
                "calls": len(calls),
                "inbound_calls": len(inbound_calls),
                "scheduled_calls": scheduled,
                "scheduled_rate": scheduled / len(inbound_calls) * 100 if inbound_calls else 0.0,
                "avg_score": mean(scores) if scores else None,
            }
        )

    rows.sort(key=lambda r: (-r["calls"], r["agent_name"].lower(), r["source"].lower()))
    return rows


def build_dashboard(records: List[Dict[str, Any]], report_label: str) -> Dict[str, Any]:
    sorted_records = sorted(
        records, key=lambda r: (r["unix_time"], r["id"]), reverse=True
    )
    agent_breakdown = build_agent_breakdown(sorted_records)
    source_breakdown = build_source_breakdown(sorted_records)
    return {
        "report_label": report_label,
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "overview": build_overview(sorted_records),
        "agent_breakdown": agent_breakdown,
        "source_breakdown": source_breakdown,
        "agent_source_breakdown": build_agent_source_matrix(sorted_records),
        "call_details": sorted_records,
    }


# ---------------------------------------------------------------------------
# Coaching / notable call helpers
# ---------------------------------------------------------------------------


def build_exec_bullets(
    overview: Dict[str, Any],
    agent_rows: List[Dict[str, Any]],
    source_rows: List[Dict[str, Any]],
) -> List[str]:
    bullets: List[str] = []
    if overview["inbound_calls"] or overview["total_calls"]:
        bullets.append(
            f"{overview['inbound_calls']} inbound leads generated "
            f"{overview['scheduled_calls']} booked calls, "
            f"a {fmt_pct(overview['scheduled_rate'])} booking rate."
        )
    if source_rows:
        top = source_rows[0]
        bullets.append(
            f"{top['source']} led volume with {top['calls']} calls "
            f"and converted at {fmt_pct(top['scheduled_rate'])}."
        )
    if agent_rows:
        best = sorted(
            agent_rows,
            key=lambda r: (
                r["scheduled_calls"], r["scheduled_rate"],
                r["calls"], r["avg_score"] or 0,
            ),
            reverse=True,
        )[0]
        bullets.append(
            f"{best['agent_name']} led the team with {best['scheduled_calls']} "
            f"booked calls on {best['calls']} conversations."
        )
    return bullets[:3]


def build_team_coaching_insights(
    agent_rows: List[Dict[str, Any]],
    overview: Dict[str, Any],
) -> Dict[str, Any]:
    missed_counter: Counter = Counter()
    objection_counter: Counter = Counter()
    outcome_counter: Counter = Counter()
    for row in agent_rows:
        missed_counter.update(row["missed_counter"])
        objection_counter.update(row["objection_counter"])
        outcome_counter.update(row["outcome_counter"])
    return {
        "top_missed": missed_counter,
        "top_objections": objection_counter,
        "top_outcomes": outcome_counter or overview["top_outcomes"],
    }


def build_notable_calls(records: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    summarized = [r for r in records if r["summary"]]
    best_calls = sorted(
        [r for r in summarized if r["did_schedule"] == "Yes"],
        key=lambda r: (r["score"] or 0, r["talk_time"], r["unix_time"]),
        reverse=True,
    )[:3]
    missed_calls = sorted(
        [
            r for r in summarized
            if r["did_schedule"] != "Yes"
            and (r["objection"] not in {"None", "Unknown", ""} or r["missed_questions"])
        ],
        key=lambda r: (r["talk_time"], -(r["score"] or 0), r["unix_time"]),
        reverse=True,
    )[:3]
    return {
        "best_calls": best_calls,
        "missed_calls": missed_calls,
        "recent_calls": summarized[:8],
    }


# ---------------------------------------------------------------------------
# HTML rendering helpers
# ---------------------------------------------------------------------------


def render_email_table(
    headers: Iterable[str],
    rows: Iterable[Iterable[Any]],
    column_widths: Optional[List[str]] = None,
) -> str:
    header_html = ""
    for idx, header in enumerate(headers):
        width_attr = ""
        if column_widths and idx < len(column_widths) and column_widths[idx]:
            width_attr = f' width="{column_widths[idx]}"'
        header_html += (
            f'<th{width_attr} align="left" '
            'style="padding:8px 6px;border-bottom:1px solid #d9d2c7;'
            'font-family:Arial,sans-serif;font-size:12px;line-height:16px;'
            'color:#6b7280;text-transform:uppercase;letter-spacing:0.04em;">'
            f"{esc(header)}</th>"
        )

    row_list = list(rows)
    if not row_list:
        body_html = (
            '<tr><td colspan="99" style="padding:12px 8px;border-bottom:1px solid #e7e0d5;'
            'font-family:Arial,sans-serif;font-size:13px;line-height:18px;color:#6b7280;">'
            "No data</td></tr>"
        )
    else:
        body_html = ""
        for row in row_list:
            body_html += "<tr>" + "".join(
                '<td valign="top" style="padding:8px 6px;border-bottom:1px solid #e7e0d5;'
                'font-family:Arial,sans-serif;font-size:13px;line-height:18px;color:#1f2937;">'
                f"{cell}</td>"
                for cell in row
            ) + "</tr>"

    return (
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" '
        'style="width:100%;border-collapse:collapse;">'
        f"<thead><tr>{header_html}</tr></thead>"
        f"<tbody>{body_html}</tbody>"
        "</table>"
    )


def render_email_kpis(kpis: List[tuple]) -> str:
    cells = ""
    for idx, (label, value) in enumerate(kpis):
        if idx and idx % 3 == 0:
            cells += "</tr><tr>"
        cells += (
            '<td width="33.33%" valign="top" style="padding:4px;">'
            '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" '
            'style="border-collapse:separate;background:#fffdfa;border:1px solid #ddd5c7;border-radius:10px;">'
            '<tr><td style="padding:10px 12px 3px 12px;font-family:Arial,sans-serif;font-size:10px;'
            'line-height:14px;color:#6b7280;text-transform:uppercase;letter-spacing:0.06em;">'
            f"{esc(label)}</td></tr>"
            '<tr><td style="padding:0 12px 10px 12px;font-family:Arial,sans-serif;font-size:20px;'
            'line-height:24px;font-weight:700;color:#1f2937;">'
            f"{esc(value)}</td></tr>"
            "</table></td>"
        )
    if not cells.startswith("<tr>"):
        cells = "<tr>" + cells
    if not cells.endswith("</tr>"):
        cells += "</tr>"
    return (
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" '
        f'style="width:100%;border-collapse:collapse;">{cells}</table>'
    )


def render_email_bullets(items: List[str]) -> str:
    if not items:
        items = ["No notable items."]
    return (
        '<ul style="margin:10px 0 0 18px;padding:0;font-family:Arial,sans-serif;'
        'font-size:14px;line-height:21px;color:#1f2937;">'
        + "".join(f'<li style="margin:0 0 10px 0;">{esc(item)}</li>' for item in items)
        + "</ul>"
    )


def render_email_call_block(
    title: str, subtitle: str, calls: List[Dict[str, Any]]
) -> str:
    content = (
        f'<div style="font-family:Arial,sans-serif;font-size:14px;line-height:20px;'
        f'color:#6b7280;">{esc(subtitle)}</div>'
    )
    if not calls:
        content += (
            '<div style="padding:12px 0 0 0;font-family:Arial,sans-serif;font-size:14px;'
            'line-height:20px;color:#6b7280;">No calls to highlight.</div>'
        )
    else:
        for row in calls:
            content += (
                '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" '
                'style="width:100%;border-collapse:separate;background:#fffdfa;border:1px solid #ddd5c7;'
                'border-radius:12px;margin-top:10px;">'
                '<tr><td style="padding:12px 12px 5px 12px;font-family:Arial,sans-serif;font-size:11px;'
                'line-height:15px;color:#6b7280;text-transform:uppercase;letter-spacing:0.05em;">'
                f"{esc(row['agent_name'])} · {esc(row['source'])}</td></tr>"
                '<tr><td style="padding:0 12px 6px 12px;font-family:Arial,sans-serif;font-size:16px;'
                'line-height:22px;font-weight:700;color:#1f2937;">'
                f"{esc(row['service_type'])} · {esc(row['call_outcome'])}</td></tr>"
                '<tr><td style="padding:0 12px 8px 12px;font-family:Arial,sans-serif;font-size:14px;'
                'line-height:20px;color:#1f2937;">'
                f"{esc(row['summary'] or row['explanation_of_outcome'] or '—')}</td></tr>"
                '<tr><td style="padding:0 12px 12px 12px;font-family:Arial,sans-serif;font-size:12px;'
                'line-height:18px;color:#6b7280;">'
                f"Scheduled: <b>{esc(row['did_schedule'])}</b> &nbsp;|&nbsp; "
                f"Score: <b>{esc(fmt_pct(row['score']))}</b> &nbsp;|&nbsp; "
                f"Talk: <b>{esc(fmt_sec(row['talk_time']))}</b></td></tr>"
                "</table>"
            )

    return (
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" '
        'style="width:100%;border-collapse:separate;background:#fdfaf4;border:1px solid #ddd5c7;'
        'border-radius:12px;">'
        '<tr><td style="padding:12px;">'
        f'<div style="font-family:Arial,sans-serif;font-size:18px;line-height:22px;'
        f'font-weight:700;color:#1f2937;">{esc(title)}</div>'
        f'<div style="padding-top:4px;">{content}</div>'
        "</td></tr></table>"
    )


def render_brand_block(logo_url: str) -> str:
    logomark_fallback = (
        '<table role="presentation" width="44" height="44" cellpadding="0" cellspacing="0" border="0" '
        'style="width:44px;height:44px;border-collapse:collapse;background:#1f5d8b;">'
        '<tr>'
        '<td width="22" height="22" style="background:#36b6f4;border-right:2px solid #f2f6fb;border-bottom:2px solid #f2f6fb;"></td>'
        '<td width="22" height="22" style="background:#1f5d8b;border-bottom:2px solid #f2f6fb;"></td>'
        '</tr>'
        '<tr>'
        '<td width="22" height="22" style="background:#f2f6fb;border-right:2px solid #f2f6fb;"></td>'
        '<td width="22" height="22" style="background:#1e2a55;"></td>'
        '</tr>'
        '</table>'
    )
    logo_img = (
        f'<img src="{esc(logo_url)}" alt="CTM" width="44" '
        'style="display:block;border:0;outline:none;text-decoration:none;width:44px;height:auto;">'
        if safe_str(logo_url) else logomark_fallback
    )
    wordmark = (
        '<div style="font-size:11px;line-height:14px;font-weight:700;color:#bae6fd;'
        'text-transform:uppercase;letter-spacing:0.08em;">Call Tracking Metrics</div>'
        '<div style="font-size:18px;line-height:22px;font-weight:700;color:#ffffff;">'
        'Daily Executive Summary</div>'
    )
    return (
        '<table role="presentation" cellpadding="0" cellspacing="0" border="0" '
        'style="border-collapse:collapse;"><tr>'
        f'<td valign="middle" style="padding-right:10px;">{logo_img}</td>'
        f'<td valign="middle" style="font-family:Arial,sans-serif;">{wordmark}</td>'
        '</tr></table>'
    )


# ---------------------------------------------------------------------------
# Full HTML report
# ---------------------------------------------------------------------------


def generate_html_report(
    dashboard: Dict[str, Any],
    max_detail_rows: int = DEFAULT_MAX_DETAIL_ROWS,
    logo_url: str = "",
) -> str:
    overview = dashboard["overview"]
    agent_rows = dashboard["agent_breakdown"]
    source_rows = dashboard["source_breakdown"]
    team_insights = build_team_coaching_insights(agent_rows, overview)
    exec_bullets = build_exec_bullets(overview, agent_rows, source_rows)
    notable_calls = build_notable_calls(dashboard["call_details"])

    sorted_agent_rows = sorted(
        agent_rows,
        key=lambda r: (r["scheduled_calls"], r["scheduled_rate"], r["calls"], r["avg_score"] or 0),
        reverse=True,
    )
    top_agent = sorted_agent_rows[0]["agent_name"] if sorted_agent_rows else "—"
    top_source = source_rows[0]["source"] if source_rows else "—"

    detail_rows = [
        r for r in dashboard["call_details"] if safe_str(r.get("summary"))
    ][:max_detail_rows]

    trend_rows = [
        r for r in agent_rows
        if r["service_counter"] or r["outcome_counter"]
        or r["missed_counter"] or r["objection_counter"]
    ]

    kpi_html = render_email_kpis(
        [
            ("Inbound Leads", overview["inbound_calls"]),
            ("Answered Rate", fmt_pct(overview["answered_rate"])),
            ("Booked Rate", fmt_pct(overview["scheduled_rate"])),
            ("Avg AI Score", fmt_pct(overview["avg_score"])),
            ("Top Agent", top_agent),
            ("Top Source", top_source),
        ]
    )

    agent_table = render_email_table(
        ["Agent", "Handled", "Booked", "Booked Rate", "Answered Rate",
         "Avg Score", "Primary Source", "Coaching Focus"],
        [
            [
                esc(r["agent_name"]), esc(r["calls"]), esc(r["scheduled_calls"]),
                esc(fmt_pct(r["scheduled_rate"])), esc(fmt_pct(r["answered_rate"])),
                esc(fmt_pct(r["avg_score"])), esc(r["top_source"]),
                esc(top_items(r["missed_counter"], 2) if r["missed_counter"]
                    else top_items(r["objection_counter"], 2)),
            ]
            for r in sorted_agent_rows
        ],
        ["19%", "9%", "9%", "11%", "11%", "10%", "15%", "16%"],
    )

    source_table = render_email_table(
        ["Source", "Lead Volume", "Booked", "Booked Rate", "Answered Rate",
         "Avg Score", "Top Agent", "Top Outcome"],
        [
            [
                esc(r["source"]), esc(r["calls"]), esc(r["scheduled_calls"]),
                esc(fmt_pct(r["scheduled_rate"])), esc(fmt_pct(r["answered_rate"])),
                esc(fmt_pct(r["avg_score"])), esc(r["top_agent"]), esc(r["top_outcome"]),
            ]
            for r in source_rows
        ],
        ["22%", "10%", "9%", "11%", "11%", "10%", "14%", "13%"],
    )

    coaching_table = render_email_table(
        ["Teamwide Focus", "Most Common"],
        [
            ["Missed questions", esc(top_items(team_insights["top_missed"], 6))],
            ["Objections",       esc(top_items(team_insights["top_objections"], 6))],
            ["Outcomes",         esc(top_items(team_insights["top_outcomes"], 6))],
        ],
        ["28%", "72%"],
    )

    trends_table = render_email_table(
        ["Agent", "Service Mix", "Outcomes", "Missed Questions", "Objections"],
        [
            [
                esc(r["agent_name"]),
                esc(top_items(r["service_counter"], 4)),
                esc(top_items(r["outcome_counter"], 4)),
                esc(top_items(r["missed_counter"], 4)),
                esc(top_items(r["objection_counter"], 4)),
            ]
            for r in trend_rows
        ],
        ["18%", "20%", "20%", "24%", "18%"],
    )

    detail_table = render_email_table(
        ["Called At", "Agent", "Source", "Status", "Service",
         "Outcome", "Scheduled", "Score", "Summary"],
        [
            [
                esc(r["called_at"]), esc(r["agent_name"]), esc(r["source"]),
                esc(r["dial_status"] or r["status"]), esc(r["service_type"]),
                esc(r["call_outcome"]), esc(r["did_schedule"]),
                esc(fmt_pct(r["score"])),
                esc(r["summary"] or r["explanation_of_outcome"] or "—"),
            ]
            for r in detail_rows
        ],
        ["14%", "10%", "12%", "8%", "9%", "10%", "8%", "7%", "22%"],
    )

    no_trends_html = (
        '<div style="font-family:Arial,sans-serif;font-size:14px;line-height:20px;'
        'color:#6b7280;">No agent trends available.</div>'
    )

    def section(title: str, body: str) -> str:
        return (
            '<tr><td style="padding:2px 6px;">'
            '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" '
            'style="background:#fdfaf4;border:1px solid #ddd5c7;border-collapse:separate;">'
            f'<tr><td style="padding:10px 10px 4px 10px;font-family:Arial,sans-serif;'
            f'font-size:18px;line-height:22px;font-weight:700;color:#1f2937;">{esc(title)}</td></tr>'
            f'<tr><td style="padding:0 10px 8px 10px;">{body}</td></tr>'
            "</table></td></tr>"
        )

    brand_block = render_brand_block(logo_url)
    exec_snapshot = render_email_bullets(exec_bullets)
    notable_html = (
        render_email_call_block("Booked Wins", "", notable_calls["best_calls"])
        + '<div style="height:6px;line-height:6px;font-size:6px;">&nbsp;</div>'
        + render_email_call_block("Missed Opportunities", "", notable_calls["missed_calls"])
    )

    return f"""<!DOCTYPE html>
<html lang="en" xmlns="http://www.w3.org/1999/xhtml">
<body style="margin:0;padding:0;background-color:#ffffff;">
<div style="display:none;max-height:0;overflow:hidden;opacity:0;color:transparent;">
Daily CTM summary: bookings, agent scorecard, source scorecard, coaching insights, and notable calls.
</div>
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"
  style="width:100%;background-color:#ffffff;margin:0;padding:0;">
<tr><td align="left" style="padding:0;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"
  style="width:100%;border-collapse:collapse;background-color:#fcf8f1;">

<!-- Header -->
<tr><td style="padding:0;background-color:#1f5d8b;border-bottom:4px solid #36b6f4;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"
  style="width:100%;border-collapse:collapse;">
<tr>
  <td valign="middle" align="left" style="padding:8px 10px;">
    {brand_block}
    <div style="padding-top:4px;font-family:Arial,sans-serif;font-size:12px;line-height:16px;color:#d9edf7;">
      {esc(dashboard['report_label'])}
    </div>
  </td>
  <td valign="middle" align="right" style="padding:8px 10px 8px 0;font-family:Arial,sans-serif;white-space:nowrap;">
    <span style="display:inline-block;padding:6px 8px;background-color:#eff8f7;border:1px solid #b9d7d1;
      font-size:11px;line-height:14px;font-weight:700;color:#0f766e;">
      Generated {esc(dashboard['generated_at'])}
    </span>
  </td>
</tr>
</table>
</td></tr>

<!-- KPIs -->
<tr><td style="padding:2px 6px 0 6px;">{kpi_html}</td></tr>

<!-- Executive Snapshot -->
<tr><td style="padding:2px 6px;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"
  style="background:#fdfaf4;border:1px solid #ddd5c7;border-collapse:separate;">
<tr><td style="padding:10px 10px 2px 10px;font-family:Arial,sans-serif;font-size:18px;
  line-height:22px;font-weight:700;color:#1f2937;">Executive Snapshot</td></tr>
<tr><td style="padding:0 10px 2px 10px;">{exec_snapshot}</td></tr>
<tr><td style="padding:0 10px 10px 10px;font-family:Arial,sans-serif;font-size:13px;
  line-height:19px;color:#4b5563;">
  Summarized: <b>{esc(overview['summarized_calls'])}</b> &nbsp;|&nbsp;
  New callers: <b>{esc(overview['new_callers'])}</b> &nbsp;|&nbsp;
  Inbound / Outbound: <b>{esc(overview['inbound_calls'])} / {esc(overview['outbound_calls'])}</b> &nbsp;|&nbsp;
  Avg ring / hold / wait: <b>{esc(fmt_sec(overview['avg_ring_time']))} /
    {esc(fmt_sec(overview['avg_hold_time']))} / {esc(fmt_sec(overview['avg_wait_time']))}</b>
</td></tr>
</table>
</td></tr>

{section("Agent Scorecard", agent_table)}
{section("Source Scorecard", source_table)}
{section("Team Coaching Insights", coaching_table)}
{section("AI Trends By Agent", trends_table if trend_rows else no_trends_html)}
{section("Notable Summarized Calls", notable_html)}
{section("Recent Summarized Call Detail", detail_table)}

</table>
</td></tr>
</table>
</body>
</html>"""


# ---------------------------------------------------------------------------
# CSV / JSON exports
# ---------------------------------------------------------------------------


def export_csv(
    rows: List[Dict[str, Any]],
    path: Path,
    fields: List[str],
) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    log.info("Wrote %s", path)


def export_all(dashboard: Dict[str, Any], date_str: str) -> None:
    call_fields = [
        "id", "called_at", "agent_name", "source", "direction", "status",
        "dial_status", "service_type", "call_outcome", "did_schedule",
        "score", "talk_time", "ring_time", "hold_time", "wait_time_s",
        "is_new_caller", "objection", "missed_questions_text",
        "summary", "explanation_of_outcome", "city", "state",
    ]
    agent_fields = [
        "agent_name", "calls", "inbound_calls", "answered_calls", "answered_rate",
        "scheduled_calls", "scheduled_rate", "avg_score", "avg_talk_time",
        "avg_ring_time", "avg_hold_time", "avg_wait_time",
        "top_source", "top_outcome", "top_service",
    ]
    source_fields = [
        "source", "calls", "inbound_calls", "answered_calls", "answered_rate",
        "scheduled_calls", "scheduled_rate", "avg_score", "avg_talk_time",
        "top_agent", "top_outcome",
    ]
    matrix_fields = [
        "agent_name", "source", "calls", "inbound_calls",
        "scheduled_calls", "scheduled_rate", "avg_score",
    ]

    export_csv(dashboard["call_details"],        Path(f"ctm_calls_{date_str}.csv"),         call_fields)
    export_csv(dashboard["agent_breakdown"],      Path(f"ctm_agents_{date_str}.csv"),        agent_fields)
    export_csv(dashboard["source_breakdown"],     Path(f"ctm_sources_{date_str}.csv"),       source_fields)
    export_csv(dashboard["agent_source_breakdown"], Path(f"ctm_agent_sources_{date_str}.csv"), matrix_fields)

    export_json(dashboard, date_str)


def export_json(dashboard: Dict[str, Any], date_str: str) -> None:
    json_path = Path(f"ctm_daily_summary_{date_str}.json")
    # Counters aren't JSON-serialisable — convert before writing
    serialisable = json.loads(json.dumps(dashboard, default=str))
    json_path.write_text(json.dumps(serialisable, indent=2), encoding="utf-8")
    log.info("Wrote %s", json_path)


def export_csvs(dashboard: Dict[str, Any], date_str: str) -> None:
    call_fields = [
        "id", "called_at", "agent_name", "source", "direction", "status",
        "dial_status", "service_type", "call_outcome", "did_schedule",
        "score", "talk_time", "ring_time", "hold_time", "wait_time_s",
        "is_new_caller", "objection", "missed_questions_text",
        "summary", "explanation_of_outcome", "city", "state",
    ]
    agent_fields = [
        "agent_name", "calls", "inbound_calls", "answered_calls", "answered_rate",
        "scheduled_calls", "scheduled_rate", "avg_score", "avg_talk_time",
        "avg_ring_time", "avg_hold_time", "avg_wait_time",
        "top_source", "top_outcome", "top_service",
    ]
    source_fields = [
        "source", "calls", "inbound_calls", "answered_calls", "answered_rate",
        "scheduled_calls", "scheduled_rate", "avg_score", "avg_talk_time",
        "top_agent", "top_outcome",
    ]
    matrix_fields = [
        "agent_name", "source", "calls", "inbound_calls",
        "scheduled_calls", "scheduled_rate", "avg_score",
    ]

    export_csv(dashboard["call_details"],        Path(f"ctm_calls_{date_str}.csv"),         call_fields)
    export_csv(dashboard["agent_breakdown"],      Path(f"ctm_agents_{date_str}.csv"),        agent_fields)
    export_csv(dashboard["source_breakdown"],     Path(f"ctm_sources_{date_str}.csv"),       source_fields)
    export_csv(dashboard["agent_source_breakdown"], Path(f"ctm_agent_sources_{date_str}.csv"), matrix_fields)


# ---------------------------------------------------------------------------
# Webhook delivery
# ---------------------------------------------------------------------------


def post_to_webhook(html_report: str, webhook_url: str, timeout: int = 30) -> None:
    if not webhook_url:
        log.warning("No webhook URL configured — skipping POST.")
        return
    log.info("Posting report to webhook...")
    resp = requests.post(
        webhook_url,
        json={"html": html_report},
        timeout=timeout,
    )
    resp.raise_for_status()
    log.info("Webhook response: %s", resp.status_code)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Generate and deliver a CTM daily executive summary report."
    )
    p.add_argument(
        "--time-duration", default=DEFAULT_TIME_DURATION,
        help="CTM time duration filter (default: %(default)s)",
    )
    p.add_argument(
        "--no-hydrate-details", action="store_true",
        help="Skip per-call detail API calls (faster, less data)",
    )
    p.add_argument(
        "--no-webhook", action="store_true",
        help="Write output files only; skip posting to Make.com",
    )
    p.add_argument(
        "--max-detail-rows", type=int, default=DEFAULT_MAX_DETAIL_ROWS, metavar="N",
        help="Max rows in the call detail table (default: %(default)s)",
    )
    p.add_argument(
        "--sleep-s", type=float, default=DEFAULT_SLEEP_S, metavar="F",
        help="Seconds to sleep between detail API calls (default: %(default)s)",
    )
    p.add_argument(
        "--account-id", default=None,
        help="CTM account ID (overrides CTM_ACCOUNT_ID env var)",
    )
    p.add_argument(
        "--webhook-url", default=None,
        help="Make.com webhook URL (overrides CTM_WEBHOOK_URL env var)",
    )
    p.add_argument(
        "--logo-url", default=None,
        help="Public URL of logo image (overrides CTM_LOGO_URL env var)",
    )
    p.add_argument(
        "--converted-field", default=None,
        help="Custom field key that indicates a conversion (overrides CTM_ONVERTED_FIELD)",
    )
    p.add_argument(
        "--score-field", default=None,
        help="Custom field key for AI score (overrides CTM_SCORE_FIELD)",
    )
    p.add_argument(
        "--export-csv", action="store_true",
        help="Write CSV exports (default: off)",
    )
    p.add_argument(
        "--export-json", action="store_true",
        help="Write JSON export (default: off)",
    )
    p.add_argument(
        "--export-all", action="store_true",
        help="Write CSV and JSON exports (default: off)",
    )
    return p


def main() -> None:
    args = _build_arg_parser().parse_args()
    config = _resolve_config()

    account_id = args.account_id or config["CTM_ACCOUNT_ID"]
    webhook_url = args.webhook_url or config["CTM_WEBHOOK_URL"]
    logo_url = args.logo_url or config["CTM_LOGO_URL"]
    converted_field = args.converted_field or config["CTM_ONVERTED_FIELD"] or "did_the_caller_schedule"
    score_field = args.score_field or config["CTM_SCORE_FIELD"] or "cumulative_score_percentage"

    if not account_id:
        raise SystemExit(
            "CTM_ACCOUNT_ID not set. Set the env var, add it to ENV.txt, or pass --account-id."
        )

    auth_header = _resolve_auth_header()
    client = CTMClient(
        account_id=account_id,
        base_url=DEFAULT_BASE_URL,
        auth_header=auth_header,
    )

    log.info("Fetching calls for time_duration=%s", args.time_duration)
    raw_calls = client.fetch_calls(time_duration=args.time_duration)
    log.info("Fetched %d calls total", len(raw_calls))

    calls = hydrate_calls(
        client=client,
        calls=raw_calls,
        hydrate_details=not args.no_hydrate_details,
        sleep_s=args.sleep_s,
    )

    records = [extract_call_record(c, converted_field=converted_field, score_field=score_field) for c in calls]
    log.info("Normalized %d call records", len(records))

    today = dt.date.today()
    report_date = today - dt.timedelta(days=1) if args.time_duration == "yesterday" else today
    date_str = report_date.strftime("%Y-%m-%d")
    report_label = f"Report for {date_str}"

    dashboard = build_dashboard(records, report_label)
    html_report = generate_html_report(
        dashboard,
        max_detail_rows=args.max_detail_rows,
        logo_url=logo_url,
    )

    html_path = Path(f"ctm_daily_summary_{date_str}.html")
    html_path.write_text(html_report, encoding="utf-8")
    log.info("Wrote %s", html_path)

    if args.export_all or args.export_csv:
        export_csvs(dashboard, date_str)
    if args.export_all or args.export_json:
        export_json(dashboard, date_str)

    if not args.no_webhook:
        post_to_webhook(html_report, webhook_url)
    else:
        log.info("--no-webhook set; skipping email delivery.")

    log.info("Done.")


if __name__ == "__main__":
    main()
