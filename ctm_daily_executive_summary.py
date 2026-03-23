#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import html
import json
import os
import re
import time
from collections import Counter, defaultdict
from pathlib import Path
from statistics import mean
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urlparse

import requests


ACCOUNT_ID = os.getenv("CTM_ACCOUNT_ID", "")
DEFAULT_WEBHOOK_URL = os.getenv("CTM_WEBHOOK_URL", "")
DEFAULT_BASE_URL = "https://api.calltrackingmetrics.com/api/v1"
DEFAULT_TIME_DURATION = "yesterday"
DEFAULT_TIMEZONE = "America/Chicago"
DEFAULT_LOGO_URL = os.getenv("CTM_LOGO_URL", "")
HARDCODED_CTM_API_KEY = ""


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
    except Exception:
        return None


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def normalize_wait_seconds(wait_time: Any) -> float:
    value = safe_float(wait_time)
    if value is None:
        return 0.0
    return value / 1000.0 if value > 1000 else value


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
    if minutes == 0:
        return f"{remainder}s"
    return f"{minutes}m {remainder:02d}s"


def esc(value: Any) -> str:
    if value is None:
        return ""
    return html.escape(str(value))


def split_multi_value(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        parts = value
    else:
        parts = re.split(r"[;\n|]+", str(value))
    out: List[str] = []
    for part in parts:
        item = safe_str(part)
        if item and item.lower() not in {"none", "null", "n/a", "na"}:
            out.append(item)
    return out


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


class CTMClient:
    def __init__(self, account_id: str, base_url: str, auth_header: str, timeout: int = 60):
        self.account_id = account_id
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": auth_header,
                "Accept": "application/json",
                "User-Agent": "just-right-lawns-daily-report",
            }
        )

    def get(self, path_or_url: str, params: Optional[Dict[str, Any]] = None) -> Any:
        path = path_from_url(path_or_url)
        if path.startswith("/api/v1"):
            url = f"{self.base_url}{path[len('/api/v1'):]}"
        elif path.startswith("/"):
            url = f"{self.base_url}{path}"
        else:
            url = f"{self.base_url}/{path}"

        response = self.session.get(url, params=params or {}, timeout=self.timeout)
        if response.status_code == 401:
            raise RuntimeError(
                "CTM authentication failed (401). "
                "Check HARDCODED_CTM_API_KEY, CTM_AUTH, CTM_BASIC_AUTH, or CTM_API_KEY."
            )
        response.raise_for_status()
        return response.json()

    def fetch_calls(self, time_duration: str = DEFAULT_TIME_DURATION, per_page: int = 100) -> List[Dict[str, Any]]:
        calls: List[Dict[str, Any]] = []
        next_path: str = f"/accounts/{self.account_id}/calls"
        params: Optional[Dict[str, Any]] = {"per_page": per_page, "time_duration": time_duration}

        while next_path:
            payload = self.get(next_path, params=params)
            batch = payload.get("calls", []) if isinstance(payload, dict) else []
            calls.extend([call for call in batch if isinstance(call, dict)])

            next_page = payload.get("next_page") if isinstance(payload, dict) else None
            next_path = path_from_url(next_page) if next_page else ""
            params = None

        return calls

    def fetch_call_detail(self, call_id: Any) -> Dict[str, Any]:
        return self.get(f"/accounts/{self.account_id}/calls/{call_id}")


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

        detail = client.fetch_call_detail(call_id)
        hydrated.append(merge_call(call, detail))
        if sleep_s:
            time.sleep(sleep_s)
        if index % 25 == 0:
            print(f"Hydrated {index}/{len(calls)} calls...")

    return hydrated


def normalize_yes_no(value: Any) -> str:
    text = safe_str(value).lower()
    if text in {"yes", "true", "scheduled", "booked"}:
        return "Yes"
    if text in {"no", "false", "not scheduled"}:
        return "No"
    return safe_str(value, "Unknown")


def extract_call_record(call: Dict[str, Any]) -> Dict[str, Any]:
    cf = safe_dict(call.get("custom_fields"))
    agent = safe_dict(call.get("agent"))

    missed_questions = split_multi_value(cf.get("missed_questions"))
    score = safe_float(cf.get("cumulative_score_percentage"))
    rating = safe_float(cf.get("agent_star_rating"))

    return {
        "id": call.get("id"),
        "sid": safe_str(call.get("sid")),
        "called_at": safe_str(call.get("called_at")),
        "unix_time": safe_int(call.get("unix_time")),
        "source": safe_str(call.get("source"), "Unknown"),
        "direction": safe_str(call.get("direction"), "Unknown"),
        "status": safe_str(call.get("status") or call.get("call_status"), "Unknown"),
        "dial_status": safe_str(call.get("dial_status"), "Unknown"),
        "agent_name": safe_str(agent.get("name") or agent.get("email") or agent.get("id"), "Unassigned"),
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
        "did_schedule": normalize_yes_no(cf.get("did_the_caller_schedule")),
        "objection": safe_str(cf.get("objections_reasons_not_scheduled"), "None"),
        "missed_questions": missed_questions,
        "missed_questions_text": "; ".join(missed_questions) if missed_questions else "None",
        "score": score,
        "rating": rating,
        "explanation_of_outcome": safe_str(cf.get("explanation_of_outcome")),
        "call_type": safe_str(cf.get("call_type")),
        "caller_name": safe_str(call.get("name") or safe_dict(call.get("caller")).get("name")),
        "city": safe_str(call.get("city")),
        "state": safe_str(call.get("state")),
        "tracking_label": safe_str(call.get("tracking_label")),
    }


def build_overview(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    total = len(records)
    inbound_records = [r for r in records if r["direction"].lower() == "inbound"]
    answered = sum(
        1
        for r in inbound_records
        if r["dial_status"].lower() == "answered" or r["status"].lower() == "answered"
    )
    scheduled = sum(1 for r in inbound_records if r["did_schedule"] == "Yes")
    inbound = sum(1 for r in records if r["direction"].lower() == "inbound")
    outbound = sum(1 for r in records if r["direction"].lower() == "outbound")
    summarized = sum(1 for r in records if r["summary"])
    new_callers = sum(1 for r in records if r["is_new_caller"])
    avg_score_values = [r["score"] for r in records if r["score"] is not None]
    source_counter = Counter(r["source"] for r in records if r["source"] and r["source"] != "Unknown")
    agent_counter = Counter(r["agent_name"] for r in records if r["agent_name"] and r["agent_name"] != "Unassigned")

    return {
        "total_calls": total,
        "answered_calls": answered,
        "answered_rate": (answered / len(inbound_records) * 100) if inbound_records else 0.0,
        "scheduled_calls": scheduled,
        "scheduled_rate": (scheduled / len(inbound_records) * 100) if inbound_records else 0.0,
        "inbound_calls": inbound,
        "outbound_calls": outbound,
        "summarized_calls": summarized,
        "new_callers": new_callers,
        "unique_agents": len({r["agent_name"] for r in records}),
        "unique_sources": len({r["source"] for r in records}),
        "avg_score": mean(avg_score_values) if avg_score_values else None,
        "avg_talk_time": mean([r["talk_time"] for r in records]) if records else 0.0,
        "avg_ring_time": mean([r["ring_time"] for r in records]) if records else 0.0,
        "avg_hold_time": mean([r["hold_time"] for r in records]) if records else 0.0,
        "avg_wait_time": mean([r["wait_time_s"] for r in records]) if records else 0.0,
        "top_outcomes": Counter(r["call_outcome"] for r in records if r["call_outcome"] and r["call_outcome"] != "Unknown"),
        "top_sources": source_counter,
        "top_agents": agent_counter,
    }


def build_agent_breakdown(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for record in records:
        grouped[record["agent_name"]].append(record)

    rows: List[Dict[str, Any]] = []
    for agent_name, calls in sorted(grouped.items(), key=lambda item: item[0].lower()):
        total = len(calls)
        inbound_calls = [c for c in calls if c["direction"].lower() == "inbound"]
        answered = sum(
            1
            for c in inbound_calls
            if c["dial_status"].lower() == "answered" or c["status"].lower() == "answered"
        )
        scheduled = sum(1 for c in inbound_calls if c["did_schedule"] == "Yes")
        scores = [c["score"] for c in calls if c["score"] is not None]
        source_counter = Counter(c["source"] for c in calls)
        outcome_counter = Counter(c["call_outcome"] for c in calls if c["call_outcome"] != "Unknown")
        service_counter = Counter(c["service_type"] for c in calls if c["service_type"] != "Unknown")
        objection_counter = Counter(c["objection"] for c in calls if c["objection"] not in {"", "None", "Unknown"})
        missed_counter: Counter = Counter()
        for call in calls:
            missed_counter.update(call["missed_questions"])

        rows.append(
            {
                "agent_name": agent_name,
                "calls": total,
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

    rows.sort(key=lambda row: (-row["calls"], row["agent_name"].lower()))
    return rows


def build_source_breakdown(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for record in records:
        grouped[record["source"]].append(record)

    rows: List[Dict[str, Any]] = []
    for source, calls in sorted(grouped.items(), key=lambda item: item[0].lower()):
        total = len(calls)
        inbound_calls = [c for c in calls if c["direction"].lower() == "inbound"]
        answered = sum(
            1
            for c in inbound_calls
            if c["dial_status"].lower() == "answered" or c["status"].lower() == "answered"
        )
        scheduled = sum(1 for c in inbound_calls if c["did_schedule"] == "Yes")
        scores = [c["score"] for c in calls if c["score"] is not None]
        agent_counter = Counter(c["agent_name"] for c in calls)
        outcome_counter = Counter(c["call_outcome"] for c in calls if c["call_outcome"] != "Unknown")

        rows.append(
            {
                "source": source,
                "calls": total,
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

    rows.sort(key=lambda row: (-row["calls"], row["source"].lower()))
    return rows


def build_agent_source_matrix(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    for record in records:
        grouped[(record["agent_name"], record["source"])].append(record)

    rows: List[Dict[str, Any]] = []
    for (agent_name, source), calls in grouped.items():
        total = len(calls)
        inbound_calls = [c for c in calls if c["direction"].lower() == "inbound"]
        scheduled = sum(1 for c in inbound_calls if c["did_schedule"] == "Yes")
        scores = [c["score"] for c in calls if c["score"] is not None]
        rows.append(
            {
                "agent_name": agent_name,
                "source": source,
                "calls": total,
                "inbound_calls": len(inbound_calls),
                "scheduled_calls": scheduled,
                "scheduled_rate": scheduled / len(inbound_calls) * 100 if inbound_calls else 0.0,
                "avg_score": mean(scores) if scores else None,
            }
        )

    rows.sort(key=lambda row: (-row["calls"], row["agent_name"].lower(), row["source"].lower()))
    return rows


def build_dashboard(records: List[Dict[str, Any]], report_label: str) -> Dict[str, Any]:
    sorted_records = sorted(records, key=lambda item: (item["unix_time"], item["id"]), reverse=True)
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


def render_overview_cards(overview: Dict[str, Any], top_agent: str, top_source: str) -> str:
    cards = [
        ("Inbound Leads", overview["inbound_calls"]),
        ("Answered Rate", fmt_pct(overview["answered_rate"])),
        ("Booked Rate", fmt_pct(overview["scheduled_rate"])),
        ("Avg AI Score", fmt_pct(overview["avg_score"])),
        ("Top Agent", top_agent),
        ("Top Source", top_source),
    ]
    return "".join(
        "<div class='card'>"
        f"<div class='label'>{esc(label)}</div>"
        f"<div class='value'>{esc(value)}</div>"
        "</div>"
        for label, value in cards
    )


def render_table(headers: Iterable[str], rows: Iterable[Iterable[Any]]) -> str:
    head_html = "".join(f"<th>{esc(h)}</th>" for h in headers)
    body_parts: List[str] = []
    for row in rows:
        cells = "".join(f"<td>{cell}</td>" for cell in row)
        body_parts.append(f"<tr>{cells}</tr>")
    body_html = "".join(body_parts)
    if not body_html:
        body_html = '<tr><td colspan="99">No data</td></tr>'
    return (
        "<table>"
        f"<thead><tr>{head_html}</tr></thead>"
        f"<tbody>{body_html}</tbody>"
        "</table>"
    )


def render_bullets(items: List[str]) -> str:
    if not items:
        return "<ul><li>No notable items.</li></ul>"
    return "<ul>" + "".join(f"<li>{esc(item)}</li>" for item in items) + "</ul>"


def render_call_cards(title: str, subtitle: str, calls: List[Dict[str, Any]]) -> str:
    if not calls:
        body = "<div class='mini-empty'>No calls to highlight.</div>"
    else:
        cards = []
        for row in calls:
            cards.append(
                "<div class='call-card'>"
                f"<div class='call-kicker'>{esc(row['agent_name'])} · {esc(row['source'])}</div>"
                f"<h3>{esc(row['service_type'])} · {esc(row['call_outcome'])}</h3>"
                f"<p>{esc(row['summary'] or row['explanation_of_outcome'] or '—')}</p>"
                f"<div class='call-meta'>Scheduled: <b>{esc(row['did_schedule'])}</b> · "
                f"Score: <b>{esc(fmt_pct(row['score']))}</b> · Talk: <b>{esc(fmt_sec(row['talk_time']))}</b></div>"
                "</div>"
            )
        body = "".join(cards)

    return (
        "<div class='call-card-panel'>"
        f"<h3>{esc(title)}</h3>"
        f"<p class='subtle'>{esc(subtitle)}</p>"
        f"{body}"
        "</div>"
    )


def render_email_table(headers: Iterable[str], rows: Iterable[Iterable[Any]], column_widths: Optional[List[str]] = None) -> str:
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

    body_html = ""
    row_list = list(rows)
    if not row_list:
        body_html = (
            '<tr><td colspan="99" style="padding:12px 8px;border-bottom:1px solid #e7e0d5;'
            'font-family:Arial,sans-serif;font-size:13px;line-height:18px;color:#6b7280;">No data</td></tr>'
        )
    else:
        for row in row_list:
            body_html += "<tr>"
            for cell in row:
                body_html += (
                    '<td valign="top" style="padding:8px 6px;border-bottom:1px solid #e7e0d5;'
                    'font-family:Arial,sans-serif;font-size:13px;line-height:18px;color:#1f2937;">'
                    f"{cell}</td>"
                )
            body_html += "</tr>"

    return (
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" '
        'style="width:100%;border-collapse:collapse;">'
        f"<thead><tr>{header_html}</tr></thead>"
        f"<tbody>{body_html}</tbody>"
        "</table>"
    )


def render_email_kpis(kpis: List[tuple[str, Any]]) -> str:
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
            '</table></td>'
        )

    if not cells.startswith("<tr>"):
        cells = "<tr>" + cells
    if not cells.endswith("</tr>"):
        cells += "</tr>"

    return (
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" '
        'style="width:100%;border-collapse:collapse;">'
        f"{cells}</table>"
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


def render_email_call_block(title: str, subtitle: str, calls: List[Dict[str, Any]]) -> str:
    content = (
        f'<div style="font-family:Arial,sans-serif;font-size:14px;line-height:20px;color:#6b7280;">{esc(subtitle)}</div>'
    )
    if not calls:
        content += (
            '<div style="padding:12px 0 0 0;font-family:Arial,sans-serif;font-size:14px;line-height:20px;'
            'color:#6b7280;">No calls to highlight.</div>'
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
                f"Scheduled: <b>{esc(row['did_schedule'])}</b> &nbsp; | &nbsp; "
                f"Score: <b>{esc(fmt_pct(row['score']))}</b> &nbsp; | &nbsp; "
                f"Talk: <b>{esc(fmt_sec(row['talk_time']))}</b></td></tr>"
                "</table>"
            )

    return (
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" '
        'style="width:100%;border-collapse:separate;background:#fdfaf4;border:1px solid #ddd5c7;'
        'border-radius:12px;">'
        '<tr><td style="padding:12px;">'
        f'<div style="font-family:Arial,sans-serif;font-size:18px;line-height:22px;font-weight:700;color:#1f2937;">{esc(title)}</div>'
        f'<div style="padding-top:4px;">{content}</div>'
        "</td></tr></table>"
    )


def render_brand_block(logo_url: str) -> str:
    if safe_str(logo_url):
        return (
            '<table role="presentation" cellpadding="0" cellspacing="0" border="0" style="border-collapse:collapse;">'
            '<tr>'
            f'<td valign="middle" style="padding-right:10px;"><img src="{esc(logo_url)}" alt="CTM Daily Executive Summary" width="44" '
            'style="display:block;border:0;outline:none;text-decoration:none;width:44px;height:auto;"></td>'
            '<td valign="middle" style="font-family:Arial,sans-serif;">'
            '<div style="font-size:11px;line-height:14px;font-weight:700;color:#bae6fd;text-transform:uppercase;letter-spacing:0.08em;">Call Tracking Metrics</div>'
            '<div style="font-size:18px;line-height:22px;font-weight:700;color:#ffffff;">Daily Executive Summary</div>'
            '</td>'
            '</tr>'
            '</table>'
        )

    return (
        '<table role="presentation" cellpadding="0" cellspacing="0" border="0" style="border-collapse:collapse;">'
        '<tr>'
        '<td valign="middle" style="padding-right:10px;">'
        '<table role="presentation" width="44" height="44" cellpadding="0" cellspacing="0" border="0" '
        'style="width:44px;height:44px;border-collapse:collapse;background:#1f5d8b;">'
        '<tr><td width="22" height="22" style="background:#36b6f4;border-right:2px solid #f2f6fb;border-bottom:2px solid #f2f6fb;"></td>'
        '<td width="22" height="22" style="background:#1f5d8b;border-bottom:2px solid #f2f6fb;"></td></tr>'
        '<tr><td width="22" height="22" style="background:#f2f6fb;border-right:2px solid #f2f6fb;"></td>'
        '<td width="22" height="22" style="background:#1e2a55;"></td></tr>'
        '</table>'
        '</td>'
        '<td valign="middle" style="font-family:Arial,sans-serif;">'
        '<div style="font-size:11px;line-height:14px;font-weight:700;color:#bae6fd;text-transform:uppercase;letter-spacing:0.08em;">Call Tracking Metrics</div>'
        '<div style="font-size:18px;line-height:22px;font-weight:700;color:#ffffff;">Daily Executive Summary</div>'
        '</td>'
        '</tr>'
        '</table>'
    )


def build_exec_bullets(overview: Dict[str, Any], agent_rows: List[Dict[str, Any]], source_rows: List[Dict[str, Any]]) -> List[str]:
    bullets: List[str] = []
    if overview["inbound_calls"] or overview["total_calls"]:
        bullets.append(
            f"{overview['inbound_calls']} inbound leads generated {overview['scheduled_calls']} booked calls, a {fmt_pct(overview['scheduled_rate'])} booking rate."
        )
    if source_rows:
        top_source = source_rows[0]
        bullets.append(
            f"{top_source['source']} led volume with {top_source['calls']} calls and converted at {fmt_pct(top_source['scheduled_rate'])}."
        )
    if agent_rows:
        best_agent = sorted(
            agent_rows,
            key=lambda row: (row["scheduled_calls"], row["scheduled_rate"], row["calls"], row["avg_score"] or 0),
            reverse=True,
        )[0]
        bullets.append(
            f"{best_agent['agent_name']} led the team with {best_agent['scheduled_calls']} booked calls on {best_agent['calls']} conversations."
        )
    return bullets[:3]


def build_team_coaching_insights(agent_rows: List[Dict[str, Any]], overview: Dict[str, Any]) -> Dict[str, Any]:
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
    summarized = [row for row in records if row["summary"]]
    best_calls = sorted(
        [row for row in summarized if row["did_schedule"] == "Yes"],
        key=lambda row: (row["score"] or 0, row["talk_time"], row["unix_time"]),
        reverse=True,
    )[:3]
    missed_calls = sorted(
        [
            row
            for row in summarized
            if row["did_schedule"] != "Yes" and (row["objection"] not in {"None", "Unknown", ""} or row["missed_questions"])
        ],
        key=lambda row: (row["talk_time"], -(row["score"] or 0), row["unix_time"]),
        reverse=True,
    )[:3]
    recent_calls = summarized[:8]
    return {
        "best_calls": best_calls,
        "missed_calls": missed_calls,
        "recent_calls": recent_calls,
    }


def generate_html_report(dashboard: Dict[str, Any], max_detail_rows: int = 75, logo_url: str = "") -> str:
    overview = dashboard["overview"]
    agent_rows = dashboard["agent_breakdown"]
    source_rows = dashboard["source_breakdown"]
    team_insights = build_team_coaching_insights(agent_rows, overview)
    exec_bullets = build_exec_bullets(overview, agent_rows, source_rows)
    notable_calls = build_notable_calls(dashboard["call_details"])
    summarized_detail_rows = [
        row for row in dashboard["call_details"] if safe_str(row.get("summary"))
    ]
    detail_rows = summarized_detail_rows[:max_detail_rows]
    sorted_agent_rows = sorted(
        agent_rows,
        key=lambda row: (row["scheduled_calls"], row["scheduled_rate"], row["calls"], row["avg_score"] or 0),
        reverse=True,
    )

    top_agent = "—"
    if sorted_agent_rows:
        top_agent = sorted_agent_rows[0]["agent_name"]

    top_source = source_rows[0]["source"] if source_rows else "—"

    trend_rows = [
        row
        for row in agent_rows
        if row["service_counter"] or row["outcome_counter"] or row["missed_counter"] or row["objection_counter"]
    ]

    exec_snapshot = render_email_bullets(exec_bullets)
    coaching_snapshot = render_email_table(
        ["Teamwide Focus", "Most Common"],
        [
            ["Missed questions", esc(top_items(team_insights["top_missed"], 6))],
            ["Objections", esc(top_items(team_insights["top_objections"], 6))],
            ["Outcomes", esc(top_items(team_insights["top_outcomes"], 6))],
        ],
        ["28%", "72%"],
    )

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
        [
            "Agent",
            "Handled",
            "Booked",
            "Booked Rate",
            "Answered Rate",
            "Avg Score",
            "Primary Source",
            "Coaching Focus",
        ],
        [
            [
                esc(row["agent_name"]),
                esc(row["calls"]),
                esc(row["scheduled_calls"]),
                esc(fmt_pct(row["scheduled_rate"])),
                esc(fmt_pct(row["answered_rate"])),
                esc(fmt_pct(row["avg_score"])),
                esc(row["top_source"]),
                esc(top_items(row["missed_counter"], 2) if row["missed_counter"] else top_items(row["objection_counter"], 2)),
            ]
            for row in sorted_agent_rows
        ],
        ["19%", "9%", "9%", "11%", "11%", "10%", "15%", "16%"],
    )

    source_table = render_email_table(
        [
            "Source",
            "Lead Volume",
            "Booked",
            "Booked Rate",
            "Answered Rate",
            "Avg Score",
            "Top Agent",
            "Top Outcome",
        ],
        [
            [
                esc(row["source"]),
                esc(row["calls"]),
                esc(row["scheduled_calls"]),
                esc(fmt_pct(row["scheduled_rate"])),
                esc(fmt_pct(row["answered_rate"])),
                esc(fmt_pct(row["avg_score"])),
                esc(row["top_agent"]),
                esc(row["top_outcome"]),
            ]
            for row in source_rows
        ],
        ["22%", "10%", "9%", "11%", "11%", "10%", "14%", "13%"],
    )

    trends_table = render_email_table(
        ["Agent", "Service Mix", "Outcomes", "Missed Questions", "Objections"],
        [
            [
                esc(row["agent_name"]),
                esc(top_items(row["service_counter"], 4)),
                esc(top_items(row["outcome_counter"], 4)),
                esc(top_items(row["missed_counter"], 4)),
                esc(top_items(row["objection_counter"], 4)),
            ]
            for row in trend_rows
        ],
        ["18%", "20%", "20%", "24%", "18%"],
    )

    detail_table = render_email_table(
        [
            "Called At",
            "Agent",
            "Source",
            "Status",
            "Service",
            "Outcome",
            "Scheduled",
            "Score",
            "Summary",
        ],
        [
            [
                esc(row["called_at"]),
                esc(row["agent_name"]),
                esc(row["source"]),
                esc(row["dial_status"] or row["status"]),
                esc(row["service_type"]),
                esc(row["call_outcome"]),
                esc(row["did_schedule"]),
                esc(fmt_pct(row["score"])),
                esc(row["summary"] or row["explanation_of_outcome"] or "—"),
            ]
            for row in detail_rows
        ],
        ["14%", "10%", "12%", "8%", "9%", "10%", "8%", "7%", "22%"],
    )

    notable_html = (
        f"{render_email_call_block('Booked Wins', '', notable_calls['best_calls'])}"
        '<div style="height:6px;line-height:6px;font-size:6px;">&nbsp;</div>'
        f"{render_email_call_block('Missed Opportunities', '', notable_calls['missed_calls'])}"
    )

    no_agent_trends_html = (
        '<div style="font-family:Arial,sans-serif;font-size:14px;line-height:20px;color:#6b7280;">No agent trends available.</div>'
    )
    brand_block = render_brand_block(logo_url)

    return f"""<!DOCTYPE html>
<html lang="en" xmlns="http://www.w3.org/1999/xhtml">
<body style="margin:0;padding:0;background-color:#ffffff;">
  <div style="display:none;max-height:0;overflow:hidden;opacity:0;color:transparent;">
    Daily CTM summary with bookings, agent scorecard, source scorecard, coaching insights, and notable calls.
  </div>
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="width:100%;background-color:#ffffff;margin:0;padding:0;">
    <tr>
      <td align="left" style="padding:0;">
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="width:100%;border-collapse:collapse;background-color:#fcf8f1;">
          <tr>
            <td style="padding:0;background-color:#1f5d8b;border-bottom:4px solid #36b6f4;">
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="width:100%;border-collapse:collapse;">
                <tr>
                  <td valign="middle" align="left" style="padding:8px 10px 8px 10px;">
                    {brand_block}
                    <div style="padding-top:4px;font-family:Arial,sans-serif;font-size:12px;line-height:16px;color:#d9edf7;">
                      {esc(dashboard['report_label'])}
                    </div>
                  </td>
                  <td valign="middle" align="right" style="padding:8px 10px 8px 0;font-family:Arial,sans-serif;white-space:nowrap;">
                    <span style="display:inline-block;padding:6px 8px;background-color:#eff8f7;border:1px solid #b9d7d1;font-size:11px;line-height:14px;font-weight:700;color:#0f766e;">
                      Generated {esc(dashboard['generated_at'])}
                    </span>
                  </td>
                </tr>
              </table>
            </td>
          </tr>

          <tr>
            <td style="padding:2px 6px 0 6px;">{kpi_html}</td>
          </tr>

          <tr>
            <td style="padding:2px 6px 2px 6px;">
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#fdfaf4;border:1px solid #ddd5c7;border-collapse:separate;">
                <tr><td style="padding:10px 10px 2px 10px;font-family:Arial,sans-serif;font-size:18px;line-height:22px;font-weight:700;color:#1f2937;">Executive Snapshot</td></tr>
                <tr><td style="padding:0 10px 2px 10px;">{exec_snapshot}</td></tr>
                <tr><td style="padding:0 10px 10px 10px;font-family:Arial,sans-serif;font-size:13px;line-height:19px;color:#4b5563;">
                  Summarized conversations: <b>{esc(overview['summarized_calls'])}</b> &nbsp; | &nbsp;
                  New callers: <b>{esc(overview['new_callers'])}</b> &nbsp; | &nbsp;
                  Inbound / Outbound: <b>{esc(overview['inbound_calls'])} / {esc(overview['outbound_calls'])}</b> &nbsp; | &nbsp;
                  Avg ring / hold / wait: <b>{esc(fmt_sec(overview['avg_ring_time']))} / {esc(fmt_sec(overview['avg_hold_time']))} / {esc(fmt_sec(overview['avg_wait_time']))}</b>
                </td></tr>
              </table>
            </td>
          </tr>

          <tr><td style="padding:2px 6px;">
            <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#fdfaf4;border:1px solid #ddd5c7;border-collapse:separate;">
              <tr><td style="padding:10px 10px 4px 10px;font-family:Arial,sans-serif;font-size:18px;line-height:22px;font-weight:700;color:#1f2937;">Agent Scorecard</td></tr>
              <tr><td style="padding:0 10px 8px 10px;">{agent_table}</td></tr>
            </table>
          </td></tr>

          <tr><td style="padding:2px 6px;">
            <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#fdfaf4;border:1px solid #ddd5c7;border-collapse:separate;">
              <tr><td style="padding:10px 10px 4px 10px;font-family:Arial,sans-serif;font-size:18px;line-height:22px;font-weight:700;color:#1f2937;">Source Scorecard</td></tr>
              <tr><td style="padding:0 10px 8px 10px;">{source_table}</td></tr>
            </table>
          </td></tr>

          <tr><td style="padding:2px 6px;">
            <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#fdfaf4;border:1px solid #ddd5c7;border-collapse:separate;">
              <tr><td style="padding:10px 10px 4px 10px;font-family:Arial,sans-serif;font-size:18px;line-height:22px;font-weight:700;color:#1f2937;">Team Coaching Insights</td></tr>
              <tr><td style="padding:0 10px 8px 10px;">{coaching_snapshot}</td></tr>
            </table>
          </td></tr>

          <tr><td style="padding:2px 6px;">
            <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#fdfaf4;border:1px solid #ddd5c7;border-collapse:separate;">
              <tr><td style="padding:10px 10px 4px 10px;font-family:Arial,sans-serif;font-size:18px;line-height:22px;font-weight:700;color:#1f2937;">AI Trends By Agent</td></tr>
              <tr><td style="padding:0 10px 8px 10px;">{trends_table if trend_rows else no_agent_trends_html}</td></tr>
            </table>
          </td></tr>

          <tr><td style="padding:2px 6px;">
            <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#fdfaf4;border:1px solid #ddd5c7;border-collapse:separate;">
              <tr><td style="padding:10px 10px 4px 10px;font-family:Arial,sans-serif;font-size:18px;line-height:22px;font-weight:700;color:#1f2937;">Notable Summarized Calls</td></tr>
              <tr><td style="padding:0 10px 8px 10px;">{notable_html}</td></tr>
            </table>
          </td></tr>

          <tr><td style="padding:2px 6px 6px 6px;">
            <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#fdfaf4;border:1px solid #ddd5c7;border-collapse:separate;">
              <tr><td style="padding:10px 10px 4px 10px;font-family:Arial,sans-serif;font-size:18px;line-height:22px;font-weight:700;color:#1f2937;">Recent Summarized Call Detail</td></tr>
              <tr><td style="padding:0 10px 8px 10px;">{detail_table}</td></tr>
            </table>
          </td></tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>"""


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def write_csv(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name) for name in fieldnames})


def export_files(dashboard: Dict[str, Any], html_report: str, out_dir: Path, slug: str) -> Dict[str, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)

    html_path = out_dir / f"ctm_daily_executive_summary_{slug}.html"
    json_path = out_dir / f"ctm_daily_executive_summary_{slug}.json"
    calls_csv_path = out_dir / f"ctm_daily_calls_{slug}.csv"
    agent_csv_path = out_dir / f"ctm_daily_agents_{slug}.csv"
    source_csv_path = out_dir / f"ctm_daily_sources_{slug}.csv"
    matrix_csv_path = out_dir / f"ctm_daily_agent_source_{slug}.csv"

    html_path.write_text(html_report, encoding="utf-8")
    write_json(json_path, dashboard)

    write_csv(
        calls_csv_path,
        dashboard["call_details"],
        [
            "id",
            "called_at",
            "agent_name",
            "agent_email",
            "source",
            "direction",
            "status",
            "dial_status",
            "duration",
            "talk_time",
            "ring_time",
            "hold_time",
            "wait_time_s",
            "service_type",
            "call_outcome",
            "did_schedule",
            "objection",
            "missed_questions_text",
            "score",
            "rating",
            "summary",
            "explanation_of_outcome",
            "tracking_label",
            "caller_name",
            "city",
            "state",
        ],
    )

    write_csv(
        agent_csv_path,
        [
            {
                "agent_name": row["agent_name"],
                "calls": row["calls"],
                "answered_calls": row["answered_calls"],
                "answered_rate": round(row["answered_rate"], 2),
                "scheduled_calls": row["scheduled_calls"],
                "scheduled_rate": round(row["scheduled_rate"], 2),
                "avg_score": round(row["avg_score"], 2) if row["avg_score"] is not None else "",
                "avg_talk_time": round(row["avg_talk_time"], 2),
                "avg_ring_time": round(row["avg_ring_time"], 2),
                "avg_hold_time": round(row["avg_hold_time"], 2),
                "avg_wait_time": round(row["avg_wait_time"], 2),
                "top_source": row["top_source"],
                "top_outcome": row["top_outcome"],
                "top_service": row["top_service"],
                "missed_questions": top_items(row["missed_counter"], 6),
                "objections": top_items(row["objection_counter"], 6),
            }
            for row in dashboard["agent_breakdown"]
        ],
        [
            "agent_name",
            "calls",
            "answered_calls",
            "answered_rate",
            "scheduled_calls",
            "scheduled_rate",
            "avg_score",
            "avg_talk_time",
            "avg_ring_time",
            "avg_hold_time",
            "avg_wait_time",
            "top_source",
            "top_outcome",
            "top_service",
            "missed_questions",
            "objections",
        ],
    )

    write_csv(
        source_csv_path,
        [
            {
                "source": row["source"],
                "calls": row["calls"],
                "answered_calls": row["answered_calls"],
                "answered_rate": round(row["answered_rate"], 2),
                "scheduled_calls": row["scheduled_calls"],
                "scheduled_rate": round(row["scheduled_rate"], 2),
                "avg_score": round(row["avg_score"], 2) if row["avg_score"] is not None else "",
                "avg_talk_time": round(row["avg_talk_time"], 2),
                "top_agent": row["top_agent"],
                "top_outcome": row["top_outcome"],
            }
            for row in dashboard["source_breakdown"]
        ],
        [
            "source",
            "calls",
            "answered_calls",
            "answered_rate",
            "scheduled_calls",
            "scheduled_rate",
            "avg_score",
            "avg_talk_time",
            "top_agent",
            "top_outcome",
        ],
    )

    write_csv(
        matrix_csv_path,
        [
            {
                "agent_name": row["agent_name"],
                "source": row["source"],
                "calls": row["calls"],
                "scheduled_calls": row["scheduled_calls"],
                "scheduled_rate": round(row["scheduled_rate"], 2),
                "avg_score": round(row["avg_score"], 2) if row["avg_score"] is not None else "",
            }
            for row in dashboard["agent_source_breakdown"]
        ],
        [
            "agent_name",
            "source",
            "calls",
            "scheduled_calls",
            "scheduled_rate",
            "avg_score",
        ],
    )

    return {
        "html": html_path,
        "json": json_path,
        "calls_csv": calls_csv_path,
        "agents_csv": agent_csv_path,
        "sources_csv": source_csv_path,
        "agent_source_csv": matrix_csv_path,
    }


def sanitize_for_json(payload: Dict[str, Any]) -> Dict[str, Any]:
    safe_payload = json.loads(json.dumps(payload, default=str))
    for row in safe_payload.get("agent_breakdown", []):
        for key in ["source_counter", "outcome_counter", "service_counter", "objection_counter", "missed_counter"]:
            if key in row:
                row[key] = dict(row[key])
    for row in safe_payload.get("source_breakdown", []):
        if "agent_counter" in row:
            row["agent_counter"] = dict(row["agent_counter"])
    if "overview" in safe_payload and "top_outcomes" in safe_payload["overview"]:
        safe_payload["overview"]["top_outcomes"] = dict(safe_payload["overview"]["top_outcomes"])
    return safe_payload


def post_to_webhook(webhook_url: str, html_report: str) -> None:
    response = requests.post(
        webhook_url,
        headers={"Content-Type": "text/html; charset=utf-8"},
        data=html_report.encode("utf-8"),
        timeout=30,
    )
    response.raise_for_status()


def resolve_auth_header(cli_auth_header: str = "", cli_api_key: str = "") -> str:
    raw_auth = safe_str(cli_auth_header) or safe_str(os.getenv("CTM_AUTH")) or safe_str(os.getenv("CTM_BASIC_AUTH"))
    api_key = (
        safe_str(cli_api_key)
        or safe_str(HARDCODED_CTM_API_KEY)
        or safe_str(os.getenv("CTM_API_KEY"))
    )
    if raw_auth:
        return raw_auth
    if api_key:
        return f"Basic {api_key}"
    raise RuntimeError(
        "Missing CTM auth. Provide one of: "
        "--auth-header 'Basic ...', --api-key '<base64 token>', "
        "HARDCODED_CTM_API_KEY, CTM_AUTH, CTM_BASIC_AUTH, or CTM_API_KEY."
    )


def build_slug(time_duration: str) -> str:
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    cleaned = re.sub(r"[^a-zA-Z0-9_-]+", "_", time_duration).strip("_").lower()
    return f"{cleaned}_{stamp}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a daily CTM executive summary report.")
    parser.add_argument("--account-id", default=ACCOUNT_ID)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--auth-header", default="")
    parser.add_argument("--api-key", default="")
    parser.add_argument("--time-duration", default=os.getenv("CTM_TIME_DURATION", DEFAULT_TIME_DURATION))
    parser.add_argument("--per-page", type=int, default=100)
    parser.add_argument("--out-dir", default=str(Path.home() / "ctm_daily_reports"))
    parser.add_argument("--webhook-url", default=os.getenv("WEBHOOK_URL", DEFAULT_WEBHOOK_URL))
    parser.add_argument("--logo-url", default=os.getenv("CTM_LOGO_URL", DEFAULT_LOGO_URL))
    parser.add_argument("--sleep", type=float, default=0.0, help="Sleep between detail calls.")
    parser.add_argument("--max-detail-rows", type=int, default=75)
    parser.add_argument("--print-html", action="store_true")
    parser.add_argument("--no-hydrate-details", action="store_true", help="Skip per-call detail fetches.")
    parser.add_argument("--no-webhook", action="store_true", help="Do not post the HTML report.")

    args = parser.parse_args()

    if not safe_str(args.account_id):
        raise RuntimeError("Missing account id. Set CTM_ACCOUNT_ID or pass --account-id.")

    auth_header = resolve_auth_header(args.auth_header, args.api_key)
    client = CTMClient(
        account_id=args.account_id,
        base_url=args.base_url,
        auth_header=auth_header,
    )

    print(f"Fetching CTM calls for time_duration={args.time_duration}...")
    calls = client.fetch_calls(time_duration=args.time_duration, per_page=args.per_page)
    print(f"Fetched {len(calls)} calls from list endpoint.")

    calls = hydrate_calls(
        client=client,
        calls=calls,
        hydrate_details=not args.no_hydrate_details,
        sleep_s=args.sleep,
    )

    records = [extract_call_record(call) for call in calls]
    report_label = f"Account {args.account_id} | time_duration={args.time_duration}"
    dashboard = build_dashboard(records, report_label=report_label)
    safe_dashboard = sanitize_for_json(dashboard)
    html_report = generate_html_report(
        safe_dashboard,
        max_detail_rows=args.max_detail_rows,
        logo_url=args.logo_url,
    )

    if args.print_html:
        print(html_report)

    slug = build_slug(args.time_duration)
    paths = export_files(
        dashboard=safe_dashboard,
        html_report=html_report,
        out_dir=Path(args.out_dir),
        slug=slug,
    )

    if not args.no_webhook and args.webhook_url:
        post_to_webhook(args.webhook_url, html_report)
        print("Webhook post succeeded.")

    print("Report outputs:")
    for name, path in paths.items():
        print(f"  {name}: {path}")


if __name__ == "__main__":
    main()
