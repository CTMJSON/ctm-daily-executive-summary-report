# CTM Daily Executive Summary

> A daily automated email report that turns yesterday's CTM call activity into a polished executive summary — agent scorecards, source performance, coaching insights, and notable call highlights, delivered to your inbox every morning.

---

## What it does

Every morning, this script fetches the previous day's calls from the CTM API, aggregates them into operational and AI-derived metrics, and delivers a complete HTML executive summary via Make.com + Gmail.

The report includes:

- **KPI bar** — inbound leads, answered rate, booking rate, avg AI score, top agent, top source
- **Executive snapshot** — auto-generated narrative bullets summarizing the day
- **Agent scorecard** — calls handled, booked rate, answered rate, avg AI score, coaching focus per agent
- **Source scorecard** — lead volume, conversion, and agent coverage by tracking source
- **Team coaching insights** — most-missed questions, top objections, outcome distribution
- **AI trends by agent** — service mix, outcomes, missed questions, and objections broken down per agent
- **Notable calls** — booked wins and missed opportunities surfaced from AI summaries
- **Full call detail table** — every summarized call with outcome, score, and AI summary text

---

## How it works

```
CTM API  →  Python script  →  Make.com webhook  →  Gmail
```

1. A scheduled job runs the script once per day.
2. The script fetches and aggregates the previous day's call data from CTM.
3. It POSTs the finished HTML to a Make.com webhook.
4. Make passes the HTML body to a Gmail module, which sends the email.

The script also writes local HTML, JSON, and CSV exports each run for archiving or downstream use.

---

## Requirements

- Python 3.9+
- CTM API access
- Make.com scenario with a Custom Webhook module and a Gmail module
- A daily scheduler (cron, PythonAnywhere, GitHub Actions, etc.)

---

## Install

```bash
python3 -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

---

## Configuration

Set these environment variables (or pass the equivalent CLI flags):

```bash
export CTM_ACCOUNT_ID="your_account_id"
export CTM_API_KEY="your_base64_ctm_basic_auth_token"
export CTM_WEBHOOK_URL="https://hook.us1.make.com/your-webhook"
export CTM_LOGO_URL="https://your-public-logo-url"   # optional
export CTM_ONVERTED_FIELD="did_the_caller_schedule"  # optional (custom booked/converted field)
export CTM_SCORE_FIELD="cumulative_score_percentage" # optional (custom AI score field)
```

`CTM_API_KEY` is the base64 token sent as `Authorization: Basic <token>`.  
`CTM_WEBHOOK_URL` is optional if you run with `--no-webhook`.  
`CTM_LOGO_URL` is optional — a default CTM logomark block is rendered if omitted.

---

## Run

```bash
# Standard daily run — fetches yesterday, posts to webhook
python3 ctm_daily_executive_summary.py

# Useful flags
python3 ctm_daily_executive_summary.py --time-duration yesterday
python3 ctm_daily_executive_summary.py --no-hydrate-details    # faster, skips per-call detail API calls
python3 ctm_daily_executive_summary.py --no-webhook            # write files only, skip email
python3 ctm_daily_executive_summary.py --max-detail-rows 25    # limit rows in the detail table
python3 ctm_daily_executive_summary.py --export-all            # write CSV + JSON
python3 ctm_daily_executive_summary.py --export-csv            # write CSV only
python3 ctm_daily_executive_summary.py --export-json           # write JSON only
```

---

## All options

| Flag | Default | Description |
|---|---|---|
| `--time-duration` | `yesterday` | CTM time duration filter (`yesterday`, `today`, `last_7_days`, etc.) |
| `--no-hydrate-details` | off | Skip per-call detail API calls (faster but may have less custom field data) |
| `--no-webhook` | off | Skip posting to Make.com |
| `--export-csv` | off | Write CSV exports |
| `--export-json` | off | Write JSON export |
| `--export-all` | off | Write CSV + JSON exports |
| `--max-detail-rows N` | `75` | Max rows shown in the call detail table |
| `--sleep-s F` | `0.1` | Seconds to sleep between per-call detail API requests |

---

## Scheduling

The script is designed to run unattended once per day. Any of the following work:

- **PythonAnywhere** scheduled task — simplest option for Python-only deploys
- **Linux cron**: `0 7 * * * /path/to/.venv/bin/python /path/to/ctm_daily_executive_summary.py`
- **GitHub Actions** with a `schedule:` trigger
- Any hosted job runner with outbound access to the CTM API and Make.com webhook

The script needs no web server — just outbound network access.

---

## Make.com + Gmail setup

1. Create a new Make scenario.
2. Add a **Custom Webhook** module — copy its URL into `CTM_WEBHOOK_URL`.
3. Add a **Gmail** module connected to your send account, configured to use the webhook payload as the HTML email body.
4. Activate the scenario.

When the script runs and POSTs the HTML, Make routes it through Gmail automatically.

---

## Adapting to your CTM custom fields

The script maps specific CTM AI custom fields inside `extract_call_record()`. If your account uses different field names, you can set these:

```
CTM_ONVERTED_FIELD   # booked/converted indicator
CTM_SCORE_FIELD      # AI score field
```

Or pass:

```
--converted-field <field_key>
--score-field <field_key>
```

For other fields, update the keys in `extract_call_record()`:

```python
# Current mappings in extract_call_record()
cf.get("service_type")
cf.get("call_outcome")
cf.get("did_the_caller_schedule")
cf.get("objections_reasons_not_scheduled")
cf.get("missed_questions")
cf.get("cumulative_score_percentage")
cf.get("explanation_of_outcome")
```

If you rename any of these, also update the aggregation logic in `build_overview()`, `build_agent_breakdown()`, `build_source_breakdown()`, and `build_team_coaching_insights()`, and update the column labels in `generate_html_report()` to match.

---

## Outputs

Each run writes the following files dated to the report period:

| File | Contents |
|---|---|
| `ctm_daily_summary_YYYY-MM-DD.html` | Full email-ready HTML report |
| `ctm_daily_summary_YYYY-MM-DD.json` | Complete aggregated dashboard data |
| `ctm_calls_YYYY-MM-DD.csv` | Raw normalized call records |
| `ctm_agents_YYYY-MM-DD.csv` | Agent breakdown |
| `ctm_sources_YYYY-MM-DD.csv` | Source breakdown |
| `ctm_agent_sources_YYYY-MM-DD.csv` | Agent × source matrix |

---

## Security

- Never commit CTM API keys, webhook URLs, or account IDs to version control.
- Use environment variables or a secrets manager in production.
- The `.gitignore` in this repo excludes common credential file patterns.

---

## Files

| File | Description |
|---|---|
| `ctm_daily_executive_summary.py` | Main script |
| `requirements.txt` | Python dependencies |
