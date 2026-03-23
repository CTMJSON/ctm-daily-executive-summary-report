# Daily CTM Executive Summary Report

Daily CallTrackingMetrics reporting script that:

- fetches yesterday's CTM activities
- follows CTM pagination
- optionally hydrates each activity with the detail endpoint
- aggregates operational and AI-derived metrics by agent and source
- generates an email-safe HTML executive summary
- posts the HTML to a Make.com webhook for downstream email delivery

The intended flow is:

1. A scheduled worker runs the script once per day.
2. The script fetches CTM activity data and builds the report.
3. The script posts the HTML to a Make.com webhook.
4. Make passes the HTML into a Gmail module to send the email.

This repo is generic. Replace the placeholders with your own CTM account, auth, webhook, and optional logo.

## Requirements

- CTM API access
- Make.com scenario with:
  - Custom Webhook module
  - Gmail module
- Gmail account connected to Make
- A daily scheduler such as:
  - PythonAnywhere scheduled task
  - cron
  - GitHub Actions
  - another job runner

## Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Configuration

Set these environment variables, or pass the equivalent CLI args:

```bash
export CTM_ACCOUNT_ID="YOUR_ACCOUNT_ID"
export CTM_API_KEY="YOUR_BASE64_CTM_BASIC_AUTH_TOKEN"
export CTM_WEBHOOK_URL="https://hook.us1.make.com/your-webhook"
export CTM_LOGO_URL="https://your-public-logo-url"
```

Notes:

- `CTM_API_KEY` should be the base64 CTM token that gets sent as `Authorization: Basic <token>`.
- `CTM_WEBHOOK_URL` is optional if you run with `--no-webhook`.
- `CTM_LOGO_URL` is optional.

## Run

```bash
python3 ctm_daily_executive_summary.py
```

Useful flags:

```bash
python3 ctm_daily_executive_summary.py --time-duration yesterday
python3 ctm_daily_executive_summary.py --no-hydrate-details
python3 ctm_daily_executive_summary.py --no-webhook
python3 ctm_daily_executive_summary.py --max-detail-rows 25
```

## Daily Scheduling

Typical deployment is a scheduled job that runs once per day.

Example options:

- PythonAnywhere scheduled task
- Linux cron job
- another hosted worker that can run Python on a schedule

The script does not require a web server. It just needs outbound network access to:

- CTM API
- Make webhook

## Make.com + Gmail Workflow

Recommended Make scenario:

1. Custom Webhook module receives the HTML payload from this script.
2. Gmail module sends an email using the webhook body as HTML content.

That means the full workflow requires access to:

- CTM
- Make.com
- Gmail
- a scheduled Python runtime

## Swapping AI Custom Fields

The script is written around CTM `custom_fields`, but you can swap the mapped fields to use any AI-generated custom fields available in your CTM account.

The main mapping happens in:

- `extract_call_record()`

Current examples include:

- `custom_fields["service_type"]`
- `custom_fields["call_outcome"]`
- `custom_fields["did_the_caller_schedule"]`
- `custom_fields["objections_reasons_not_scheduled"]`
- `custom_fields["missed_questions"]`
- `custom_fields["cumulative_score_percentage"]`
- `custom_fields["explanation_of_outcome"]`

If your CTM AI setup uses different field names:

1. Update the keys inside `extract_call_record()`.
2. If needed, update the aggregation logic in:
   - `build_overview()`
   - `build_agent_breakdown()`
   - `build_source_breakdown()`
   - `build_team_coaching_insights()`
3. Update table headers or labels in `generate_html_report()` if the new fields describe different concepts.

Example:

If your account uses:

- `ai_summary_score`
- `appointment_booked`
- `primary_objection`

then replace the existing references in `extract_call_record()` with those names and keep the rest of the pipeline the same.

## Outputs

Each run writes:

- HTML report
- JSON summary
- CSV exports for:
  - calls
  - agents
  - sources
  - agent/source combinations

## Security Notes

- Do not commit real CTM tokens, webhook URLs, or account-specific identifiers.
- Prefer environment variables or a secure secrets store for production.
- If you use a hardcoded fallback during deployment, keep that version private.
