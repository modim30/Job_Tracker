# Job_Tracker

Job_Tracker reads job-related emails from two Gmail accounts, extracts structured application updates with Gemini, and writes deduplicated records to a Google Sheet.

## What it does

- Fetches recent job emails from Gmail (`tracker.py`)
- Extracts:
  - Company Name
  - Application Status
  - Role
  - Job Ref ID
  - Date Submitted
  - Rejection Reason
- Upserts results into a Google Sheet and keeps one latest row per application

## Automation

GitHub Actions workflow: `.github/workflows/main.yml`

- Scheduled run: every 6 hours (`0 */6 * * *`)
- Manual run: supported via **Run workflow**

## Required GitHub Secrets

- `MEETMODI_JSON` (Google OAuth authorized user JSON for one Gmail account)
- `MODIM_JSON` (Google OAuth authorized user JSON for the second Gmail account + Sheets write access)
- `GEMINI_API_KEY`
- `SPREADSHEET_ID`

The workflow recreates:

- `meetmodi400.json`
- `modim417.json`

from secrets at runtime.

## Local setup (optional)

1. Install dependencies:
   - `requests`
   - `google-api-python-client`
   - `google-auth`
   - `google-auth-httplib2`
   - `google-auth-oauthlib`
2. Set environment variables:
   - `GEMINI_API_KEY`
   - `SPREADSHEET_ID`
   - Optional: `FETCH_LOOKBACK_HOURS` (default: `12`)
3. Provide credential JSON files expected by `tracker.py`:
   - `meetmodi400.json`
   - `modim417.json`
4. Run:
   - `python tracker.py`

## Token helper script

`token_fetching_script.py` can generate a `tokens.json` file from `credentials.json` for OAuth login flows when running locally.
