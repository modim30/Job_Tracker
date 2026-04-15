import json
import os
import re
import time
import base64
import html
from datetime import datetime
from email.utils import parsedate_to_datetime
import requests
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

# === Configuration ===
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID')
SHEET_NAME = 'Sheet1'
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')
GEMINI_MODEL_CANDIDATES = [
    'gemini-2.5-flash',
    'gemini-3-flash-preview',
    'gemini-3.1-flash-lite-preview',
    'gemini-2.5-flash-lite',
]
MAX_RETRIES = 5
EMAIL_CHUNK_SIZE = 50

SHEET_HEADERS = [
    "Company Name",
    "Application Status",
    "Role",
    "Job Ref ID",
    "Date Submitted",
    "Rejection Reason",
    "Last Updated"
]


def fetch_emails():
    accounts = ['meetmodi400.json', 'modim417.json']
    all_raw_emails = []

    def decode_base64url(data):
        if not data:
            return ""
        try:
            return base64.urlsafe_b64decode(data.encode('utf-8')).decode('utf-8', errors='ignore')
        except Exception:
            return ""

    def extract_plain_text(payload):
        if not isinstance(payload, dict):
            return ""

        mime_type = (payload.get('mimeType') or '').lower()
        body_data = payload.get('body', {}).get('data', '')

        if mime_type == 'text/plain':
            return decode_base64url(body_data)

        for part in payload.get('parts', []) or []:
            text = extract_plain_text(part)
            if text:
                return text

        return ""

    def html_to_text(raw_html):
        if not raw_html:
            return ""
        text = re.sub(r'(?is)<(script|style).*?>.*?</\1>', ' ', raw_html)
        text = re.sub(r'(?s)<[^>]+>', ' ', text)
        text = html.unescape(text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def extract_html_text(payload):
        if not isinstance(payload, dict):
            return ""

        mime_type = (payload.get('mimeType') or '').lower()
        body_data = payload.get('body', {}).get('data', '')

        if mime_type == 'text/html':
            return html_to_text(decode_base64url(body_data))

        for part in payload.get('parts', []) or []:
            text = extract_html_text(part)
            if text:
                return text

        return ""

    # 1. Gather all snippets first
    for acc in accounts:
        print(f"Fetching from {acc}...")
        try:
            creds = Credentials.from_authorized_user_file(acc)
            service = build('gmail', 'v1', credentials=creds)
            query = (
                '(application OR "thank you for applying" OR "we received your application" '
                'OR "not moving forward" OR "unfortunately" OR "interview" OR "offer" OR "thanks") '
                'newer_than:4d'
                )

            results = service.users().messages().list(
                userId='me',
                q=query,
                maxResults=500
            ).execute()

            for msg in results.get('messages', []):
                m = service.users().messages().get(userId='me', id=msg['id'], format='full').execute()
                headers = {h['name']: h['value'] for h in m.get('payload', {}).get('headers', [])}
                raw_date = headers.get('Date', '')
                try:
                    email_date = parsedate_to_datetime(raw_date).date().isoformat() if raw_date else ''
                except Exception:
                    email_date = ''

                plain_body = extract_plain_text(m.get('payload', {})).strip()
                html_body = extract_html_text(m.get('payload', {})).strip()
                snippet_body = (m.get('snippet', '') or '').strip()
                final_body = plain_body or html_body or snippet_body
                all_raw_emails.append({
                    'from': headers.get('From', ''),
                    'subject': headers.get('Subject', ''),
                    'date': email_date,
                    'body': final_body
                })
        except Exception as e:
            print(f"Failed to fetch emails for {acc}: {e}")

    return all_raw_emails

def extract_job_ref_id(email):
    combined_text = " ".join([
        email.get('from', ''),
        email.get('subject', ''),
        email.get('body', '')
    ])
    patterns = [
        r'(?i)(?:job\s*ref(?:erence)?|ref\s*id|job\s*id|requisition\s*id|req\s*id|application\s*id)[:#\-\s]*([A-Za-z0-9._/\-]+)',
        r'(?i)(?:requisition|req)\s*(?:#|no\.?|number)?[:#\-\s]*([A-Za-z0-9._/\-]+)'
    ]
    for pattern in patterns:
        match = re.search(pattern, combined_text)
        if match:
            return match.group(1).strip()
    return ""


def build_gemini_url(model_name):
    return f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent"


def ask_gemini_batched(email_list, start_index=0):
    if not email_list:
        return []

    # Join emails with delimiters for a single batched prompt
    combined_emails = "\n---\n".join([
        f"EMAIL {start_index + i + 1}:\nFrom: {email.get('from', '')}\nSubject: {email.get('subject', '')}\nEmail Date: {email.get('date', '')}\nBody: {email.get('body', '')}"
        for i, email in enumerate(email_list)
    ])

    prompt = f"""
    You are a strict data extraction engine.

    Extract ONLY structured data. DO NOT summarize. DO NOT infer missing values.

    Return a JSON array. Each object MUST follow this schema EXACTLY:

    {{
    "Company Name": string,
    "Application Status": one of ["Submitted", "Rejected", "Interview", "Offer", "Withdrawn", ""],
    "Role": string,
    "Job Ref ID": string,
    "Date Submitted": string (MM/DD/YYYY or ""),
    "Rejection Reason": string,
    "Source Email Index": number,
    "Confidence": number (0 to 1)
    }}

    Rules:
    - If a field is missing → return ""
    - DO NOT write "Not specified"
    - DO NOT merge multiple jobs
    - DO NOT hallucinate role or company name
    - Skip non-job emails
    - Normalize status EXACTLY to allowed values
    - If the email says not moving forward / no longer under consideration / regret to inform / not selected, set Application Status to Rejected
    - If a job reference / requisition / application id exists in the email, capture it in Job Ref ID
    - Use the EMAIL DATE as Date Submitted when the application status is Submitted
    - Include the original EMAIL number in Source Email Index

    EMAILS:
    {combined_emails}
    """

    payload = {
        "contents": [{"parts": [{"text": prompt}]}], 
        "generationConfig": {
            "responseMimeType": "application/json"
        }
    }

    def run_gemini_request(model_name):
        response = None
        for attempt in range(MAX_RETRIES):
            response = requests.post(
                f"{build_gemini_url(model_name)}?key={GEMINI_API_KEY}",
                json=payload,
            )
            print(
                f"Gemini API response status: {response.status_code} "
                f"for model {model_name} (attempt {attempt + 1}/{MAX_RETRIES})"
                f"Response {response.reason}"
            )

            if response.status_code == 200:
                return response
            if response.status_code == 429:
                return None  # Try next model immediately on rate limit

            if attempt < MAX_RETRIES - 1:
                print("Retrying request...")
                time.sleep(2 ** (attempt + 1))

        return response

    response = None
    for model_name in GEMINI_MODEL_CANDIDATES:
        print(f"Using Gemini model: {model_name}...")
        response = run_gemini_request(model_name)
        if response is None:
            continue
        if response.status_code == 200:
            print(f"Gemini request succeeded with model {model_name}")
            break

        if response.status_code in (429, 503):
            print(f"Model {model_name} returned {response.status_code}; trying next model...")
            continue

        print(f"Model {model_name} returned non-retriable status {response.status_code}; stopping.")
        break

    if response is None or response.status_code != 200:
        return []

    try:
        raw_text = response.json()['candidates'][0]['content']['parts'][0]['text']
        text = raw_text.strip()
        if text.startswith('```'):
            # strip any code fence and optional language tag
            text = re.sub(r'^```\w*\n', '', text)
        if text.endswith('```'):
            text = text[:-3].strip()

        data = json.loads(text)
        # Expecting a list of objects
        parsed = []
        if isinstance(data, dict):
            # single object -> wrap
            data = [data]
        for entry in data:
            if not isinstance(entry, dict):
                continue
            obj = {
                "Company Name": entry.get("Company Name", "").strip(),
                "Application Status": entry.get("Application Status", "").strip(),
                "Role": entry.get("Role", "").strip(),
                "Job Ref ID": entry.get("Job Ref ID", "").strip(),
                "Date Submitted": entry.get("Date Submitted", "").strip(),
                "Rejection Reason": entry.get("Rejection Reason", "").strip()
            }
            source_email_index = entry.get("Source Email Index", "")
            try:
                obj["Source Email Index"] = int(source_email_index)
            except Exception:
                obj["Source Email Index"] = None
            parsed.append(obj)
        return parsed
    except Exception as e:
        print(f"Extraction/parsing failed: {e}\nCleaned text: {text}")
        return []


def looks_like_job_ref(value):
    text = (value or '').strip()
    if not text:
        return False
    if re.fullmatch(r'[$€£]?[\d,]+(\.?\d{0,2})?(k|K)?', text):
        return False
    if re.fullmatch(r'\d{1,4}', text):
        return False
    return bool(re.search(r'[A-Za-z]', text)) and bool(re.search(r'[\d\-_/]', text))
    
def update_sheets(extracted_data, source_emails):
    write_creds = Credentials.from_authorized_user_file('modim417.json')
    sheets_service = build('sheets', 'v4', credentials=write_creds)

    def parse_date_str(s):
        if not s:
            return datetime.min
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
            try:
                return datetime.strptime(s, fmt)
            except Exception:
                continue
        try:
            return datetime.fromisoformat(s)
        except Exception:
            return datetime.min

    def format_sheet_date(date_value):
        if not date_value:
            return ""
        if isinstance(date_value, str):
            try:
                return datetime.fromisoformat(date_value).strftime('%m/%d/%Y')
            except Exception:
                return date_value
        if isinstance(date_value, datetime):
            return date_value.strftime('%m/%d/%Y')
        return str(date_value)

    # get existing rows (older sheets may not have Job Ref ID yet)
    existing = sheets_service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=f"{SHEET_NAME}!A:H"
    ).execute().get('values', [])

    def normalize_text(value):
        return (value or "").strip().lower()

    def build_key(company, role, date_sub, status, job_ref_id):
        c = normalize_text(company)
        r = normalize_text(role)
        d = normalize_text(date_sub)
        s = normalize_text(status)
        j = normalize_text(job_ref_id)
        # Best key: job ref id if available
        if j:
            return ("jobref", j)
        # Default key: company + role (status/date changes should update same row, not create new rows)
        if c and r:
            return (c, r)
        # Fallback key: company + date + status
        if c and (d or s):
            return (c, d, s)
        # Fallback key: role + date + status
        if r and (d or s):
            return (r, d, s)
        # Fallback key when company is missing
        return ("__unknown__", r or c, d or s)

    def merge_rows(old_row, new_row):
        # Keep known values from old row when new extraction is missing fields.
        status_priority = {
            "": 0,
            "submitted": 1,
            "interview": 2,
            "withdrawn": 3,
            "rejected": 4,
            "offer": 5
        }
        merged = []
        max_len = max(len(old_row), len(new_row))
        for i in range(max_len):
            old_val = old_row[i] if i < len(old_row) else ""
            new_val = new_row[i] if i < len(new_row) else ""
            if i == 6:
                # Last Updated should always move to latest run date.
                merged.append(new_val or old_val)
            elif i == 1:
                old_status = (old_val or "").strip().lower()
                new_status = (new_val or "").strip().lower()
                old_score = status_priority.get(old_status, 0)
                new_score = status_priority.get(new_status, 0)
                merged.append(new_val if new_score >= old_score else old_val)
            else:
                merged.append(new_val if (new_val or "").strip() else old_val)
        return merged

    # normalize existing to dicts keyed by stable composite key
    unique_map = {}
    for row in existing:
        def cell(idx):
            return (row[idx] or "").strip() if len(row) > idx else ""

        # Skip header rows that may already be present in fetched data.
        if cell(0).lower() == "company name" and cell(1).lower() == "application status":
            continue

        company = cell(0)
        status = cell(1)
        role = cell(2)
        job_ref_candidate = cell(3)
        date_sub_candidate = cell(4)
        rej = cell(5)
        last_updated = cell(6) or cell(5) or cell(4) or cell(3)

        # Legacy rows may still have salary in column D; only treat it as Job Ref ID when it looks like one.
        job_ref_id = job_ref_candidate if looks_like_job_ref(job_ref_candidate) else ""
        date_sub = date_sub_candidate
        if not date_sub and len(row) > 3 and not looks_like_job_ref(job_ref_candidate):
            date_sub = job_ref_candidate if re.search(r'\d{1,2}/\d{1,2}/\d{2,4}|\d{4}-\d{2}-\d{2}', job_ref_candidate or '') else date_sub
        key = build_key(company, role, date_sub, status, job_ref_id)
        dt = parse_date_str(last_updated)
        if key not in unique_map or dt > unique_map[key]['date']:
            normalized_row = [company, status, role, job_ref_id, date_sub, rej, last_updated]
            unique_map[key] = {'row': normalized_row, 'date': dt}

    # Build lookup indexes from existing normalized rows to map partial new records.
    by_jobref = {}
    by_company_role = {}
    for key, value in unique_map.items():
        row = value['row']
        c = normalize_text(row[0] if len(row) > 0 else "")
        r = normalize_text(row[2] if len(row) > 2 else "")
        j = normalize_text(row[3] if len(row) > 3 else "")
        if j:
            by_jobref[j] = key
        if c and r:
            by_company_role[(c, r)] = key

    # merge extracted data
    today = datetime.now().strftime('%Y-%m-%d')
    for entry in extracted_data:
        company = (entry.get('Company Name', '') or '').strip()
        role = (entry.get('Role', '') or '').strip()
        status = (entry.get('Application Status', '') or '').strip()
        source_index = entry.get('Source Email Index')
        job_ref_id = (entry.get('Job Ref ID', '') or '').strip()
        date_sub = entry.get('Date Submitted', '')
        rej = entry.get('Rejection Reason', '')

        source_email = {}
        if isinstance(source_index, int) and 1 <= source_index <= len(source_emails):
            source_email = source_emails[source_index - 1] if source_index - 1 < len(source_emails) else {}

        if not date_sub and source_email:
            date_sub = source_email.get('date', '')

        if status == 'Submitted':
            # For submitted rows, prefer the email date over model output
            date_sub = source_email.get('date', date_sub) if source_email else date_sub

        if not job_ref_id and source_email:
            job_ref_id = extract_job_ref_id(source_email)

        if not company and source_email:
            sender = source_email.get('from', '')
            subject = source_email.get('subject', '')
            sender_match = re.search(r'@([A-Za-z0-9.-]+)', sender)
            if sender_match:
                company = sender_match.group(1).split('.')[0].capitalize()
            elif subject:
                words = re.findall(r'[A-Z][A-Za-z0-9&.-]+', subject)
                if words:
                    company = words[0]

        date_sub = format_sheet_date(date_sub)

        c_norm = normalize_text(company)
        r_norm = normalize_text(role)
        j_norm = normalize_text(job_ref_id)

        # Resolve to an existing key first when extraction is partial/inconsistent.
        resolved_key = None
        if j_norm and j_norm in by_jobref:
            resolved_key = by_jobref[j_norm]
        elif c_norm and r_norm and (c_norm, r_norm) in by_company_role:
            resolved_key = by_company_role[(c_norm, r_norm)]

        key = resolved_key if resolved_key is not None else build_key(company, role, date_sub, status, job_ref_id)
        new_row = [company, status, role, job_ref_id, date_sub, rej, today]
        new_dt = parse_date_str(today)
        if key in unique_map:
            merged_row = merge_rows(unique_map[key]['row'], new_row)
            unique_map[key] = {'row': merged_row, 'date': new_dt}
        else:
            unique_map[key] = {'row': new_row, 'date': new_dt}
            # Keep indexes updated for subsequent entries in this run.
            c_new = normalize_text(company)
            r_new = normalize_text(role)
            j_new = normalize_text(job_ref_id)
            if j_new:
                by_jobref[j_new] = key
            if c_new and r_new:
                by_company_role[(c_new, r_new)] = key

    # Final collapse pass: merge residual duplicates from legacy keys
    collapsed = {}
    for value in unique_map.values():
        row = value['row']
        row_company = normalize_text(row[0] if len(row) > 0 else "")
        row_role = normalize_text(row[2] if len(row) > 2 else "")
        row_job_ref = normalize_text(row[3] if len(row) > 3 else "")
        row_last_updated = parse_date_str(row[6] if len(row) > 6 else "")

        if row_job_ref:
            collapse_key = ("jobref", row_job_ref)
        elif row_company and row_role:
            collapse_key = (row_company, row_role)
        else:
            collapse_key = (row_company or "__unknown__", row_role or "__unknown__")

        if collapse_key not in collapsed or row_last_updated >= collapsed[collapse_key]['date']:
            collapsed[collapse_key] = {'row': row, 'date': row_last_updated}

    final_rows = [v['row'] for v in collapsed.values()]
    final_rows.sort(key=lambda r: ((r[0] or '').lower(), (r[2] or '').lower()))

    values = [SHEET_HEADERS] + final_rows
    sheets_service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}!A1",
        valueInputOption='USER_ENTERED',
        body={'values': values}
    ).execute()
    print(f"Successfully processed {len(extracted_data)} applications; sheet now has {len(final_rows)} unique rows.")

def main():

    all_raw_emails = fetch_emails()    

    # 2. Process emails in chunks of EMAIL_CHUNK_SIZE
    print(f"Batch processing {len(all_raw_emails)} snippets with Gemini (chunks of {EMAIL_CHUNK_SIZE})...")
    extracted_data = []
    
    for i in range(0, len(all_raw_emails), EMAIL_CHUNK_SIZE):
        chunk = all_raw_emails[i:i + EMAIL_CHUNK_SIZE]
        print(f"Processing chunk {i // EMAIL_CHUNK_SIZE + 1}/{(len(all_raw_emails) + EMAIL_CHUNK_SIZE - 1) // EMAIL_CHUNK_SIZE} (emails {i + 1}-{min(i + EMAIL_CHUNK_SIZE, len(all_raw_emails))})...")
        chunk_data = ask_gemini_batched(chunk, start_index=i)
        extracted_data.extend(chunk_data)
    
    if not extracted_data:
        print("No job data extracted.")
        return

    # 3. UPSERT: delegate to functions for clarity
    update_sheets(extracted_data, all_raw_emails)
    

if __name__ == "__main__":
    main()
