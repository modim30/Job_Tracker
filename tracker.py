import os
import json
import re
import requests
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

# === Configuration ===
SPREADSHEET_ID = '1S3FRJTnXoEirGv6ohSNTo-bH6wnoIrdCYL5O5INb290'
SHEET_NAME = 'Sheet1'
GEMINI_API_KEY = ''
GEMINI_API_URL = 'https://generativelanguage.googleapis.com/v1beta/models/gemini-3.1-flash-lite-preview:generateContent'

SHEET_HEADERS = ["Company Name", "Application Status", "Role", "Salary", "Date Submitted", "Link to Job Req", "Rejection Reason"]

def ask_gemini_batched(email_list):
    if not email_list: return []
    
    # We join all emails into one big string with clear delimiters
    combined_emails = "\n---\n".join([f"EMAIL {i+1}:\n{text}" for i, text in enumerate(email_list)])
    
    prompt = f"""
    You are a career automation assistant. I will provide a list of email snippets delimited by '---'. 
    For each snippet that is a JOB APPLICATION update (confirmation, interview, or rejection), extract the details into a JSON array of objects.

    SCHEMA:
    - "Company Name": Full name of the company.
    - "Application Status": Strictly one of [Submitted, Interviewing, Rejected, Offer].
    - "Role": The job title.
    - "Salary": Any mentioned pay (otherwise "").
    - "Date Submitted": The date mentioned in the email (Format: MM/DD/YYYY).
    - "Link to Job Req": The direct URL to the job portal or description.
    - "Rejection Reason": Brief reason if rejected (otherwise "").

    If a snippet is NOT a job application email, skip it.
    Return ONLY the JSON array.

    EMAILS TO PROCESS:
    {combined_emails}
    """
    
    payload = { "contents": [{ "parts": [{ "text": prompt }] }] }
    response = requests.post(f"{GEMINI_API_URL}?key={GEMINI_API_KEY}", json=payload)
    print(f"Gemini API response status: {response.status_code}")
    print(f"Gemini API raw response: {response.text}...")  # Print first 500 chars for debugging
    if response.status_code == 200:
        try:
            raw_text = response.json()['candidates'][0]['content']['parts'][0]['text']
            # Remove code block markers if present
            text = raw_text.strip()
            if text.startswith('```json'):
                text = text[len('```json'):].strip()
            if text.endswith('```'):
                text = text[:-3].strip()
            # Try to extract JSON array
            data = json.loads(text)
            # Only keep rows that are dicts and not just the header list
            valid_rows = []
            for entry in data:
                if isinstance(entry, dict):
                    row = [
                        entry.get("Company Name", ""),
                        entry.get("Application Status", ""),
                        entry.get("Role", ""),
                        entry.get("Salary", ""),
                        entry.get("Date Submitted", ""),
                        entry.get("Link to Job Req", ""),
                        entry.get("Rejection Reason", "")
                    ]
                    # Skip header-only or empty rows
                    if row != SHEET_HEADERS and any(row):
                        valid_rows.append(row)
            return valid_rows
        except Exception as e:
            print(f"Extraction failed: {e}\nCleaned Gemini text: {text if 'text' in locals() else raw_text}")
    return []

def main():

    accounts = ['meetmodi400.json', 'modim417.json']
    all_raw_emails = []

    # 1. Gather all snippets first
    for acc in accounts:
        print(f"Fetching from {acc}...")
        creds = Credentials.from_authorized_user_file(acc)
        service = build('gmail', 'v1', credentials=creds)
        # Search for recent emails (last 30 days)
        query = 'newer_than:30d'
        results = service.users().messages().list(userId='me', q=query, maxResults=15).execute()
        
        for msg in results.get('messages', []):
            m = service.users().messages().get(userId='me', id=msg['id'], format='metadata').execute()
            all_raw_emails.append(m.get('snippet', ''))

    # 2. Process all at once
    print(f"Batch processing {len(all_raw_emails)} snippets with Gemini...")
    extracted_data = ask_gemini_batched(all_raw_emails)
    
    if not extracted_data:
        print("No job data extracted.")
        return

    # 3. UPSERT logic (same as before, but handles the list)
    # [Insert the Upsert logic from previous turns here]
    # --- UPSERT LOGIC ---
    write_creds = Credentials.from_authorized_user_file('modim417.json')
    service = build('sheets', 'v4', credentials=write_creds)

    # 1. Get existing data to find matches
    all_rows = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=f"{SHEET_NAME}!A:G"
    ).execute().get('values', [])

    # 2. Create a lookup map: (Company, Role) -> Row Number
    # We lowercase everything to avoid "Tesla" vs "tesla" issues
    lookup = {}
    for i, row in enumerate(all_rows):
        if len(row) >= 3:
            key = (row[0].lower().strip(), row[2].lower().strip())
            lookup[key] = i + 1 # Sheets is 1-indexed

    final_append = []
    for item in extracted_data:
        key = (item[0].lower().strip(), item[2].lower().strip())
        
        if key in lookup:
            # UPDATE existing row
            row_num = lookup[key]
            print(f"Updating existing application: {item[0]} - {item[2]} at row {row_num}")
            service.spreadsheets().values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{SHEET_NAME}!A{row_num}",
                valueInputOption='USER_ENTERED',
                body={'values': [item]}
            ).execute()
        else:
            # NEW entry
            final_append.append(item)

    if final_append:
        print(f"Adding {len(final_append)} new entries.")
        service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME}!A1",
            valueInputOption='USER_ENTERED',
            insertDataOption='INSERT_ROWS',
            body={'values': final_append}
        ).execute()
    print(f"Successfully processed {len(extracted_data)} applications.")

if __name__ == "__main__":
    main()
