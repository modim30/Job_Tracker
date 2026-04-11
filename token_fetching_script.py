# lets make a script that will log in once for both my gmail accounts. this creates the tokens. i will save these tokesn in a file called tokens.json. this way i can use the tokens to access the gmail api without having to log in every time.
import os
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly', 'https://www.googleapis.com/auth/spreadsheets']

def get_tokens():
    creds = None
    if os.path.exists('tokens.json'):
        creds = Credentials.from_authorized_user_file('tokens.json', SCOPES)
    
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0, access_type='offline', prompt='consent')
        
        with open('tokens.json', 'w') as token:
            token.write(creds.to_json())

if __name__ == '__main__':
    get_tokens()