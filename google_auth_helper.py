# from google_auth_oauthlib.flow import InstalledAppFlow
# from googleapiclient.discovery import build
# import os, pickle
#
#
# SCOPES = [
#     'https://www.googleapis.com/auth/drive.file',
#     'https://www.googleapis.com/auth/gmail.readonly'
# ]
#
# flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
# creds = flow.run_local_server(port=0)
#
# with open('token.pickle', 'wb') as token:
#     pickle.dump(creds, token)
#
# print("✅ Token saved to token.pickle")

import os
import pickle
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# Define the scopes your app will need
SCOPES = [
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/gmail.readonly',
]


def get_credentials():
    """
    Loads or refreshes Google OAuth credentials.
    If no token exists, creates a new one using credentials.json.
    """

    creds = None

    # Load existing token if available
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)

    # If there are no valid credentials, request new login
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)

        # Save updated token
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    return creds