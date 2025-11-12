import json
import os
import pickle
import requests
from requests_oauthlib import OAuth2Session
from dotenv import load_dotenv

# Load environment variables (e.g., CLIENT_ID, CLIENT_SECRET)
load_dotenv()

# --- Configuration for Microsoft/Outlook OAuth ---
# NOTE: AUTHORITY will be built using the tenant_id from the credentials file.
DEFAULT_AUTHORITY = 'https://login.microsoftonline.com/common'
TOKEN_URL_TEMPLATE = '{authority}/oauth2/v2.0/token'
AUTHORIZE_URL_TEMPLATE = '{authority}/oauth2/v2.0/authorize'

# SCOPES needed for IMAP access
# IMAP.AccessAsUser.All is the scope required to read mail via IMAP
SCOPES = ['https://outlook.office.com/IMAP.AccessAsUser.All', 'offline_access']

# File paths for saving tokens and credentials
TOKEN_FILE = 'outlook_token.pickle'
CREDENTIALS_FILE = '/Users/mind/dual_wave_tech/outlook_credentials.json'

def get_outlook_credentials():
    """
    Handles the full OAuth 2.0 authorization flow for Outlook/Office 365.
    If a valid refresh token exists, it gets a new access token.
    Otherwise, it initiates a browser-based interactive flow to get a new refresh token.

    Returns:
        tuple[str, str] | None: A tuple (username, access_token) or None on failure.
    """

    # 1. Load credentials from file
    if not os.path.exists(CREDENTIALS_FILE):
        print(f"FATAL: Missing {CREDENTIALS_FILE}. Please create this file with your Azure App details.")
        print("Required keys: 'client_id', 'client_secret', 'redirect_uri', 'username'")
        return None

    with open(CREDENTIALS_FILE, 'r') as f:
        config = json.load(f)

    client_id = config.get('client_id')
    client_secret = config.get('client_secret')
    redirect_uri = config.get('redirect_uri')
    username = config.get('username')  # The email address you are authenticating
    tenant_id = config.get('tenant_id')  # Optionally use the specific tenant ID

    if not all([client_id, client_secret, redirect_uri, username]):
        print(
            f"FATAL: {CREDENTIALS_FILE} is incomplete. Check for client_id, client_secret, redirect_uri, and username.")
        return None

    # Determine the Authority URL based on if a tenant ID was provided
    authority_url = f'https://login.microsoftonline.com/{tenant_id}' if tenant_id else DEFAULT_AUTHORITY
    token_url = TOKEN_URL_TEMPLATE.format(authority=authority_url)
    authorize_url = AUTHORIZE_URL_TEMPLATE.format(authority=authority_url)

    # --- 2. Load or initiate the OAuth flow ---
    token = None
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, 'rb') as f:
            token = pickle.load(f)

    # Create an OAuth2Session instance
    client = OAuth2Session(client_id, scope=SCOPES, redirect_uri=redirect_uri)

    if token and 'refresh_token' in token:
        # 2a. Token refresh flow (non-interactive)
        try:
            # We use requests.post directly for token refresh because OAuth2Session's refresh method
            # sometimes expects specific parameters that Microsoft's endpoint is particular about.
            response = requests.post(
                token_url,
                data={
                    'client_id': client_id,
                    'scope': " ".join(SCOPES),
                    'refresh_token': token['refresh_token'],
                    'grant_type': 'refresh_token',
                    'client_secret': client_secret  # Required for server-side refresh
                }
            )
            response.raise_for_status()  # Raise exception for bad status codes
            new_token = response.json()

            # Update and save the new token, preserving the old refresh token if new one is missing
            token['access_token'] = new_token.get('access_token')
            token['refresh_token'] = new_token.get('refresh_token', token['refresh_token'])

            with open(TOKEN_FILE, 'wb') as f:
                pickle.dump(token, f)

            print(" Successfully refreshed Access Token via OAuth.")
            return username, token['access_token']

        except Exception as e:
            print(f"⚠️ Failed to refresh token: {e}. Initiating interactive sign-in.")
            # Fall through to interactive login
            token = None
            os.remove(TOKEN_FILE) if os.path.exists(TOKEN_FILE) else None

    if not token:
        # 2b. Interactive sign-in flow (first time setup)
        try:
            authorization_url, state = client.authorization_url(authorize_url)

            print("\n-------------------------------------------------------------")
            print("🛑 INTERACTIVE SIGN-IN REQUIRED 🛑")
            print(f"1. Open the following URL in your web browser:\n{authorization_url}")
            print("\n2. After signing in and granting permissions, the browser will redirect.")
            print("   Copy the full redirect URL from the address bar and paste it below.")
            print("-------------------------------------------------------------")

            redirect_response = input('Paste the full redirect URL here: ')

            # Fetch the final token, including the refresh token
            token = client.fetch_token(
                token_url,
                authorization_response=redirect_response,
                client_secret=client_secret
            )

            with open(TOKEN_FILE, 'wb') as f:
                pickle.dump(token, f)

            print("✅ Successfully obtained and saved Access & Refresh Tokens.")
            return username, token['access_token']

        except Exception as e:
            print(f"❌ Failed Interactive OAuth Flow: {e}")
            return None