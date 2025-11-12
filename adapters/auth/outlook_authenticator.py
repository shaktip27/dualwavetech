import os
import msal
from adapters.utils.config import config
from adapters.utils.logger import get_logger

logger = get_logger("OutlookAuthenticator")

class OutlookAuthenticator:
    """
    Handles Outlook / Microsoft Graph authentication using client credentials.
    Supports token caching and automatic refresh.
    """

    def __init__(self, client_id: str = None, client_secret: str = None, tenant_id: str = None):
        # Use config if values not provided
        self.client_id = client_id or config["outlook"].get("client_id")
        self.client_secret = client_secret or config["outlook"].get("client_secret")
        self.tenant_id = tenant_id or config["outlook"].get("tenant_id")

        if not all([self.client_id, self.client_secret, self.tenant_id]):
            raise ValueError("Missing Outlook credentials in .env or config.yaml")

        self.authority = f"https://login.microsoftonline.com/{self.tenant_id}"
        self.scope = ["https://graph.microsoft.com/.default"]

        # MSAL Confidential Client
        self.app = msal.ConfidentialClientApplication(
            client_id=self.client_id,
            client_credential=self.client_secret,
            authority=self.authority
        )
        self.token_cache = None
        logger.info("OutlookAuthenticator initialized.")

    def get_access_token(self) -> str:
        """
        Returns a valid access token. Refreshes if expired or not present.
        """
        result = self.app.acquire_token_silent(self.scope, account=None)
        if not result:
            logger.debug("No cached token, acquiring new token from Microsoft Graph...")
            result = self.app.acquire_token_for_client(scopes=self.scope)
            if "access_token" not in result:
                raise RuntimeError(f"Failed to acquire token: {result}")
        return result["access_token"]

    def get_headers(self):
        """
        Returns standard Graph API headers with Authorization Bearer token.
        """
        return {"Authorization": f"Bearer {self.get_access_token()}"}
