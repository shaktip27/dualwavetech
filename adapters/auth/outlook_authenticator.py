import os
import msal
from adapters.utils.config import config
from adapters.utils.logger import get_logger
from datetime import datetime, timedelta

logger = get_logger("OutlookAuthenticator")


class OutlookAuthenticator:
    """
    Handles Outlook / Microsoft Graph authentication using client credentials.
    Supports token caching and automatic refresh with expiration handling.
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

        # Token cache
        self.cached_token = None
        self.token_expiry = None

        logger.info("OutlookAuthenticator initialized with auto-refresh capability.")

    def _is_token_expired(self) -> bool:
        """
        Check if the cached token is expired or about to expire (within 5 minutes).

        Returns:
            bool: True if token is expired or about to expire, False otherwise
        """
        if not self.token_expiry:
            return True

        # Consider token expired if it expires within 5 minutes
        buffer_time = timedelta(minutes=5)
        return datetime.now() >= (self.token_expiry - buffer_time)

    def get_access_token(self) -> str:
        """
        Returns a valid access token. Automatically refreshes if expired or not present.

        Returns:
            str: Valid access token

        Raises:
            RuntimeError: If token acquisition fails
        """
        # Check if we have a valid cached token
        if self.cached_token and not self._is_token_expired():
            logger.debug("Using cached token (still valid)")
            return self.cached_token

        # Token is expired or not present, acquire new one
        logger.info("🔄 Token expired or not present. Acquiring new token...")

        try:
            # Try to get token silently first
            result = self.app.acquire_token_silent(self.scope, account=None)

            if not result:
                logger.info("No cached token found. Acquiring new token from Microsoft Graph...")
                result = self.app.acquire_token_for_client(scopes=self.scope)

            if "access_token" not in result:
                error_msg = result.get("error_description", result.get("error", "Unknown error"))
                logger.error(f"❌ Failed to acquire token: {error_msg}")
                raise RuntimeError(f"Failed to acquire token: {error_msg}")

            # Cache the new token
            self.cached_token = result["access_token"]

            # Calculate expiry time (tokens typically expire in 3600 seconds/1 hour)
            expires_in = result.get("expires_in", 3600)  # Default to 1 hour
            self.token_expiry = datetime.now() + timedelta(seconds=expires_in)

            logger.info(
                f"✅ New token acquired successfully. Expires at: {self.token_expiry.strftime('%Y-%m-%d %H:%M:%S')}")

            return self.cached_token

        except Exception as e:
            logger.error(f"❌ Exception during token acquisition: {e}")
            # Clear cached token on error
            self.cached_token = None
            self.token_expiry = None
            raise RuntimeError(f"Failed to acquire access token: {e}")

    def get_headers(self):
        """
        Returns standard Graph API headers with Authorization Bearer token.
        Automatically refreshes token if expired.

        Returns:
            dict: Headers with valid Bearer token
        """
        try:
            token = self.get_access_token()
            return {"Authorization": f"Bearer {token}"}
        except Exception as e:
            logger.error(f"❌ Failed to get headers: {e}")
            raise

    def force_refresh_token(self):
        """
        Force refresh the access token (useful for testing or error recovery).
        """
        logger.info("🔄 Forcing token refresh...")
        self.cached_token = None
        self.token_expiry = None
        return self.get_access_token()