import os
import requests
from datetime import datetime, timedelta
from adapters.utils.logger import get_logger

logger = get_logger(name="sharepoint_adapter")


class SharePointAdapter:
    """
    SharePoint adapter with automatic token refresh capability.
    Features:
        - Folder creation
        - File uploads (attachments + PDF copies)
        - Share links
        - Metadata logging
        - Automatic token refresh when expired
    """

    def __init__(self, client_id, client_secret, tenant_id, site_name):
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.site_name = site_name

        self.base_graph_url = "https://graph.microsoft.com/v1.0"
        self.access_token = None
        self.token_expiry = None

        # Initialize with fresh token
        self._refresh_token()

        self.site_id = self._get_site_id()
        self.drive_id = self.get_drive_id()

        logger.info("SharePoint Adapter initialized successfully.")

    def _refresh_token(self):
        """
        Refresh the OAuth2 access token for Microsoft Graph.
        Sets token expiry time with 5-minute safety margin.
        """
        try:
            url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
            payload = {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "scope": "https://graph.microsoft.com/.default"
            }
            res = requests.post(url, data=payload, timeout=10)
            res.raise_for_status()

            token_data = res.json()
            self.access_token = token_data["access_token"]

            # Set expiry time (default 3600 seconds, subtract 300 for safety margin)
            expires_in = token_data.get("expires_in", 3600)
            self.token_expiry = datetime.now() + timedelta(seconds=expires_in - 300)

            logger.info(
                f" Token refreshed successfully. Expires at: {self.token_expiry.strftime('%Y-%m-%d %H:%M:%S')}")

        except Exception as e:
            logger.critical(f" Failed to refresh access token: {e}")
            raise

    def _ensure_valid_token(self):
        """
        Check if current token is expired or about to expire.
        Automatically refreshes if needed.
        """
        if self.token_expiry is None or datetime.now() >= self.token_expiry:
            logger.warning("Token expired or about to expire. Refreshing now...")
            self._refresh_token()

    def _make_request(self, method, url, retry_on_401=True, **kwargs):
        """
        Make HTTP request with automatic token refresh on 401 errors.

        Args:
            method: HTTP method ('GET', 'POST', 'PUT', 'PATCH', 'DELETE')
            url: Request URL
            retry_on_401: Whether to retry once on 401 errors (default: True)
            **kwargs: Additional arguments for requests

        Returns:
            requests.Response object
        """
        # Ensure token is valid before making request
        self._ensure_valid_token()

        # Add authorization header
        if 'headers' not in kwargs:
            kwargs['headers'] = {}
        kwargs['headers']['Authorization'] = f"Bearer {self.access_token}"

        # Set default timeout if not provided
        if 'timeout' not in kwargs:
            kwargs['timeout'] = 30

        # Make the request
        response = requests.request(method, url, **kwargs)

        # Handle 401 Unauthorized - force refresh and retry once
        if response.status_code == 401 and retry_on_401:
            logger.warning(" Received 401 Unauthorized. Force refreshing token and retrying...")
            self._refresh_token()

            # Update header with new token
            kwargs['headers']['Authorization'] = f"Bearer {self.access_token}"

            # Retry request
            response = requests.request(method, url, **kwargs)

            if response.status_code == 401:
                logger.error(" Still got 401 after token refresh. Check credentials and permissions.")

        return response

    def get_access_token(self):
        """
        Get current OAuth2 access token for Microsoft Graph.
        This method is kept for backward compatibility but uses the refresh mechanism.
        """
        self._ensure_valid_token()
        return self.access_token

    def _get_site_id(self):
        """Retrieve SharePoint site ID"""
        try:
            hostname = "atiumcapital.sharepoint.com"
            url = f"{self.base_graph_url}/sites/{hostname}:/sites/{self.site_name}"

            res = self._make_request('GET', url)
            res.raise_for_status()

            site_id = res.json()["id"]
            logger.info(f"Site ID fetched: {site_id}")
            return site_id

        except Exception as e:
            logger.critical(f" Failed to get Site ID: {e}")
            raise

    def get_drive_id(self):
        """Retrieve default document library drive ID"""
        try:
            url = f"{self.base_graph_url}/sites/{self.site_id}/drives"

            res = self._make_request('GET', url)
            res.raise_for_status()

            drives = res.json().get("value", [])
            if not drives:
                raise ValueError("No drives found in SharePoint site")

            return drives[0]["id"]  # default document library

        except Exception as e:
            logger.critical(f" Failed to get Drive ID: {e}")
            raise

    def _ensure_folder_exists(self, folder_path: str):
        """
        Creates nested folder structure if not exists.
        Example folder_path: "2025-10-22-CompanyName/Dataroom"
        Returns final folder item ID
        """
        parent_id = "root"

        try:
            for folder in folder_path.strip("/").split("/"):
                url = f"{self.base_graph_url}/drives/{self.drive_id}/items/{parent_id}/children"

                res = self._make_request('GET', url)
                res.raise_for_status()

                existing = [
                    item for item in res.json().get("value", [])
                    if item["name"] == folder and "folder" in item
                ]

                if existing:
                    parent_id = existing[0]["id"]
                    logger.debug(f"Folder exists: {folder}")
                else:
                    payload = {
                        "name": folder,
                        "folder": {},
                        "@microsoft.graph.conflictBehavior": "rename"
                    }

                    create = self._make_request('POST', url, json=payload)
                    create.raise_for_status()

                    parent_id = create.json()["id"]
                    logger.info(f"✅ Created SharePoint folder: {folder}")

            return parent_id

        except Exception as e:
            logger.error(f" Folder creation failed '{folder_path}': {e}")
            raise

    def folder_exists(self, folder_path: str) -> bool:
        """
        Check if a folder exists in SharePoint by traversing the folder structure.
        This method matches the logic used in _ensure_folder_exists().

        Args:
            folder_path: Folder path relative to root (e.g., "2025.11.14-company_name")

        Returns:
            bool: True if folder exists, False otherwise
        """
        try:
            logger.info(f"🔍 Checking if folder exists: {folder_path}")

            parent_id = "root"

            # Split the path and check each folder level
            folders = folder_path.strip("/").split("/")

            for i, folder_name in enumerate(folders):
                url = f"{self.base_graph_url}/drives/{self.drive_id}/items/{parent_id}/children"

                try:
                    res = self._make_request('GET', url)
                    res.raise_for_status()

                    # Check if this folder exists at current level
                    existing = [
                        item for item in res.json().get("value", [])
                        if item["name"] == folder_name and "folder" in item
                    ]

                    if existing:
                        parent_id = existing[0]["id"]
                        logger.info(f"   ✓ Found folder level {i + 1}/{len(folders)}: {folder_name}")
                    else:
                        logger.info(f"   ✗ Folder does NOT exist: {folder_path} (missing at: {folder_name})")
                        return False

                except requests.RequestException as e:
                    logger.error(f"Error checking folder level '{folder_name}': {e}")
                    return False

            # If we made it through all levels, folder exists
            logger.info(f" Folder EXISTS: {folder_path}")
            return True

        except Exception as e:
            logger.error(f"Unexpected error checking folder {folder_path}: {e}", exc_info=True)
            return False

    def upload_file(self, file_path, folder_path=None):
        """
        Upload file to SharePoint with auto folder creation.

        Args:
            file_path: Local path to file to upload
            folder_path: SharePoint folder path (creates if doesn't exist)

        Returns:
            dict: Contains drive_id, access_token, id, webUrl, downloadUrl
        """
        try:
            file_name = os.path.basename(file_path)
            parent_id = self._ensure_folder_exists(folder_path) if folder_path else "root"

            url = f"{self.base_graph_url}/drives/{self.drive_id}/items/{parent_id}:/{file_name}:/content"

            with open(file_path, "rb") as f:
                file_data = f.read()

            # Make request with file data
            res = self._make_request(
                'PUT',
                url,
                data=file_data,
                headers={'Content-Type': 'application/octet-stream'}
            )

            res.raise_for_status()
            data = res.json()

            logger.info(f" Uploaded: {file_name} → {data.get('webUrl')}")

            return {
                "drive_id": self.drive_id,
                "access_token": self.access_token,
                "id": data.get("id"),
                "webUrl": data.get("webUrl"),
                "downloadUrl": data.get("@microsoft.graph.downloadUrl")
            }

        except Exception as e:
            logger.error(f" File upload failed: {e}")
            raise

    def create_share_link(self, item_id, link_type="view"):
        """
        Generate anonymous share link.

        Args:
            item_id: SharePoint item ID
            link_type: "view" or "edit"

        Returns:
            str: Share link URL
        """
        try:
            url = f"{self.base_graph_url}/drives/{self.drive_id}/items/{item_id}/createLink"
            payload = {
                "type": link_type,
                "scope": "anonymous"
            }

            res = self._make_request('POST', url, json=payload)
            res.raise_for_status()

            share_url = res.json()["link"]["webUrl"]
            logger.info(f" Created share link for item {item_id}")
            return share_url

        except Exception as e:
            logger.error(f" Failed to create share link: {e}")
            raise

    @staticmethod
    def get_today_folder_prefix(company_name):
        """Helper to generate YYYY-MM-DD-CompanyName folder format"""
        today = datetime.utcnow().strftime("%Y-%m-%d")
        return f"{today}-{company_name}"