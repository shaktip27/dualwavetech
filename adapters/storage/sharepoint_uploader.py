import os
import requests
from datetime import datetime
from adapters.utils.logger import get_logger

logger = get_logger(name="sharepoint_adapter")


class SharePointAdapter:
    """
        - Folder creation
        - File uploads (attachments + PDF copies)
        - Share links
        - Metadata logging
    """

    def __init__(self, client_id, client_secret, tenant_id, site_name):
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.site_name = site_name

        self.base_graph_url = "https://graph.microsoft.com/v1.0"
        self.access_token = self.get_access_token()
        self.site_id = self._get_site_id()
        self.drive_id = self.get_drive_id()

        logger.info("SharePoint Adapter initialized successfully.")

    def get_access_token(self):
        """Get OAuth2 access token for Microsoft Graph"""
        try:
            url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
            payload = {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "scope": "https://graph.microsoft.com/.default"
            }
            res = requests.post(url, data=payload)
            res.raise_for_status()
            return res.json()["access_token"]
        except Exception as e:
            logger.critical(f"Failed to get access token: {e}")
            raise

    def _get_site_id(self):
        """Retrieve SharePoint site ID"""
        try:
            hostname = "atiumcapital.sharepoint.com"
            url = f"{self.base_graph_url}/sites/{hostname}:/sites/{self.site_name}"
            headers = {"Authorization": f"Bearer {self.access_token}"}

            res = requests.get(url, headers=headers)
            res.raise_for_status()

            site_id = res.json()["id"]
            logger.info(f"Site ID fetched: {site_id}")
            return site_id

        except Exception as e:
            logger.critical(f"❌ Failed to get Site ID: {e}")
            raise

    def get_drive_id(self):
        """Retrieve default document library drive ID"""
        try:
            url = f"{self.base_graph_url}/sites/{self.site_id}/drives"
            headers = {"Authorization": f"Bearer {self.access_token}"}
            res = requests.get(url, headers=headers)
            res.raise_for_status()
            return res.json()["value"][0]["id"]  # default document library
        except Exception as e:
            logger.critical(f"Failed to get Drive ID: {e}")
            raise

    def _ensure_folder_exists(self, folder_path: str):
        """
        Creates nested folder structure if not exists
        Example folder_path: "2025-10-22-CompanyName"
        Returns final folder item ID
        """
        parent_id = "root"
        headers = {"Authorization": f"Bearer {self.access_token}"}

        try:
            for folder in folder_path.strip("/").split("/"):
                url = f"{self.base_graph_url}/drives/{self.drive_id}/items/{parent_id}/children"
                res = requests.get(url, headers=headers)
                res.raise_for_status()

                existing = [
                    item for item in res.json().get("value", [])
                    if item["name"] == folder and "folder" in item
                ]

                if existing:
                    parent_id = existing[0]["id"]
                else:
                    payload = {"name": folder, "folder": {}, "@microsoft.graph.conflictBehavior": "rename"}
                    create = requests.post(url, headers=headers, json=payload)
                    create.raise_for_status()
                    parent_id = create.json()["id"]
                    logger.info(f"Created SharePoint folder: {folder}")

            return parent_id
        except Exception as e:
            logger.error(f"Folder creation failed '{folder_path}': {e}")
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
            logger.info(f" Checking if folder exists: {folder_path}")

            parent_id = "root"
            headers = {"Authorization": f"Bearer {self.access_token}"}

            # Split the path and check each folder level
            folders = folder_path.strip("/").split("/")

            for i, folder_name in enumerate(folders):
                url = f"{self.base_graph_url}/drives/{self.drive_id}/items/{parent_id}/children"

                try:
                    res = requests.get(url, headers=headers, timeout=10)
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
            logger.info(f"Folder EXISTS: {folder_path}")
            return True

        except Exception as e:
            logger.error(f"Unexpected error checking folder {folder_path}: {e}", exc_info=True)
            return False

    def upload_file(self, file_path, folder_path=None):
        """
        Upload file to SharePoint + auto create folders
        Returns: file id, webUrl, downloadUrl
        """
        try:
            file_name = os.path.basename(file_path)
            parent_id = self._ensure_folder_exists(folder_path) if folder_path else "root"

            url = f"{self.base_graph_url}/drives/{self.drive_id}/items/{parent_id}:/{file_name}:/content"
            headers = {
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/octet-stream"
            }

            with open(file_path, "rb") as f:
                res = requests.put(url, headers=headers, data=f)

            res.raise_for_status()
            data = res.json()

            logger.info(f"Uploaded: {file_name} → {data.get('webUrl')}")

            return {
                "drive_id": self.drive_id,
                "access_token": self.access_token,
                "id": data.get("id"),
                "webUrl": data.get("webUrl"),
                "downloadUrl": data.get("@microsoft.graph.downloadUrl")
            }

        except Exception as e:
            logger.error(f"File upload failed: {e}")
            raise

    def create_share_link(self, item_id, link_type="view"):
        """
        Generate anonymous share link
        :param link_type: "view" | "edit"
        """
        try:
            url = f"{self.base_graph_url}/drives/{self.drive_id}/items/{item_id}/createLink"
            headers = {"Authorization": f"Bearer {self.access_token}", "Content-Type": "application/json"}
            payload = {"type": link_type, "scope": "anonymous"}

            res = requests.post(url, headers=headers, json=payload)
            res.raise_for_status()

            logger.info(f"Created share link for item {item_id}")
            return res.json()["link"]["webUrl"]
        except Exception as e:
            logger.error(f"Failed to create share link: {e}")
            raise

    @staticmethod
    def get_today_folder_prefix(company_name):
        """Helper to generate YYYY-MM-DD-CompanyName folder format"""
        today = datetime.utcnow().strftime("%Y-%m-%d")
        return f"{today}-{company_name}"