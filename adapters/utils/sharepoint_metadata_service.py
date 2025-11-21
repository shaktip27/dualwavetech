import requests
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class SharePointMetadataService:
    def __init__(self, site_id, list_id, sharepoint_adapter):
        """
        Initialize metadata service with reference to SharePointAdapter.

        Args:
            site_id: SharePoint site ID
            list_id: SharePoint list/library ID
            sharepoint_adapter: Reference to SharePointAdapter instance (for token refresh)
        """
        self.site_id = site_id
        self.list_id = list_id
        self.sharepoint_adapter = sharepoint_adapter  # Store adapter reference instead of token

        # Create columns once when class loads
        self.create_sharepoint_columns()

    def _get_access_token(self):
        """Get current access token from SharePointAdapter (always fresh)"""
        return self.sharepoint_adapter.get_access_token()

    def _make_request(self, method, url, **kwargs):
        """
        Make HTTP request using SharePointAdapter's token refresh mechanism.

        Args:
            method: HTTP method ('GET', 'POST', 'PATCH', etc.)
            url: Request URL
            **kwargs: Additional arguments for requests

        Returns:
            requests.Response object
        """
        access_token = self._get_access_token()

        if 'headers' not in kwargs:
            kwargs['headers'] = {}
        kwargs['headers']['Authorization'] = f"Bearer {access_token}"

        if 'timeout' not in kwargs:
            kwargs['timeout'] = 10

        response = requests.request(method, url, **kwargs)

        if response.status_code == 401:
            logger.warning(" Metadata service got 401. Force refreshing token and retrying...")

            self.sharepoint_adapter._refresh_token()

            access_token = self._get_access_token()
            kwargs['headers']['Authorization'] = f"Bearer {access_token}"
            response = requests.request(method, url, **kwargs)

            if response.status_code == 401:
                logger.error("Still got 401 after token refresh in metadata service.")

        return response

    def create_sharepoint_columns(self):
        """Create columns if not exists (runs only once)"""
        url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/lists/{self.list_id}/columns"

        columns = [
            {"name": "AttachmentHash", "text": {}},
            {"name": "SourceEmailId", "text": {}},
            {"name": "SourceSender", "text": {}},
            {"name": "ProcessingStatus", "text": {}},
            {"name": "HeronID", "text": {}},
            {"name": "CompanyName", "text": {}},
            {"name": "ParsedAt", "dateTime": {}},
            {"name": "SharePointURL", "text": {}},
            {"name": "EndUserId", "text": {}}
        ]

        logger.info("Initializing SharePoint columns...")

        for col in columns:
            col_name = col["name"]
            try:
                res = self._make_request('POST', url, json=col)

                if res.status_code == 409:
                    logger.info(f"Column '{col_name}' already exists. Skipping...")
                elif res.status_code in (200, 201):
                    logger.info(f"Column '{col_name}' created successfully!")
                else:
                    logger.error(f"Failed to create '{col_name}' → {res.status_code} | {res.text}")
            except Exception as e:
                logger.error(f"Exception while creating column '{col_name}': {e}")

    def update_sharepoint_metadata_graph(
            self,
            drive_id: str,
            item_id: str,
            attachment_hash: str,
            source_email_id: str,
            source_sender: str,
            processing_status: str,
            heron_pdf_id: str = "",
            company_name: str = "",
            sharepoint_url: str = "",
            end_user_id: str = ""
    ) -> bool:
        """
        Update metadata after Heron parsed PDF.
        Uses fresh token from SharePointAdapter.
        """
        url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/listItem/fields"

        metadata = {
            "AttachmentHash": attachment_hash,
            "SourceEmailId": source_email_id,
            "SourceSender": source_sender,
            "ProcessingStatus": processing_status,
            "HeronID": heron_pdf_id,
            "CompanyName": company_name,
            "ParsedAt": datetime.now().isoformat(),
            "SharePointURL": sharepoint_url,
            "EndUserId": end_user_id
        }

        try:
            r = self._make_request('PATCH', url, json=metadata)

            if r.status_code in (200, 201):
                logger.info("SharePoint metadata updated successfully")
                return True
            else:
                logger.error(f"Metadata update failed: {r.status_code} → {r.text}")
                return False

        except Exception as e:
            logger.error(f"Metadata update exception: {e}")
            return False