import requests
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class SharePointMetadataService:
    def __init__(self, site_id, list_id, access_token):
        self.site_id = site_id
        self.list_id = list_id
        self.access_token = access_token

        #Call once when class loads
        self.create_sharepoint_columns()

    #Create columns if not exists (runs only once)
    def create_sharepoint_columns(self):
        url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/lists/{self.list_id}/columns"

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }

        columns = [
            {"name": "AttachmentHash", "text": {}},
            {"name": "SourceEmailId", "text": {}},
            {"name": "SourceSender", "text": {}},
            {"name": "ProcessingStatus", "text": {}},# Duplicate / Success / UploadFailed / NotBankStatement / Parsed / ParseFailed
            {"name": "HeronID", "text": {}},
            {"name": "CompanyName", "text": {}},
            {"name": "ParsedAt", "dateTime": {}},
            {"name": "SharePointURL", "text": {}}
        ]

        logger.info("Initializing SharePoint columns...")

        for col in columns:
            col_name = col["name"]
            try:
                res = requests.post(url, json=col, headers=headers)
                if res.status_code == 409:
                    logger.info(f"Column '{col_name}' already exists. Skipping...")
                elif res.status_code in (200, 201):
                    logger.info(f"Column '{col_name}' created successfully!")
                else:
                    logger.error(f"Failed to create '{col_name}' → {res.status_code} | {res.text}")
            except Exception as e:
                logger.error(f"Exception while creating column '{col_name}': {e}")


    # Update metadata after Heron parsed PDF
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
            sharepoint_url: str = ""
    ) -> bool:
        url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/listItem/fields"

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }

        metadata = {
            "AttachmentHash": attachment_hash,
            "SourceEmailId": source_email_id,
            "SourceSender": source_sender,
            "ProcessingStatus": processing_status,
            "HeronID": heron_pdf_id,
            "CompanyName": company_name,
            "ParsedAt": datetime.now().isoformat(),
            "SharePointURL": sharepoint_url
        }
        try:
            r = requests.patch(url, json=metadata, headers=headers)
            if r.status_code in (200, 201):
                logger.info("SharePoint metadata updated successfully")
                return True
            else:
                logger.error(f"Metadata update failed: {r.status_code} → {r.text}")
                return False

        except Exception as e:
            logger.error(f"Metadata update exception: {e}")
            return False
