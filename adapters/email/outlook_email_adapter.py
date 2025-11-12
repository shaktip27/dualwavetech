import os
import requests
import base64
from adapters.utils.logger import get_logger

logger = get_logger("outlook_email_adapter")

class OutlookConnectionError(Exception):
    """Custom exception for Outlook failures"""
    pass

class OutlookEmailAdapter:
    def __init__(self, authenticator, download_dir="downloads", filters=None):
        self.auth = authenticator
        self.download_dir = download_dir
        self.filters = filters or {}
        os.makedirs(self.download_dir, exist_ok=True)

        self.username = os.getenv("OUTLOOK_EMAIL")
        self.headers = None
        self.GRAPH_ENDPOINT = "https://graph.microsoft.com/v1.0"

    def connect(self):
        logger.info("Authenticating to Outlook using OAuth2...")
        self.headers = self.auth.get_headers()
        if not self.headers:
            raise OutlookConnectionError("Failed to authenticate Outlook user")
        logger.info(f"Authenticated Outlook account: {self.username}")

    def fetch_emails(self):
        logger.info("Fetching unread Outlook emails...")
        url = f"{self.GRAPH_ENDPOINT}/users/{self.username}/messages?$filter=isRead eq false&$orderby=receivedDateTime desc&$top=50"

        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to fetch emails: {e}")
            raise OutlookConnectionError(e)

        emails = response.json().get("value", [])
        logger.info(f"Unread emails found: {len(emails)}")

        for mail in emails:
            message_id = mail.get("id")
            sender = mail.get("from", {}).get("emailAddress", {}).get("address", "")
            subject = mail.get("subject", "")
            # Add date
            date = mail.get("receivedDateTime", "")
            # Add body (HTML)
            body = mail.get("body", {}).get("content", "")
            # Add recipients
            to_recipients = [r.get("emailAddress", {}).get("address", "") for r in mail.get("toRecipients", [])]

            logger.info(f"Processing | From: {sender} | Subject: {subject}")

            if not self.is_relevant(sender, subject):
                logger.info("Skipped: Not matched filter")
                continue

            attachments = self.download_attachments(message_id)
            self.mark_as_read(message_id)

            yield {
                "id": message_id,
                "sender": sender,
                "subject": subject,
                "date": date,
                "body": body,
                "to": to_recipients,
                "attachments": attachments
            }

    def download_attachments(self, message_id):
        url = f"{self.GRAPH_ENDPOINT}/users/{self.username}/messages/{message_id}/attachments"
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
        except Exception as e:
            logger.error(f"Attachment API error: {e}")
            return []

        files = []
        for item in response.json().get("value", []):
            if item.get("@odata.type") != "#microsoft.graph.fileAttachment":
                continue

            filename = item.get("name")
            file_bytes = item.get("contentBytes")

            save_path = os.path.join(self.download_dir, filename)
            try:
                with open(save_path, "wb") as f:
                    f.write(base64.b64decode(file_bytes))
                files.append(save_path)
                logger.info(f"Attachment saved: {filename}")
            except Exception as e:
                logger.error(f"Failed saving {filename}: {e}")

        return files

    def mark_as_read(self, message_id):
        url = f"{self.GRAPH_ENDPOINT}/users/{self.username}/messages/{message_id}"
        try:
            requests.patch(url, json={"isRead": True}, headers=self.headers)
            logger.info("Marked as read")
        except Exception as e:
            logger.error(f"Mark as read failed: {e}")

    def is_relevant(self, sender, subject):
        if self.filters.get("subject_keywords"):
            if any(k.lower() in subject.lower() for k in self.filters["subject_keywords"]):
                return True

        if self.filters.get("sender_domains"):
            if any(sender.lower().endswith(d.lower()) for d in self.filters["sender_domains"]):
                return True

        return False
