import os
from googleapiclient.http import MediaFileUpload
from google_auth_helper import get_credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from adapters.utils.logger import get_logger

logger = get_logger(name="google_drive_uploader.py")


class GoogleDriveUploader:
    """
    ---->  Adapter class for handling Google Drive file uploads and path creation.
    """

    def __init__(self):
        self._creds = get_credentials()
        self._service = self._get_drive_service()
        logger.info("Google Drive Service initialized.")

    def _get_drive_service(self):
        """Builds the Google Drive V3 service."""
        return build("drive", "v3", credentials=self._creds)

    def _ensure_drive_path_exists(self, folder_path):
        """
        Creates nested folder structure in Google Drive if missing.
        Returns the ID of the final target folder.
        """

        if not folder_path:
            return None

        parent_id = None
        # Handle the root folder structure part by part
        for part in folder_path.split("/"):
            query = f"name='{part}' and mimeType='application/vnd.google-apps.folder'"
            if parent_id:
                query += f" and '{parent_id}' in parents"

            try:
                results = self._service.files().list(q=query, fields="files(id, name)").execute()
                folders = results.get("files", [])

                if folders:
                    # Folder part exists, continue with its ID
                    parent_id = folders[0]["id"]
                else:
                    # Folder part does not exist, create it
                    metadata = {"name": part, "mimeType": "application/vnd.google-apps.folder"}
                    if parent_id:
                        metadata["parents"] = [parent_id]

                    folder = self._service.files().create(body=metadata, fields="id").execute()
                    parent_id = folder.get("id")
                    logger.info(f"Created new Drive folder: {part}")

            except HttpError as e:
                logger.error(f"Drive API error while creating path '{folder_path}': {e}")
                raise

        return parent_id

    def upload_to_drive(self, file_path, folder_path=None):
        """
        Uploads a file and ensures the nested folder structure exists.

        Returns: A dictionary with the file ID and web link.
        """
        uploaded_file = None
        try:
            # Ensure folder structure is ready
            folder_id = self._ensure_drive_path_exists(folder_path)

            # Prepare upload metadata and media
            file_name = os.path.basename(file_path)
            file_metadata = {"name": file_name}

            if folder_id:
                file_metadata["parents"] = [folder_id]

            media = MediaFileUpload(file_path, resumable=True)

            # Execute upload
            uploaded_file = self._service.files().create(
                body=file_metadata,
                media_body=media,
                fields="id, name, webViewLink"
            ).execute()

            logger.info(f"Uploaded successfully: {uploaded_file['name']} → {uploaded_file['webViewLink']}")

            # Return essential information for the main service to use for metadata updates
            return {
                "id": uploaded_file.get("id"),
                "webViewLink": uploaded_file.get("webViewLink")
            }

        except HttpError as e:
            logger.error(f"Drive API upload failed for file '{file_path}': {e}")
            raise
        except Exception as e:
            logger.critical(f"An unexpected error occurred during Drive upload: {e}")
            raise