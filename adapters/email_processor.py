import os
import time
from datetime import datetime
from adapters.utils.logger import get_logger
import hashlib
import json
import re
import traceback
from adapters.utils.heron_service import HeronService
from adapters.utils.sharepoint_metadata_service import SharePointMetadataService
from adapters.utils.pdf_generator import generate_email_pdf
from adapters.utils.zip_handler import ZipHandler

logger = get_logger("email_processor")

# -------------------- Ledger --------------------
LEDGER_FILE = os.path.join(os.getcwd(), "attachment_log.json")


def ensure_ledger():
    """Ensure the ledger file exists and return its content."""
    if not os.path.exists(LEDGER_FILE):
        with open(LEDGER_FILE, "w") as f:
            json.dump([], f, indent=4)
    with open(LEDGER_FILE, "r") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return []


def compute_sha256(file_path):
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def is_duplicate(file_hash):
    ledger = ensure_ledger()
    return any(entry["hash"] == file_hash for entry in ledger)


def get_unique_filename(original_filename: str) -> str:
    """
    Check ledger for duplicate filenames and return a unique filename.
    If filename exists, append (1), (2), etc.
    """
    ledger = ensure_ledger()
    existing_filenames = [entry.get("file_name", "") for entry in ledger]

    if original_filename not in existing_filenames:
        return original_filename

    name_parts = os.path.splitext(original_filename)
    base_name = name_parts[0]
    extension = name_parts[1]

    counter = 1
    while True:
        new_filename = f"{base_name}({counter}){extension}"
        if new_filename not in existing_filenames:
            return new_filename
        counter += 1


def log_attachment(message_id, file_name, file_hash, outcome, error=None):
    """Append attachment metadata to JSON ledger."""
    ledger = ensure_ledger()
    entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "message_id": message_id,
        "file_name": file_name,
        "hash": file_hash,
        "outcome": outcome,
        "error": error
    }
    ledger.append(entry)
    with open(LEDGER_FILE, "w") as f:
        json.dump(ledger, f, indent=4)


class EmailProcessor:
    """Handles the full lifecycle of an incoming email with new SharePoint folder structure."""

    def __init__(self, storage_adapter, detector, base_download_dir: str,
                 sp_metadata_service: SharePointMetadataService, heron_service: HeronService):
        self._storage_adapter = storage_adapter
        self._detector = detector
        self._base_download_dir = base_download_dir
        self._max_retries = 3
        self.sp_metadata_service = sp_metadata_service
        self.heron_service = heron_service
        self.zip_handler = ZipHandler()

    def _cleanup_local_file(self, file_path: str, description: str):
        """Safely remove a local file if it exists."""
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
                logger.info(f"Cleaned up {description}: {file_path}")
            except OSError as e:
                logger.error(f" Error deleting {description} {file_path}: {e}")

    def _cleanup_directory(self, dir_path: str, description: str):
        """Safely remove a directory and all its contents."""
        if os.path.exists(dir_path) and os.path.isdir(dir_path):
            try:
                import shutil
                shutil.rmtree(dir_path)
                logger.info(f"Cleaned up {description} directory: {dir_path}")
            except Exception as e:
                logger.error(f"Error deleting {description} directory {dir_path}: {e}")

    def _get_unique_folder_name(self, base_folder_name: str) -> str:
        """
        Check if SharePoint folder exists and return unique folder name.
        If folder exists, append (1), (2), etc.
        """
        try:
            if not self._storage_adapter.folder_exists(base_folder_name):
                logger.info(f"Folder '{base_folder_name}' does not exist. Using as is.")
                return base_folder_name

            logger.info(f"Folder '{base_folder_name}' already EXISTS! Finding next available number...")
            counter = 1
            while True:
                new_folder_name = f"{base_folder_name}({counter})"
                logger.info(f"   Checking: {new_folder_name}")

                if not self._storage_adapter.folder_exists(new_folder_name):
                    logger.info(f"Found unique folder name: {new_folder_name}")
                    return new_folder_name

                logger.info(f" {new_folder_name} also exists, trying next...")
                counter += 1

                if counter > 100:
                    logger.error(f"Too many duplicate folders for {base_folder_name}. Using timestamp fallback.")
                    timestamp = datetime.utcnow().strftime('%H%M%S')
                    fallback = f"{base_folder_name}_{timestamp}"
                    logger.info(f"Using fallback: {fallback}")
                    return fallback

        except Exception as e:
            logger.error(f"Error checking folder uniqueness: {e}. Using original name.", exc_info=True)
            return base_folder_name

    def _upload_with_retry(self, local_path: str, folder_name: str, file_name: str):
        """Upload a file to SharePoint with exponential backoff retry logic."""
        for attempt in range(1, self._max_retries + 1):
            logger.info(f"Uploading {file_name} to SharePoint (Attempt {attempt}/{self._max_retries})...")
            try:
                result = self._storage_adapter.upload_file(local_path, folder_path=folder_name)
                logger.info(f"Upload successful for {file_name} → {result.get('webUrl')}")
                return result
            except Exception as e:
                logger.warning(f"Upload FAILED for {file_name} on attempt {attempt}: {e}")
                if attempt < self._max_retries:
                    time.sleep(2 ** attempt)
                else:
                    logger.error(f"Upload permanently FAILED for {file_name} after {self._max_retries} attempts.")
                    return None
        return None

    def _upload_email_pdf(self, email_data: dict, base_folder: str, company_name: str):
        """
        Generate a PDF from email data and upload it to SharePoint in summary_mail folder.
        """
        pdf_path = None
        new_pdf_path = None

        try:
            os.makedirs(self._base_download_dir, exist_ok=True)

            # Generate PDF
            pdf_path = generate_email_pdf(email_data, self._base_download_dir)

            # Create timestamp for filename
            timestamp = datetime.utcnow().strftime('%Y.%m.%d-%H.%M.%S')
            new_pdf_name = f"{timestamp}-{company_name}.pdf"

            # Rename the PDF file locally
            new_pdf_path = os.path.join(self._base_download_dir, new_pdf_name)
            os.rename(pdf_path, new_pdf_path)

            # Upload to summary_mail folder
            summary_mail_folder = f"{base_folder}/summary_mail"

            upload_result = self._upload_with_retry(
                new_pdf_path,
                folder_name=summary_mail_folder,
                file_name=new_pdf_name
            )

            if upload_result:
                logger.info(f"Email PDF uploaded successfully to {summary_mail_folder}: {upload_result.get('webUrl')}")
            else:
                logger.warning(f"Failed to upload email PDF for subject: {email_data.get('subject')}")

        except Exception as e:
            logger.error(f"Failed to generate/upload email PDF: {e}")
        finally:
            # CLEANUP: Always remove local PDF files
            if new_pdf_path and os.path.exists(new_pdf_path):
                self._cleanup_local_file(new_pdf_path, "email PDF")
            elif pdf_path and os.path.exists(pdf_path):
                self._cleanup_local_file(pdf_path, "email PDF")

    def process_attachment(self, adapter_temp_path: str, base_folder: str, email_data: dict) -> None:
        """
        Processes a single attachment with new folder structure.
        All files go into base_folder/Dataroom/
        """
        file_name = os.path.basename(adapter_temp_path)
        logger.info(f"Starting processing for attachment: {file_name}")

        try:
            # Check if the attachment is a ZIP file
            if self.zip_handler.is_zip_file(adapter_temp_path):
                logger.info(f"Detected ZIP file: {file_name}. Extracting contents...")
                self._process_zip_attachment(adapter_temp_path, base_folder, email_data)
            else:
                # Process as regular file - goes into Dataroom
                dataroom_folder = f"{base_folder}/Dataroom"
                self._process_single_file(adapter_temp_path, dataroom_folder, email_data)
        finally:
            # CLEANUP: Always remove the original adapter temp file
            self._cleanup_local_file(adapter_temp_path, "adapter temporary attachment")

    def _process_zip_attachment(self, zip_path: str, base_folder: str, email_data: dict):
        """
        Extract ZIP file and process each file inside.
        All extracted files go into base_folder/Dataroom/
        """
        zip_file_name = os.path.basename(zip_path)
        extract_dir = os.path.join(self._base_download_dir, f"extracted_{int(time.time())}")

        try:
            # Extract ZIP contents
            extracted_files = self.zip_handler.extract_zip(zip_path, extract_dir)

            if not extracted_files:
                logger.warning(f"No files extracted from ZIP: {zip_file_name}")
                log_attachment(
                    email_data.get('id', 'unknown'),
                    zip_file_name,
                    compute_sha256(zip_path),
                    outcome="EmptyZIP"
                )
                return

            # Filter to supported file types
            supported_files = self.zip_handler.get_supported_files(extracted_files)

            if not supported_files:
                logger.warning(f"No supported files found in ZIP: {zip_file_name}")
                log_attachment(
                    email_data.get('id', 'unknown'),
                    zip_file_name,
                    compute_sha256(zip_path),
                    outcome="NoSupportedFiles"
                )
                return

            logger.info(f"Processing {len(supported_files)} files from ZIP: {zip_file_name}")

            # All extracted files go into Dataroom
            dataroom_folder = f"{base_folder}/Dataroom"

            # Process each extracted file
            for extracted_file in supported_files:
                try:
                    logger.info(f"Processing extracted file: {os.path.basename(extracted_file)}")
                    self._process_single_file(extracted_file, dataroom_folder, email_data, is_from_zip=True)
                except Exception as e:
                    logger.error(f"Error processing file from ZIP {extracted_file}: {e}")

            # Log the ZIP file itself
            log_attachment(
                email_data.get('id', 'unknown'),
                zip_file_name,
                compute_sha256(zip_path),
                outcome="ZIPProcessed",
                error=f"Extracted {len(supported_files)} files"
            )

        except Exception as e:
            logger.error(f"Error processing ZIP file {zip_file_name}: {e}")
            log_attachment(
                email_data.get('id', 'unknown'),
                zip_file_name,
                'N/A',
                outcome="ZIPError",
                error=str(e)
            )
        finally:
            # CLEANUP: Always remove extracted directory and ZIP file
            self._cleanup_directory(extract_dir, "ZIP extraction")
            self._cleanup_local_file(zip_path, "ZIP file")

    def _process_single_file(self, file_path: str, folder_name: str, email_data: dict, is_from_zip: bool = False):
        """
        Process a single file (PDF, DOC, CSV, etc.) and upload to specified folder.
        CLEANUP happens in finally block regardless of success/failure.
        """
        original_file_name = os.path.basename(file_path)
        unique_file_name = get_unique_filename(original_file_name)
        local_path = os.path.join(self._base_download_dir, unique_file_name)

        upload_result = None
        file_hash = None
        parse_status = "NotProcessed"
        heron_user_id = None
        pdf_id = None
        company_name = ""
        sharepoint_url = ""
        file_content = None

        heron_service_client = self.heron_service

        logger.info(f"Processing file: {original_file_name}{' (from ZIP)' if is_from_zip else ''}")
        if unique_file_name != original_file_name:
            logger.info(f"Renamed to avoid duplicate: {unique_file_name}")

        try:
            # Save locally (if not already in base_download_dir)
            if file_path != local_path:
                if os.path.exists(file_path):
                    with open(file_path, "rb") as src:
                        file_content = src.read()
                    with open(local_path, "wb") as dst:
                        dst.write(file_content)
                    logger.info(f"Saved file locally → {local_path}")
                else:
                    logger.warning(f"File not found: {file_path}")
                    log_attachment(email_data.get('id', 'unknown'), unique_file_name, 'N/A', outcome="file_missing")
                    return
            else:
                with open(file_path, "rb") as f:
                    file_content = f.read()

            # Compute hash
            file_hash = compute_sha256(local_path)

            # Upload to SharePoint using unique filename
            upload_result = self._upload_with_retry(local_path, folder_name, unique_file_name)
            if upload_result:
                parse_status = "Success"
                sharepoint_url = upload_result.get("webUrl", "")
                logger.info(f"File uploaded to: {folder_name}/{unique_file_name}")
            else:
                parse_status = "UploadFailed"
                logger.error(f"Failed to upload {unique_file_name} to SharePoint")
                return

            # Detect bank statement
            is_bank, company_name = self._detector.detect(file_content, unique_file_name)

            if not is_bank:
                parse_status = "NotBankStatement"
                logger.info(f"File {unique_file_name} is not a bank statement.")
            else:
                try:
                    # Heron: Upload & Parse
                    heron_user_id = heron_service_client.ensure_user(company_name)
                    logger.info(f"Heron End User ID: {heron_user_id}")

                    success, pdf_id = heron_service_client.upload_and_parse_with_retry(
                        heron_user_id=heron_user_id,
                        file_path=local_path,
                        max_retries=3,
                    )

                    if success:
                        parsed_data = heron_service_client.get_enriched_transactions(heron_user_id)
                        parse_status = "Parsed"
                        num_tx = len(parsed_data.get('transactions_enriched', [])) if parsed_data else 0
                        logger.info(f"Heron retrieval successful. Transactions retrieved: {num_tx} Tx")
                    else:
                        parse_status = "HeronError"
                        logger.error(f"Heron processing failed after retries for {unique_file_name}")

                except Exception as e:
                    logger.error(f"Heron Processing Error for {unique_file_name}: {e}")
                    parse_status = "HeronError"

            # Update SharePoint metadata
            if upload_result:
                sp_update = self.sp_metadata_service.update_sharepoint_metadata_graph(
                    drive_id=upload_result["drive_id"],
                    item_id=upload_result["id"],
                    attachment_hash=file_hash,
                    source_email_id=email_data.get("id", ""),
                    source_sender=email_data.get("sender", ""),
                    processing_status=parse_status,
                    heron_pdf_id=pdf_id or "",
                    company_name=company_name,
                    sharepoint_url=sharepoint_url,
                    end_user_id=heron_user_id
                )
                if sp_update:
                    logger.info(f"SharePoint metadata updated for {unique_file_name} → {parse_status}")
                else:
                    logger.error(f"SharePoint metadata update failed for {unique_file_name}")

            # Log attachment outcome with unique filename
            log_attachment(
                email_data.get('id', 'unknown'),
                unique_file_name,
                file_hash,
                outcome=parse_status
            )

        except Exception as e:
            parse_status = "Error"
            logger.critical(f"Unexpected error during processing of {unique_file_name}:\n{traceback.format_exc()}")
            log_attachment(email_data.get('id', 'unknown'), unique_file_name, file_hash or 'N/A',
                           outcome=parse_status, error=str(e))

        finally:
            # CLEANUP: Always remove local files regardless of success/failure
            logger.info(f"Starting cleanup for {unique_file_name}...")

            # Remove the processed file from base_download_dir
            if os.path.exists(local_path):
                self._cleanup_local_file(local_path, "processed file")

            # If file was copied from different location, remove original too
            if not is_from_zip and file_path != local_path:
                if os.path.exists(file_path):
                    self._cleanup_local_file(file_path, "original file")

            logger.info(f"Cleanup completed for {unique_file_name}")

    def process_email(self, email_data: dict):
        """
        Processes all attachments within a single email with new folder structure.

        Structure created:
        YYYY.MM.DD-company_name/
          ├── Dataroom/
          └── summary_mail/
        """
        sender = email_data['sender']
        subject = email_data['subject']
        attachments = email_data.get('attachments', [])

        if not attachments:
            logger.info("No attachments found, skipping email.")
            return

        # Extract company name from sender
        company_name = sender.split('@')[0] if '@' in sender else sender

        # Create base folder name with date
        date_str = datetime.utcnow().strftime('%Y.%m.%d')
        base_folder_initial = f"{date_str} {company_name}"

        # Get unique folder name (adds (1), (2) if folder already exists in SharePoint)
        base_folder = self._get_unique_folder_name(base_folder_initial)

        logger.info(f"Processing email from {sender}, base folder → {base_folder}")
        if base_folder != base_folder_initial:
            logger.info(f"Folder renamed to avoid duplicate: {base_folder}")
        logger.info(f"Folder structure: {base_folder}/Dataroom/ and {base_folder}/summary_mail/")

        # Process all attachments - they will go into Dataroom
        for attachment_path in attachments:
            self.process_attachment(attachment_path, base_folder, email_data)

        # Upload email PDF to summary_mail folder
        self._upload_email_pdf(email_data, base_folder, company_name)

        logger.info(f"Email processing completed for {base_folder}")