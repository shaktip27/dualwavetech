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

logger = get_logger("email_processor")
# -------------------- Heron --------------------


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
            return []  # empty if file is corrupted


def compute_sha256(file_path):
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def is_duplicate(file_hash):
    ledger = ensure_ledger()
    return any(entry["hash"] == file_hash for entry in ledger)


def log_attachment(message_id, file_name, file_hash, outcome, error=None):
    """Append attachment metadata to JSON ledger."""
    ledger = ensure_ledger()  # load fresh every time
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

    #----> description----
    """Handles the full lifecycle of an incoming email:
    downloading attachments, uploading to SharePoint with retries,
    detecting bank statements, uploading to Heron, and cleaning up local files.
    """

    def __init__(self, storage_adapter, detector, base_download_dir: str, sp_metadata_service: SharePointMetadataService,heron_service: HeronService):
        """
        :param storage_adapter: SharePointAdapter instance
        :param detector: Bank statement detector
        :param base_download_dir: local directory to temporarily store attachments
        :param sharepoint_site_url: SharePoint site URL
        :param sharepoint_client_id: SharePoint client ID
        :param sharepoint_client_secret: SharePoint client secret
        """
        self._storage_adapter = storage_adapter
        self._detector = detector
        self._base_download_dir = base_download_dir
        self._max_retries = 3
        self.sp_metadata_service=sp_metadata_service
        self.heron_service = heron_service


    def _cleanup_local_file(self, file_path: str, description: str):
        """
        Safely remove a local file if it exists.

        Args:
            file_path (str): Full path to the file to be removed
            description (str): Description of the file for logging purposes
        """
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
                logger.debug(f"Successfully removed {description} file: {file_path}")
            except OSError as e:
                logger.error(f"Error deleting {description} file {file_path}: {e}")

    def _upload_with_retry(self, local_path: str, folder_name: str, file_name: str):
        """
        Attempt to upload a file to SharePoint with exponential backoff retry logic.

        Args:
            local_path (str): Local path to the file to upload
            folder_name (str): Target folder name in SharePoint
            file_name (str): Name to give the file in SharePoint

        Returns:
            dict or None: Upload result on success, None on failure
        """
        for attempt in range(1, self._max_retries + 1):
            logger.info(f"Uploading {file_name} to SharePoint (Attempt {attempt}/{self._max_retries})...")
            try:
                result = self._storage_adapter.upload_file(local_path, folder_path=folder_name)
                logger.info(f"Upload successful for {file_name} → {result.get('webUrl')}")
                logger.info("===>>>>> ATTACHMENTS UPLOADED ON THE SHAREPOINT SUCCESSFULLY,READY TO DETECT THE ATTACHMENTS WEATHER IT IS BANK STATEMENT OR NOT")
                return result
            except Exception as e:
                logger.warning(f"Upload FAILED for {file_name} on attempt {attempt}: {e}")
                if attempt < self._max_retries:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error(f"Upload permanently FAILED for {file_name} after {self._max_retries} attempts.")
                    return None
        return None

    def _upload_email_pdf(self, email_data: dict):
        """
        Generate a PDF from email data and upload it to SharePoint.

        Args:
            email_data (dict): Dictionary containing email data (subject, body, etc.)
        """
        try:
            os.makedirs(self._base_download_dir, exist_ok=True)
            pdf_path = generate_email_pdf(email_data, self._base_download_dir)

            upload_result = self._upload_with_retry(pdf_path, folder_name="mail_pdf",
                                                    file_name=os.path.basename(pdf_path))

            if upload_result:
                logger.info(f"Email PDF uploaded successfully: {upload_result.get('webUrl')}")
            else:
                logger.warning(f"Failed to upload email PDF for subject: {email_data.get('subject')}")

            # Clean up local PDF
            if os.path.exists(pdf_path):
                os.remove(pdf_path)

        except Exception as e:
            logger.error(f"Failed to generate/upload email PDF: {e}")

    # def process_attachment(self, adapter_temp_path: str, folder_name: str, email_data: dict) -> None:
    #     """
    #     Processes a single attachment:
    #     - Saves locally
    #     - Computes hash and logs duplicate
    #     - Uploads to SharePoint (if not duplicate)
    #     - Detects bank statement (if not duplicate)
    #     - Uploads to Heron if applicable
    #     - Triggers parsing
    #     - Updates SharePoint metadata
    #     - Logs outcome and cleans up local files
    #     """
    #
    #     file_name = os.path.basename(adapter_temp_path)
    #     # Sanitize folder_name for SharePoint (remove invalid chars)
    #     safe_folder_name = re.sub(r'[\\/*?:"<>|]', "_", folder_name)
    #
    #     local_path = os.path.join(self._base_download_dir, file_name)
    #     upload_result = None
    #     file_hash = None
    #     parse_status = "NotProcessed"
    #     heron_user_id = None
    #     pdf_id = None
    #     company_name = ""
    #     sharepoint_url = ""
    #
    #     logger.info(f"Starting processing for attachment: {file_name}")
    #
    #     try:
    #         # -------------------- Save locally --------------------
    #         if os.path.exists(adapter_temp_path):
    #             with open(adapter_temp_path, "rb") as src:
    #                 file_content = src.read()
    #             with open(local_path, "wb") as dst:
    #                 dst.write(file_content)
    #             logger.info(f"Saved attachment locally → {local_path}")
    #         else:
    #             logger.warning(f"Attachment file not found: {adapter_temp_path}")
    #             log_attachment(email_data.get('id', 'unknown'), file_name, file_hash or 'N/A', outcome="file_missing")
    #             return
    #
    #         # -------------------- Compute hash --------------------
    #         file_hash = compute_sha256(local_path)
    #
    #         # -------------------- Check duplicate --------------------
    #         if is_duplicate(file_hash):
    #             parse_status = "Duplicate"
    #             logger.warning(f"Duplicate detected: {file_name}, skipping upload and Heron processing.")
    #
    #             # Just log duplicate, DO NOT update SharePoint
    #             log_attachment(
    #                 email_data.get('id', 'unknown'),
    #                 file_name,
    #                 file_hash,
    #                 outcome=parse_status
    #             )
    #             return  # Skip further processing for duplicates
    #
    #         # -------------------- Upload to SharePoint --------------------
    #         upload_result = self._upload_with_retry(local_path, safe_folder_name, file_name)
    #         if upload_result and parse_status != "UploadFailed":
    #             parse_status = "Success"
    #             sharepoint_url = upload_result.get("webUrl", "")
    #         else:
    #             parse_status = "UploadFailed"
    #             logger.error(f"Failed to upload {file_name} to SharePoint")
    #
    #         # -------------------- Detect bank statement --------------------
    #         is_bank, company_name = self._detector.detect(file_content, file_name)
    #         logger.info(f"========> company_name,{company_name}")
    #         if not is_bank:
    #             parse_status = "NotBankStatement"
    #             logger.info(f"Attachment {file_name} is not a bank statement.")
    #         else:
    #             # -------------------- Heron: user & PDF --------------------
    #             internal_user_id = heron.generate_user_id(company_name)
    #             logger.info(f"Generated internal user ID: {internal_user_id}")
    #
    #             existing = heron.check_user_exists(internal_user_id)
    #             if existing:
    #                 heron_user_id = existing["end_user"]["heron_id"]
    #                 logger.info(f"Heron user already exists → heron_id: {heron_user_id}")
    #             else:
    #                 data = heron.create_user(internal_user_id, company_name)
    #                 heron_user_id = data["end_user"]["heron_id"]
    #                 logger.info(f"Created new Heron user → heron_id: {heron_user_id}")
    #
    #             # Upload PDF to Heron
    #             pdf_id = heron.upload_pdf(heron_user_id, local_path)
    #             if not pdf_id:
    #                 parse_status = "UploadFailed"
    #                 logger.error(f"Failed to upload PDF to Heron for {heron_user_id}")
    #             else:
    #                 # Trigger parse
    #                 parse_result = heron.parse_all_pdfs(heron_user_id)
    #                 logger.info(f"=============> parse_result, {parse_result}")
    #                 if parse_result:
    #                     parse_status = "Parsed"
    #                     logger.info(f"Heron parsing started successfully for {heron_user_id}")
    #                 else:
    #                     parse_status = "ParseFailed"
    #                     logger.error(f"Heron parsing failed for {heron_user_id}")
    #
    #         # -------------------- Update SharePoint metadata --------------------
    #         if upload_result:
    #             sp_update = self.sp_metadata_service.update_sharepoint_metadata_graph(
    #                 drive_id=upload_result["drive_id"],
    #                 item_id=upload_result["id"],
    #                 attachment_hash=file_hash,
    #                 source_email_id=email_data.get("id", ""),
    #                 source_sender=email_data.get("sender", ""),
    #                 processing_status=parse_status,
    #                 heron_pdf_id=pdf_id or "",
    #                 company_name=company_name,
    #                 sharepoint_url=sharepoint_url
    #             )
    #             if sp_update:
    #                 logger.info(f"SharePoint metadata updated for {file_name} → {parse_status}")
    #             else:
    #                 logger.error(f"SharePoint metadata update failed for {file_name}")
    #
    #         # -------------------- Log attachment outcome --------------------
    #         log_attachment(
    #             email_data.get('id', 'unknown'),
    #             file_name,
    #             file_hash,
    #             outcome=parse_status
    #         )
    #
    #     except Exception as e:
    #         parse_status = "Error"
    #         logger.critical(f"Unexpected error during processing of {file_name}:\n{traceback.format_exc()}")
    #         log_attachment(email_data.get('id', 'unknown'), file_name, file_hash or 'N/A', outcome=parse_status,
    #                        error=str(e))
    #
    #     finally:
    #         # -------------------- Cleanup --------------------
    #         if upload_result:
    #             self._cleanup_local_file(local_path, "attachment")
    #         self._cleanup_local_file(adapter_temp_path, "adapter temporary")

    def process_attachment(self, adapter_temp_path: str, folder_name: str, email_data: dict) -> None:
        """
        Processes a single attachment:
        - Saves locally
        - Computes hash and logs duplicate
        - Uploads to SharePoint (if not duplicate)
        - Detects bank statement (if not duplicate)
        - Uploads to Heron, triggers parsing, WAITS for completion, and RETRIEVES data.
        - Updates SharePoint metadata
        - Logs outcome and cleans up local files
        """

        file_name = os.path.basename(adapter_temp_path)

        # Sanitize folder_name for SharePoint (remove invalid chars)
        safe_folder_name = re.sub(r'[\\/*?:"<>|]', "_", folder_name)

        local_path = os.path.join(self._base_download_dir, file_name)
        upload_result = None
        file_hash = None
        parse_status = "NotProcessed"
        heron_user_id = None
        pdf_id = None
        company_name = ""
        sharepoint_url = ""

        #CRITICAL FIX: Assigns self.heron_service to the local variable for use below
        heron_service_client = self.heron_service

        logger.info(f"Starting processing for attachment: {file_name}")

        try:
            # -------------------- Save locally --------------------
            if os.path.exists(adapter_temp_path):
                with open(adapter_temp_path, "rb") as src:
                    file_content = src.read()
                with open(local_path, "wb") as dst:
                    dst.write(file_content)
                logger.info(f"Saved attachment locally → {local_path}")
            else:
                logger.warning(f"Attachment file not found: {adapter_temp_path}")
                log_attachment(email_data.get('id', 'unknown'), file_name, file_hash or 'N/A', outcome="file_missing")
                return

            # -------------------- Compute hash --------------------
            file_hash = compute_sha256(local_path)

            # -------------------- Check duplicate --------------------
            if is_duplicate(file_hash):
                parse_status = "Duplicate"
                logger.warning(f"Duplicate detected: {file_name}, skipping upload and Heron processing.")

                log_attachment(
                    email_data.get('id', 'unknown'),
                    file_name,
                    file_hash,
                    outcome=parse_status
                )
                return  # Skip further processing for duplicates

            # -------------------- Upload to SharePoint --------------------
            upload_result = self._upload_with_retry(local_path, safe_folder_name, file_name)
            if upload_result and parse_status != "UploadFailed":
                parse_status = "Success"
                sharepoint_url = upload_result.get("webUrl", "")
            else:
                parse_status = "UploadFailed"
                logger.error(f"Failed to upload {file_name} to SharePoint")
                # If SharePoint upload failed, we can't update metadata later, so we return.
                return

                # -------------------- Detect bank statement --------------------
            # to handle any bank PDF, returning a company_name
            is_bank, company_name = self._detector.detect(file_content, file_name)
            logger.info(f"========> company_name,{company_name}")

            if not is_bank:
                parse_status = "NotBankStatement"
                logger.info(f"Attachment {file_name} is not a bank statement.")
            else:
                try:
                    # -------------------- Heron: User Setup --------------------
                    # We rely on the robust ensure_user method from the HeronService class
                    heron_user_id = heron_service_client.ensure_user(company_name)
                    logger.info(f"Heron End User ID: {heron_user_id}")

                    # Upload PDF to Heron
                    pdf_id = heron_service_client.upload_pdf(heron_user_id, local_path)
                    if not pdf_id:
                        parse_status = "HeronUploadFailed"
                        raise Exception("Failed to upload PDF to Heron.")

                    # Trigger parse
                    heron_service_client.parse_all_pdfs(heron_user_id)
                    logger.info("Heron parsing triggered. Starting wait cycle.")
                    parse_status = "ParsingInProgress"

                    # -------------------- CRITICAL: WAIT FOR PARSING --------------------
                    if heron_service_client.wait_for_parsing(heron_user_id):
                        # Step A: Parsing is complete. Now retrieve the enriched data.
                        parsed_data = heron_service_client.get_enriched_transactions(heron_user_id)

                        # --- FINAL DEFENSIVE CHECK ON RETRIEVED DATA ---
                        if parsed_data is not None and isinstance(parsed_data, dict) and parsed_data.get(
                                'transactions_enriched') is not None:
                            # Set status to simple 'Parsed' success, regardless of Tx count
                            parse_status = "Parsed"
                            num_tx = len(parsed_data['transactions_enriched'])
                            logger.info(
                                f"Heron retrieval successful. Status: Parsed. Transactions retrieved: {num_tx} Tx")
                        else:
                            # If parsed_data is None (API failure) or the key is missing/empty
                            parse_status = "ParseRetrievalFailed"
                            logger.error("Parsing completed, but retrieval failed or data was empty/invalid.")
                    else:
                        parse_status = "ParseTimeout"
                        logger.error("Heron parsing timed out or failed to reach 'transactions_loaded'.")

                except Exception as e:
                    # Catch specific Heron setup/processing errors
                    logger.error(f"Heron Processing Error for {file_name}: {e}")
                    parse_status = "HeronError"

            # -------------------- Update SharePoint metadata --------------------
            # This update now happens AFTER the Heron processing and waiting is complete.
            if upload_result:
                sp_update = self.sp_metadata_service.update_sharepoint_metadata_graph(
                    drive_id=upload_result["drive_id"],
                    item_id=upload_result["id"],
                    attachment_hash=file_hash,
                    source_email_id=email_data.get("id", ""),
                    source_sender=email_data.get("sender", ""),
                    processing_status=parse_status,  # Use the final determined status
                    heron_pdf_id=pdf_id or "",
                    company_name=company_name,
                    sharepoint_url=sharepoint_url
                )
                if sp_update:
                    logger.info(f"SharePoint metadata updated for {file_name} → {parse_status}")
                else:
                    logger.error(f"SharePoint metadata update failed for {file_name}")

            # -------------------- Log attachment outcome --------------------
            log_attachment(
                email_data.get('id', 'unknown'),
                file_name,
                file_hash,
                outcome=parse_status
            )

        except Exception as e:
            parse_status = "Error"
            logger.critical(f"Unexpected error during processing of {file_name}:\n{traceback.format_exc()}")
            log_attachment(email_data.get('id', 'unknown'), file_name, file_hash or 'N/A', outcome=parse_status,
                           error=str(e))

        finally:
            # -------------------- Cleanup --------------------
            self._cleanup_local_file(local_path, "attachment")
            self._cleanup_local_file(adapter_temp_path, "adapter temporary")

    def process_email(self, email_data: dict):
        """Processes all attachments within a single email."""
        sender = email_data['sender']
        subject = email_data['subject']
        attachments = email_data.get('attachments', [])

        if not attachments:
            logger.info("No attachments found, skipping email.")

        company_name = sender.split('@')[0] if '@' in sender else sender
        folder_name = f"{datetime.utcnow().strftime('%Y-%m-%d')}-{company_name}"

        logger.info(f"Processing email from {sender}, folder → {folder_name}")

        # Upload attachments
        for attachment_path in attachments:
            self.process_attachment(attachment_path, folder_name, email_data)

        # Upload email PDF
        self._upload_email_pdf(email_data)
        logger.info("==>SUCCESSFULLY PDF UPLOAD ON The HERON API")

