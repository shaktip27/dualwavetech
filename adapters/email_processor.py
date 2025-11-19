import os
import time
from datetime import datetime
from adapters.utils.logger import get_logger
import hashlib
import json
import traceback
from adapters.utils.heron_service import HeronService
from adapters.utils.sharepoint_metadata_service import SharePointMetadataService
from adapters.utils.pdf_generator import generate_email_pdf
from adapters.utils.zip_handler import ZipHandler

# Import LLM extractor
from adapters.utils.llm_company_extractor import PDFAnalyzerGenAI

logger = get_logger("email_processor")

# -------------------- Ledger --------------------

LEDGER_FILE = os.path.join(os.getcwd(), "attachment_log.json")

# Company name extraction retry configuration
MAX_COMPANY_EXTRACTION_RETRIES = 2


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

    def __init__(self, storage_adapter, detector, base_download_dir: str,
                 sp_metadata_service: SharePointMetadataService, heron_service: HeronService):
        self._storage_adapter = storage_adapter
        self._detector = detector
        self._base_download_dir = base_download_dir
        self._max_retries = 3
        self.sp_metadata_service = sp_metadata_service
        self.heron_service = heron_service
        self.zip_handler = ZipHandler()
        self._extracted_company_name = None  # Cache for company name within a batch
        self._timestamped_folder = None  # Cache for folder structure
        self._email_pdf_uploaded = False  # Flag to track if email PDF was uploaded
        self.pdf_analyzer = PDFAnalyzerGenAI()  # Initialize once

    def _cleanup_local_file(self, file_path: str, description: str):
        """Safely remove a local file if it exists."""
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
                logger.debug(f"Successfully removed {description} file: {file_path}")
            except OSError as e:
                logger.error(f"Error deleting {description} file {file_path}: {e}")

    def _cleanup_directory(self, dir_path: str, description: str):
        """Safely remove a directory and all its contents."""
        if os.path.exists(dir_path) and os.path.isdir(dir_path):
            try:
                import shutil
                shutil.rmtree(dir_path)
                logger.debug(f"Successfully removed {description} directory: {dir_path}")
            except Exception as e:
                logger.error(f"Error deleting {description} directory {dir_path}: {e}")

    def _upload_with_retry(self, local_path: str, folder_name: str, file_name: str):
        """Upload a file to SharePoint with exponential backoff retry logic."""
        for attempt in range(1, self._max_retries + 1):
            logger.info(f"Uploading {file_name} to SharePoint (Attempt {attempt}/{self._max_retries})...")
            try:
                result = self._storage_adapter.upload_file(local_path, folder_path=folder_name)
                logger.info(f"Upload successful for {file_name} to {result.get('webUrl')}")
                return result
            except Exception as e:
                logger.warning(f"Upload FAILED for {file_name} on attempt {attempt}: {e}")
                if attempt < self._max_retries:
                    time.sleep(2 ** attempt)
                else:
                    logger.error(f"Upload permanently FAILED for {file_name} after {self._max_retries} attempts.")
                    return None
        return None

    def _extract_company_name_with_retry(self, file_path: str) -> str:
        """
        Extract company name with retry logic (max 2 attempts).
        Returns company name or None if extraction fails (NO DEFAULT VALUE).
        """
        logger.info(f"Extracting company name from file: {os.path.basename(file_path)}")

        for attempt in range(1, MAX_COMPANY_EXTRACTION_RETRIES + 1):
            try:
                logger.info(f"Company name extraction attempt {attempt}/{MAX_COMPANY_EXTRACTION_RETRIES}")

                gemini_response = self.pdf_analyzer.analyze_pdf(file_path=file_path)

                if gemini_response and len(gemini_response) > 0:
                    company_name = gemini_response[0].get("owner")

                    if company_name and company_name.strip() and company_name.upper() not in ["UNKNOWN_COMPANY",
                                                                                              "UNKNOWN", ""]:
                        logger.info(f"Successfully extracted company name: {company_name}")
                        return company_name.strip()
                    else:
                        logger.warning(f"Invalid company name extracted on attempt {attempt}: {company_name}")
                else:
                    logger.warning(f"Empty response from PDF analyzer on attempt {attempt}")

            except Exception as e:
                logger.error(f"Error extracting company name on attempt {attempt}: {e}")
                logger.debug(traceback.format_exc())

            if attempt < MAX_COMPANY_EXTRACTION_RETRIES:
                logger.info(f"Retrying company name extraction after 2 seconds...")
                time.sleep(2)

        logger.error(
            f"Failed to extract company name after {MAX_COMPANY_EXTRACTION_RETRIES} attempts. ABORTING PROCESS.")
        return None  # Return None to indicate failure - NO DEFAULT VALUE

    def _create_timestamped_folder(self, company_name: str) -> str:
        """
        Create timestamped folder structure at root: YYYY.MM.DD_company_name
        If folder exists, append (1), (2), etc.
        Uses folder_exists() to properly check for existing folders.
        """
        timestamp = datetime.utcnow().strftime('%Y.%m.%d')
        base_folder = f"{timestamp}_{company_name}"

        counter = 0
        folder_name = base_folder

        # Check if folder exists using the folder_exists method
        while self._storage_adapter.folder_exists(folder_name):
            counter += 1
            folder_name = f"{base_folder}({counter})"
            if counter > 100:  # Safety limit
                logger.error(f"Reached maximum folder counter limit (100) for {base_folder}")
                break

        logger.info(f"Using folder name: {folder_name}")
        return folder_name

    def _upload_email_pdf(self, email_data: dict, company_folder: str):
        """
        Generate a PDF from email data and upload it to summary_mail folder.
        Only uploads once per email processing cycle.
        """
        if self._email_pdf_uploaded:
            logger.info("Email PDF already uploaded for this email. Skipping duplicate upload.")
            return

        try:
            os.makedirs(self._base_download_dir, exist_ok=True)

            pdf_path = generate_email_pdf(email_data, self._base_download_dir)

            timestamp = datetime.utcnow().strftime('%Y.%m.%d-%H.%M.%S')
            new_pdf_name = f"{timestamp}-email_summary.pdf"

            new_pdf_path = os.path.join(self._base_download_dir, new_pdf_name)
            os.rename(pdf_path, new_pdf_path)

            summary_mail_folder = f"{company_folder}/summary_mail"

            upload_result = self._upload_with_retry(
                new_pdf_path,
                folder_name=summary_mail_folder,
                file_name=new_pdf_name
            )

            if upload_result:
                logger.info(f"Email PDF uploaded successfully to {summary_mail_folder}")
                self._email_pdf_uploaded = True  # Mark as uploaded
            else:
                logger.warning(f"Failed to upload email PDF for subject: {email_data.get('subject')}")

            if os.path.exists(new_pdf_path):
                os.remove(new_pdf_path)

        except Exception as e:
            logger.error(f"Failed to generate/upload email PDF: {e}")

    def process_attachment(self, adapter_temp_path: str, email_data: dict) -> None:
        """
        Process attachment locally, then upload to final destination.
        """
        file_name = os.path.basename(adapter_temp_path)
        logger.info(f"Starting processing for file: {file_name}")

        if self.zip_handler.is_zip_file(adapter_temp_path):
            logger.info(f"Detected ZIP file: {file_name}. Extracting contents...")
            self._process_zip_attachment(adapter_temp_path, email_data)
        else:
            self._process_single_file(adapter_temp_path, email_data)

    def _process_zip_attachment(self, zip_path: str, email_data: dict):
        """Extract ZIP and process each file"""
        zip_file_name = os.path.basename(zip_path)
        extract_dir = os.path.join(self._base_download_dir, f"extracted_{int(time.time())}")

        try:
            extracted_files = self.zip_handler.extract_zip(zip_path, extract_dir)

            if not extracted_files:
                logger.warning(f"No files extracted from ZIP: {zip_file_name}")
                return

            supported_files = self.zip_handler.get_supported_files(extracted_files)

            if not supported_files:
                logger.warning(f"No supported files found in ZIP: {zip_file_name}")
                return

            logger.info(f"Processing {len(supported_files)} files from ZIP: {zip_file_name}")

            # Separate bank statements and non-bank files
            bank_statements = []
            non_bank_files = []

            for extracted_file in supported_files:
                try:
                    with open(extracted_file, "rb") as f:
                        file_content = f.read()

                    file_name = os.path.basename(extracted_file)
                    is_bank, _ = self._detector.detect(file_content, file_name)

                    if is_bank:
                        bank_statements.append(extracted_file)
                        logger.info(f"File {file_name} identified as BANK STATEMENT")
                    else:
                        non_bank_files.append(extracted_file)
                        logger.info(f"File {file_name} identified as NON-BANK file")

                except Exception as e:
                    logger.error(f"Error checking file type for {extracted_file}: {e}")

            # Process non-bank files first
            if non_bank_files:
                logger.info(f"Processing {len(non_bank_files)} non-bank files from ZIP...")
                for non_bank_file in non_bank_files:
                    try:
                        logger.info(f"Processing non-bank file: {os.path.basename(non_bank_file)}")
                        self._process_single_file(non_bank_file, email_data, is_from_zip=True)
                    except Exception as e:
                        logger.error(f"Error processing non-bank file {non_bank_file}: {e}")

            # Process bank statements
            if bank_statements:
                logger.info(f"Processing {len(bank_statements)} bank statements from ZIP...")
                for bank_statement in bank_statements:
                    try:
                        logger.info(f"Processing bank statement: {os.path.basename(bank_statement)}")
                        self._process_single_file(bank_statement, email_data, is_from_zip=True)
                    except Exception as e:
                        logger.error(f"Error processing bank statement {bank_statement}: {e}")

            log_attachment(
                email_data.get('id', 'unknown'),
                zip_file_name,
                compute_sha256(zip_path),
                outcome="ZIPProcessed"
            )

        except Exception as e:
            logger.error(f"Error processing ZIP file {zip_file_name}: {e}")
        finally:
            self._cleanup_directory(extract_dir, "ZIP extraction")
            self._cleanup_local_file(zip_path, "ZIP file")

    def _process_single_file(self, file_path: str, email_data: dict, is_from_zip: bool = False):
        """
        NEW FLOW IMPLEMENTATION with company name caching and retry logic.
        """
        original_file_name = os.path.basename(file_path)
        unique_file_name = get_unique_filename(original_file_name)

        local_path = os.path.join(self._base_download_dir, unique_file_name)

        logger.info(f"Processing file: {original_file_name}{' (from ZIP)' if is_from_zip else ''}")

        file_hash = None

        try:
            # Step 1: Save file locally if needed
            if file_path != local_path:
                if os.path.exists(file_path):
                    with open(file_path, "rb") as src:
                        file_content = src.read()
                    with open(local_path, "wb") as dst:
                        dst.write(file_content)
                else:
                    logger.warning(f"File not found: {file_path}")
                    return
            else:
                with open(file_path, "rb") as f:
                    file_content = f.read()

            file_hash = compute_sha256(local_path)

            # Step 2: Check if file is a BANK STATEMENT
            logger.info(f"Checking if file is a bank statement: {unique_file_name}")
            is_bank, _ = self._detector.detect(file_content, unique_file_name)

            if not is_bank:
                logger.info(f"File {unique_file_name} is NOT a bank statement. Moving to non_bank folder...")

                # Create timestamped non_bank folder
                timestamp = datetime.utcnow().strftime('%Y.%m.%d')
                non_bank_folder = f"{timestamp}_non_bank"

                upload_result = self._upload_with_retry(local_path, non_bank_folder, unique_file_name)

                if upload_result:
                    logger.info(f"File moved to non_bank folder: {upload_result.get('webUrl')}")
                    log_attachment(
                        email_data.get('id', 'unknown'),
                        unique_file_name,
                        file_hash,
                        outcome="NotBankStatement_MovedToNonBank"
                    )
                else:
                    logger.error(f"Failed to move file to non_bank folder")
                    log_attachment(
                        email_data.get('id', 'unknown'),
                        unique_file_name,
                        file_hash,
                        outcome="NotBankStatement_UploadFailed"
                    )

                logger.info(f"Processing stopped for non-bank statement: {unique_file_name}")
                return

            # Step 3: IS A BANK STATEMENT - Extract company name with caching
            logger.info(f"File IS a bank statement: {unique_file_name}")

            # Use cached company name if available, otherwise extract it
            if self._extracted_company_name:
                company_name = self._extracted_company_name
                logger.info(f"Using cached company name: {company_name}")
            else:
                company_name = self._extract_company_name_with_retry(local_path)

                if not company_name:
                    logger.error(
                        f"Failed to extract company name after {MAX_COMPANY_EXTRACTION_RETRIES} attempts. ABORTING PROCESS for {unique_file_name}")
                    log_attachment(
                        email_data.get('id', 'unknown'),
                        unique_file_name,
                        file_hash,
                        outcome="CompanyExtractionFailed_ProcessAborted",
                        error="Failed to extract company name after retries - process aborted"
                    )
                    return

                # Cache the extracted company name for subsequent files
                self._extracted_company_name = company_name

            logger.info(f"Company Name: {company_name}")

            # Step 4: Create or use cached timestamped folder structure
            if self._timestamped_folder:
                timestamped_folder = self._timestamped_folder
                logger.info(f"Using cached folder structure: {timestamped_folder}")
            else:
                timestamped_folder = self._create_timestamped_folder(company_name)
                self._timestamped_folder = timestamped_folder  # Cache for subsequent files
                logger.info(f"Created new folder structure: {timestamped_folder}")

            company_folder = f"{timestamped_folder}/Dataroom"

            # Step 5: Upload to company/Dataroom folder
            logger.info(f"Uploading file to {company_folder}...")

            final_upload_result = self._upload_with_retry(
                local_path,
                company_folder,
                unique_file_name
            )

            if not final_upload_result:
                logger.error(f"Failed to upload file to {company_folder}")
                log_attachment(email_data.get('id', 'unknown'), unique_file_name, file_hash,
                               outcome="UploadFailed")
                return

            logger.info(f"File uploaded successfully to: {final_upload_result.get('webUrl')}")

            # Step 6: Call HERON API for parsing
            logger.info(f"Calling Heron API for parsing...")

            heron_user_id = None
            pdf_id = None
            parse_status = "Pending"

            try:
                heron_user_id = self.heron_service.ensure_user(company_name)
                logger.info(f"Heron End User ID: {heron_user_id}")

                success, pdf_id = self.heron_service.upload_and_parse_with_retry(
                    heron_user_id=heron_user_id,
                    file_path=local_path,
                    max_retries=3,
                )

                if success:
                    parsed_data = self.heron_service.get_enriched_transactions(heron_user_id)
                    num_tx = len(parsed_data.get('transactions_enriched', [])) if parsed_data else 0
                    logger.info(f"Heron parsing successful. Transactions: {num_tx}")
                    parse_status = "Parsed"
                else:
                    logger.error(f"Heron processing failed after retries")
                    parse_status = "HeronError"

            except Exception as e:
                logger.error(f"Heron Processing Error: {e}")
                parse_status = "HeronError"

            # Step 7: Update SharePoint metadata
            if final_upload_result:
                sp_update = self.sp_metadata_service.update_sharepoint_metadata_graph(
                    drive_id=final_upload_result["drive_id"],
                    item_id=final_upload_result["id"],
                    attachment_hash=file_hash,
                    source_email_id=email_data.get("id", ""),
                    source_sender=email_data.get("sender", ""),
                    processing_status=parse_status,
                    heron_pdf_id=pdf_id or "",
                    company_name=company_name,
                    sharepoint_url=final_upload_result.get('webUrl', ''),
                    end_user_id=heron_user_id or ""
                )

                if sp_update:
                    logger.info(f"SharePoint metadata updated: {parse_status}")
                else:
                    logger.error(f"SharePoint metadata update failed")

            # Step 8: Upload email PDF to summary_mail folder (ONLY ONCE)
            if not self._email_pdf_uploaded:
                logger.info(f"Uploading email PDF to summary_mail folder...")
                self._upload_email_pdf(email_data, timestamped_folder)

            log_attachment(
                email_data.get('id', 'unknown'),
                unique_file_name,
                file_hash,
                outcome=parse_status
            )

            logger.info(f"Processing completed successfully for {unique_file_name}")

        except Exception as e:
            logger.critical(f"Unexpected error processing {unique_file_name}: {str(e)}")
            logger.debug(traceback.format_exc())

            log_attachment(
                email_data.get('id', 'unknown'),
                unique_file_name,
                file_hash or 'N/A',
                outcome="Error",
                error=str(e)
            )

        finally:
            self._cleanup_local_file(local_path, "attachment")

            if file_path != local_path:
                self._cleanup_local_file(file_path, "adapter temporary")

    def process_email(self, email_data: dict):

        sender = email_data['sender']
        subject = email_data['subject']
        attachments = email_data.get('attachments', [])

        if not attachments:
            logger.info("No attachments found, skipping email.")
            return

        logger.info(f"Processing email from {sender}: {subject}")
        logger.info(f"Found {len(attachments)} attachment(s)")

        # Reset all caches for each new email
        self._extracted_company_name = None
        self._timestamped_folder = None
        self._email_pdf_uploaded = False

        for attachment_path in attachments:
            try:
                self.process_attachment(attachment_path, email_data)
            except Exception as e:
                logger.error(f"Error processing attachment {attachment_path}: {e}")

        logger.info(f"Email processing completed for: {subject}")