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
from adapters.utils.llm_company_extractor import PDFAnalyzerGenAI

logger = get_logger("email_processor")

# Configuration
LEDGER_FILE = os.path.join(os.getcwd(), "attachment_log.json")
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
    """Compute SHA256 hash of a file."""
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def is_duplicate(file_hash):
    """Check if file hash exists in ledger."""
    ledger = ensure_ledger()
    return any(entry["hash"] == file_hash for entry in ledger)


def get_unique_filename(original_filename: str) -> str:
    """
    Check ledger for duplicate filenames and return unique filename.
    Appends (1), (2), etc. if filename exists.
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
    """
    Main email processor that handles bank statement detection,
    company name extraction, and file organization.
    """

    def __init__(self, storage_adapter, detector, base_download_dir: str,
                 sp_metadata_service: SharePointMetadataService, heron_service: HeronService):
        self._storage_adapter = storage_adapter
        self._detector = detector
        self._base_download_dir = base_download_dir
        self._max_retries = 3
        self.sp_metadata_service = sp_metadata_service
        self.heron_service = heron_service
        self.zip_handler = ZipHandler()
        self._extracted_company_name = None
        self._timestamped_folder = None
        self._email_pdf_uploaded = False
        self.pdf_analyzer = PDFAnalyzerGenAI()

    def _cleanup_local_file(self, file_path: str, description: str):
        """Safely remove a local file if it exists."""
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
                logger.debug(f"Removed {description}: {file_path}")
            except OSError as e:
                logger.error(f"Failed to delete {description} {file_path}: {e}")

    def _cleanup_directory(self, dir_path: str, description: str):
        """Safely remove a directory and all its contents."""
        if os.path.exists(dir_path) and os.path.isdir(dir_path):
            try:
                import shutil
                shutil.rmtree(dir_path)
                logger.debug(f"Removed {description} directory: {dir_path}")
            except Exception as e:
                logger.error(f"Failed to delete {description} directory {dir_path}: {e}")

    def _upload_with_retry(self, local_path: str, folder_name: str, file_name: str):
        """Upload file to SharePoint with exponential backoff retry."""
        for attempt in range(1, self._max_retries + 1):
            logger.info(f"[Attempt {attempt}/{self._max_retries}] Uploading: {file_name}")
            try:
                result = self._storage_adapter.upload_file(local_path, folder_path=folder_name)
                logger.info(f"Upload SUCCESS: {file_name} -> {result.get('webUrl')}")
                return result
            except Exception as e:
                logger.warning(f"Upload FAILED (Attempt {attempt}): {e}")
                if attempt < self._max_retries:
                    time.sleep(2 ** attempt)
                else:
                    logger.error(f"Upload PERMANENTLY FAILED after {self._max_retries} attempts")
                    return None
        return None

    def _extract_company_name_with_retry(self, file_path: str) -> str:
        """Extract company name using LLM with retry logic."""
        logger.info(f"Extracting company name from: {os.path.basename(file_path)}")

        for attempt in range(1, MAX_COMPANY_EXTRACTION_RETRIES + 1):
            try:
                logger.info(f"Extraction attempt {attempt}/{MAX_COMPANY_EXTRACTION_RETRIES}")

                gemini_response = self.pdf_analyzer.analyze_pdf(file_path=file_path)

                if gemini_response and len(gemini_response) > 0:
                    company_name = gemini_response[0].get("owner")

                    if company_name and company_name.strip() and company_name.upper() not in ["UNKNOWN_COMPANY",
                                                                                              "UNKNOWN", ""]:
                        logger.info(f"Company name extracted: {company_name}")
                        return company_name.strip()
                    else:
                        logger.warning(f"Invalid company name on attempt {attempt}: {company_name}")
                else:
                    logger.warning(f"Empty response from analyzer on attempt {attempt}")

            except Exception as e:
                logger.error(f"Extraction error on attempt {attempt}: {e}")
                logger.debug(traceback.format_exc())

            if attempt < MAX_COMPANY_EXTRACTION_RETRIES:
                logger.info("Retrying company extraction in 2 seconds...")
                time.sleep(2)

        logger.error(f"Company extraction FAILED after {MAX_COMPANY_EXTRACTION_RETRIES} attempts")
        return None

    def _create_timestamped_folder(self, company_name: str) -> str:
        """Create timestamped folder: YYYY.MM.DD_company_name"""
        timestamp = datetime.utcnow().strftime('%Y.%m.%d')
        base_folder = f"{timestamp}_{company_name}"

        counter = 0
        folder_name = base_folder

        while self._storage_adapter.folder_exists(folder_name):
            counter += 1
            folder_name = f"{base_folder}({counter})"
            if counter > 100:
                logger.error(f"Folder counter limit reached (100) for {base_folder}")
                break

        logger.info(f"Folder name determined: {folder_name}")
        return folder_name

    def _upload_email_pdf(self, email_data: dict, company_folder: str):
        """Generate and upload email PDF to summary_mail folder."""
        if self._email_pdf_uploaded:
            logger.info("Email PDF already uploaded, skipping")
            return

        try:
            os.makedirs(self._base_download_dir, exist_ok=True)
            pdf_path = generate_email_pdf(email_data, self._base_download_dir)

            timestamp = datetime.utcnow().strftime('%Y.%m.%d-%H.%M.%S')
            new_pdf_name = f"{timestamp}-email_summary.pdf"
            new_pdf_path = os.path.join(self._base_download_dir, new_pdf_name)
            os.rename(pdf_path, new_pdf_path)

            summary_mail_folder = f"{company_folder}/summary_mail"

            upload_result = self._upload_with_retry(new_pdf_path, summary_mail_folder, new_pdf_name)

            if upload_result:
                logger.info(f"Email PDF uploaded to {summary_mail_folder}")
                self._email_pdf_uploaded = True
            else:
                logger.warning(f"Email PDF upload failed for: {email_data.get('subject')}")

            if os.path.exists(new_pdf_path):
                os.remove(new_pdf_path)

        except Exception as e:
            logger.error(f"Email PDF generation/upload failed: {e}")

    def process_attachment(self, adapter_temp_path: str, email_data: dict) -> None:
        """Process attachment - handles both ZIP and single files."""
        file_name = os.path.basename(adapter_temp_path)
        logger.info(f"Processing attachment: {file_name}")

        if self.zip_handler.is_zip_file(adapter_temp_path):
            logger.info(f"ZIP archive detected: {file_name}")
            self._process_zip_attachment(adapter_temp_path, email_data)
        else:
            self._process_single_file(adapter_temp_path, email_data)

    def _process_zip_attachment(self, zip_path: str, email_data: dict):
        """Extract ZIP and process contents (bank statements first)."""
        zip_file_name = os.path.basename(zip_path)
        extract_dir = os.path.join(self._base_download_dir, f"extracted_{int(time.time())}")

        try:
            extracted_files = self.zip_handler.extract_zip(zip_path, extract_dir)

            if not extracted_files:
                logger.warning(f"No files extracted from ZIP: {zip_file_name}")
                return

            supported_files = self.zip_handler.get_supported_files(extracted_files)

            if not supported_files:
                logger.warning(f"No supported files in ZIP: {zip_file_name}")
                return

            logger.info(f"Processing {len(supported_files)} files from ZIP")

            # Separate bank statements and non-bank files
            bank_statements = []
            non_bank_files = []

            for extracted_file in supported_files:
                try:
                    if not os.path.exists(extracted_file):
                        logger.error(f"Extracted file not found: {extracted_file}")
                        continue

                    with open(extracted_file, "rb") as f:
                        file_content = f.read()

                    file_name = os.path.basename(extracted_file)
                    is_bank, _ = self._detector.detect(file_content, file_name)

                    if is_bank:
                        bank_statements.append(extracted_file)
                        logger.info(f"[BANK] {file_name}")
                    else:
                        non_bank_files.append(extracted_file)
                        logger.info(f"[NON-BANK] {file_name}")

                except Exception as e:
                    logger.error(f"Error checking file: {extracted_file}: {e}")

            # Process bank statements first
            if bank_statements:
                logger.info(f"Processing {len(bank_statements)} bank statement(s)")
                for bank_statement in bank_statements:
                    try:
                        if os.path.exists(bank_statement):
                            self._process_single_file(bank_statement, email_data, is_from_zip=True)
                    except Exception as e:
                        logger.error(f"Error processing bank statement: {e}")

            # Process non-bank files
            if non_bank_files:
                logger.info(f"Processing {len(non_bank_files)} non-bank file(s)")
                for non_bank_file in non_bank_files:
                    try:
                        if os.path.exists(non_bank_file):
                            self._process_single_file(non_bank_file, email_data, is_from_zip=True)
                    except Exception as e:
                        logger.error(f"Error processing non-bank file: {e}")

            # Cleanup
            self._cleanup_directory(extract_dir, "ZIP extraction")

            if os.path.exists(zip_path):
                zip_hash = compute_sha256(zip_path)
                self._cleanup_local_file(zip_path, "ZIP file")
            else:
                zip_hash = "N/A"

            log_attachment(email_data.get('id', 'unknown'), zip_file_name, zip_hash, outcome="ZIPProcessed")

        except Exception as e:
            logger.error(f"ZIP processing error: {e}")

    def _process_single_file(self, file_path: str, email_data: dict, is_from_zip: bool = False):
        """Process single file - bank detection and upload."""
        original_file_name = os.path.basename(file_path)
        unique_file_name = get_unique_filename(original_file_name)
        local_path = os.path.join(self._base_download_dir, unique_file_name)

        origin = " (from ZIP)" if is_from_zip else ""
        logger.info(f"Processing: {original_file_name}{origin}")

        file_hash = None

        try:
            # Save file locally
            if not os.path.exists(file_path):
                logger.error(f"Source file not found: {file_path}")
                return

            if file_path != local_path:
                with open(file_path, "rb") as src:
                    file_content = src.read()
                with open(local_path, "wb") as dst:
                    dst.write(file_content)

            # Verify file
            if not os.path.exists(local_path) or os.path.getsize(local_path) == 0:
                logger.error(f"Invalid local file: {local_path}")
                return

            file_hash = compute_sha256(local_path)

            # Bank detection
            file_ext = os.path.splitext(unique_file_name)[1].lower()
            bankable_extensions = ['.pdf', '.doc', '.docx', '.csv']

            if file_ext not in bankable_extensions:
                logger.info(f"Non-document file: {unique_file_name} ({file_ext})")
                is_bank = False
            else:
                with open(local_path, "rb") as f:
                    file_content_for_detection = f.read()
                is_bank, _ = self._detector.detect(file_content_for_detection, unique_file_name)

            logger.info(f"Detection result: is_bank={is_bank}")

            # CASE 1: BANK STATEMENT
            if is_bank:
                logger.info(f"[BANK STATEMENT] {unique_file_name}")

                # Extract company name
                if self._extracted_company_name:
                    company_name = self._extracted_company_name
                    logger.info(f"Using cached company: {company_name}")
                else:
                    company_name = self._extract_company_name_with_retry(local_path)
                    if not company_name:
                        logger.error("Company extraction failed - ABORTING")
                        log_attachment(email_data.get('id'), unique_file_name, file_hash,
                                       outcome="CompanyExtractionFailed")
                        return
                    self._extracted_company_name = company_name

                # Create folder
                if self._timestamped_folder:
                    timestamped_folder = self._timestamped_folder
                else:
                    timestamped_folder = self._create_timestamped_folder(company_name)
                    self._timestamped_folder = timestamped_folder

                company_folder = f"{timestamped_folder}/Dataroom"

                # Upload
                final_upload_result = self._upload_with_retry(local_path, company_folder, unique_file_name)

                if not final_upload_result:
                    logger.error(f"Upload failed: {company_folder}")
                    log_attachment(email_data.get('id'), unique_file_name, file_hash, outcome="UploadFailed")
                    return

                # Heron API
                heron_user_id = None
                pdf_id = None
                parse_status = "Pending"

                try:
                    heron_user_id = self.heron_service.ensure_user(company_name)
                    success, pdf_id = self.heron_service.upload_and_parse_with_retry(
                        heron_user_id=heron_user_id, file_path=local_path, max_retries=3
                    )
                    parse_status = "Parsed" if success else "HeronError"
                except Exception as e:
                    logger.error(f"Heron error: {e}")
                    parse_status = "HeronError"

                # Metadata
                if final_upload_result:
                    self.sp_metadata_service.update_sharepoint_metadata_graph(
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

                # Email PDF
                if not self._email_pdf_uploaded:
                    self._upload_email_pdf(email_data, timestamped_folder)

                log_attachment(email_data.get('id'), unique_file_name, file_hash, outcome=parse_status)

            # CASE 2: NON-BANK FILE
            else:
                logger.info(f"[NON-BANK] {unique_file_name}")

                # Upload to company Dataroom if exists
                if self._timestamped_folder and self._extracted_company_name:
                    logger.info("Company folder exists - uploading to Dataroom")
                    company_folder = f"{self._timestamped_folder}/Dataroom"
                    upload_result = self._upload_with_retry(local_path, company_folder, unique_file_name)

                    if upload_result:
                        self.sp_metadata_service.update_sharepoint_metadata_graph(
                            drive_id=upload_result["drive_id"],
                            item_id=upload_result["id"],
                            attachment_hash=file_hash,
                            source_email_id=email_data.get("id", ""),
                            source_sender=email_data.get("sender", ""),
                            processing_status="NonBankFile",
                            heron_pdf_id="",
                            company_name=self._extracted_company_name,
                            sharepoint_url=upload_result.get('webUrl', ''),
                            end_user_id=""
                        )
                        log_attachment(email_data.get('id'), unique_file_name, file_hash,
                                       outcome="NonBankFile_Dataroom")

                # Upload to non_bank folder
                else:
                    logger.info("No company folder - uploading to non_bank")
                    timestamp = datetime.utcnow().strftime('%Y.%m.%d')
                    non_bank_folder = f"{timestamp}_non_bank"
                    upload_result = self._upload_with_retry(local_path, non_bank_folder, unique_file_name)

                    if upload_result:
                        log_attachment(email_data.get('id'), unique_file_name, file_hash,
                                       outcome="NonBank_Folder")

        except Exception as e:
            logger.critical(f"Processing error: {e}")
            log_attachment(email_data.get('id'), unique_file_name, file_hash or 'N/A',
                           outcome="Error", error=str(e))

        finally:
            self._cleanup_local_file(local_path, "attachment")
            if file_path != local_path:
                self._cleanup_local_file(file_path, "temporary")

    def process_email(self, email_data: dict):
        """Main email processing entry point with pre-scan logic."""
        sender = email_data['sender']
        subject = email_data['subject']
        attachments = email_data.get('attachments', [])

        if not attachments:
            logger.info("No attachments - skipping")
            return

        logger.info("=" * 100)
        logger.info(f"NEW EMAIL | From: {sender} | Subject: {subject} | Attachments: {len(attachments)}")
        logger.info("=" * 100)

        # Reset caches
        self._extracted_company_name = None
        self._timestamped_folder = None
        self._email_pdf_uploaded = False

        # PHASE 1: PRE-SCAN
        logger.info("")
        logger.info("PHASE 1: PRE-SCANNING ATTACHMENTS")
        logger.info("-" * 100)

        has_bank_statement = False
        bank_statement_files = []
        non_bank_files = []
        all_files_catalog = []

        for idx, attachment_path in enumerate(attachments, 1):
            try:
                file_name = os.path.basename(attachment_path)
                file_size = os.path.getsize(attachment_path) if os.path.exists(attachment_path) else 0

                logger.info(f"[{idx}/{len(attachments)}] Scanning: {file_name} ({file_size:,} bytes)")

                # ZIP file
                if self.zip_handler.is_zip_file(attachment_path):
                    logger.info("  Type: ZIP Archive")

                    extract_dir = os.path.join(self._base_download_dir, f"prescan_{int(time.time())}_{file_name}")
                    extracted_files = self.zip_handler.extract_zip(attachment_path, extract_dir)
                    supported_files = self.zip_handler.get_supported_files(extracted_files)

                    logger.info(f"  Contents: {len(supported_files)} supported file(s)")

                    zip_has_bank = False
                    zip_file_list = []

                    for extracted_file in supported_files:
                        file_ext = os.path.splitext(extracted_file)[1].lower()
                        bankable_extensions = ['.pdf', '.doc', '.docx', '.csv']
                        extracted_file_name = os.path.basename(extracted_file)

                        zip_file_list.append(extracted_file_name)

                        if file_ext in bankable_extensions:
                            with open(extracted_file, "rb") as f:
                                file_content = f.read()
                            is_bank, _ = self._detector.detect(file_content, extracted_file_name)

                            if is_bank:
                                logger.info(f"    [BANK] {extracted_file_name}")
                                has_bank_statement = True
                                zip_has_bank = True
                                break
                            else:
                                logger.info(f"    [DOC] {extracted_file_name}")
                        else:
                            logger.info(f"    [FILE] {extracted_file_name}")

                    self._cleanup_directory(extract_dir, "prescan")

                    if zip_has_bank:
                        bank_statement_files.append(attachment_path)
                        all_files_catalog.append((file_name, "ZIP_WITH_BANK", zip_file_list))
                        logger.info("  Result: BANK STATEMENT FOUND")
                    else:
                        non_bank_files.append(attachment_path)
                        all_files_catalog.append((file_name, "ZIP_NO_BANK", zip_file_list))
                        logger.info("  Result: No bank statements")

                # Single file
                else:
                    file_ext = os.path.splitext(file_name)[1].lower()
                    bankable_extensions = ['.pdf', '.doc', '.docx', '.csv']

                    if file_ext in bankable_extensions:
                        logger.info(f"  Type: Document ({file_ext})")

                        with open(attachment_path, "rb") as f:
                            file_content = f.read()
                        is_bank, _ = self._detector.detect(file_content, file_name)

                        if is_bank:
                            logger.info("  Result: BANK STATEMENT")
                            has_bank_statement = True
                            bank_statement_files.append(attachment_path)
                            all_files_catalog.append((file_name, "BANK_STATEMENT", None))
                        else:
                            logger.info("  Result: Non-bank document")
                            non_bank_files.append(attachment_path)
                            all_files_catalog.append((file_name, "NON_BANK_DOC", None))
                    else:
                        logger.info(f"  Type: Non-document ({file_ext})")
                        logger.info("  Result: Not bankable")
                        non_bank_files.append(attachment_path)
                        all_files_catalog.append((file_name, "NON_DOCUMENT", None))

            except Exception as e:
                logger.error(f"Pre-scan error: {e}")
                non_bank_files.append(attachment_path)
                all_files_catalog.append((os.path.basename(attachment_path), "ERROR", None))

        # SCAN SUMMARY
        logger.info("")
        logger.info("=" * 100)
        logger.info("PRE-SCAN SUMMARY")
        logger.info("=" * 100)
        logger.info(f"  Total scanned: {len(attachments)}")
        logger.info(f"  Bank statements: {len(bank_statement_files)}")
        logger.info(f"  Non-bank files: {len(non_bank_files)}")
        logger.info("")
        logger.info("  File Catalog:")

        for file_name, file_type, zip_contents in all_files_catalog:
            if file_type.startswith("ZIP"):
                logger.info(f"    [ZIP] {file_name} -> {file_type}")
                if zip_contents:
                    for zf in zip_contents:
                        logger.info(f"      |- {zf}")
            else:
                logger.info(f"    [{file_type}] {file_name}")

        logger.info("=" * 100)

        # DECISION
        logger.info("")
        if has_bank_statement:
            logger.info("DECISION: BANK STATEMENT DETECTED")
            logger.info("  Action: All files -> COMPANY DATAROOM")
        else:
            logger.info("DECISION: NO BANK STATEMENT")
            logger.info("  Action: All files -> NON_BANK folder")
        logger.info("=" * 100)

        # PHASE 2: PROCESSING
        logger.info("")
        logger.info("PHASE 2: PROCESSING ATTACHMENTS")
        logger.info("-" * 100)

        # Bank statements first
        if has_bank_statement:
            logger.info(f"Step 1: Processing bank statements ({len(bank_statement_files)})")

            for i, attachment_path in enumerate(bank_statement_files, 1):
                try:
                    logger.info(f"[{i}/{len(bank_statement_files)}] {os.path.basename(attachment_path)}")
                    self.process_attachment(attachment_path, email_data)
                except Exception as e:
                    logger.error(f"Error: {e}")

            if self._timestamped_folder and self._extracted_company_name:
                logger.info(f"Company folder: {self._timestamped_folder}")
                logger.info(f"Company name: {self._extracted_company_name}")
            else:
                logger.error("CRITICAL: Company folder not created!")

        # Non-bank files
        logger.info(f"Step 2: Processing non-bank files ({len(non_bank_files)})")

        for i, attachment_path in enumerate(non_bank_files, 1):
            try:
                logger.info(f"[{i}/{len(non_bank_files)}] {os.path.basename(attachment_path)}")
                self.process_attachment(attachment_path, email_data)
            except Exception as e:
                logger.error(f"Error: {e}")

        # COMPLETION
        logger.info("")
        logger.info("=" * 100)
        logger.info("EMAIL PROCESSING COMPLETE")
        logger.info("=" * 100)
        logger.info(f"  Subject: {subject}")
        logger.info(f"  Files processed: {len(bank_statement_files) + len(non_bank_files)}")

        if has_bank_statement:
            logger.info(f"  Destination: {self._timestamped_folder}")
            logger.info(f"  Company: {self._extracted_company_name}")
        else:
            timestamp = datetime.utcnow().strftime('%Y.%m.%d')
            logger.info(f"  Destination: {timestamp}_non_bank")

        logger.info("=" * 100)
        logger.info("")