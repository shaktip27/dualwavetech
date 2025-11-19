import io
import re
import csv
import sys
from typing import Optional, List
import pdfplumber
import docx
from adapters.utils.logger import get_logger

# Import for OCR on image-based PDFs
try:
    from pdf2image import convert_from_bytes
    import pytesseract

    OCR_AVAILABLE = True
except ImportError:
    OCR_AVAILABLE = False
    print("⚠️ Warning: pdf2image or pytesseract not installed. Image-based PDF OCR will be skipped.")

logger = get_logger(name="bank_statement_detector.py")

BANK_KEYWORDS = [
    "monthly statement", "account summary", "activity report", "statement period",
    "deposits and withdrawals", "balance forward", "checking account", "savings account",
    "Bank Statement", "Checking", "Checking Account", "Monthly Statement", "Account Summary",
    "Beginning Balance", "Ending Balance", "Account History",
]
COMMON_FILE_PATTERNS = re.compile(r'(bank|statement|account)_\d{4}-\d{2}-\d{2}\.pdf$', re.IGNORECASE)


class BankStatementDetector:

    def __init__(self, required_keywords: List[str] = BANK_KEYWORDS, min_keyword_threshold: int = 3):
        self._required_keywords = [kw.lower() for kw in required_keywords]
        self._min_keyword_threshold = min_keyword_threshold

        if OCR_AVAILABLE:
            logger.info("✅ BankStatementDetector initialized with OCR support (Pytesseract)")
        else:
            logger.warning("⚠️ BankStatementDetector initialized WITHOUT OCR support")

    def _extract_text_with_ocr(self, file_content: bytes) -> Optional[str]:
        """
        Extract text from image-based PDFs using OCR (Pytesseract).

        Args:
            file_content (bytes): PDF file content

        Returns:
            Optional[str]: Extracted text or None if OCR fails
        """
        if not OCR_AVAILABLE:
            logger.warning("⚠️ OCR libraries not available. Skipping OCR extraction.")
            return None

        try:
            logger.info("🔍 Attempting OCR extraction (image-based PDF detected)...")

            # Convert PDF to images
            images = convert_from_bytes(file_content, first_page=1, last_page=3)

            extracted_text = ""

            # Run OCR on each page
            for i, image in enumerate(images, 1):
                logger.info(f"📄 Running OCR on page {i}/{len(images)}...")

                # Extract text using Pytesseract
                page_text = pytesseract.image_to_string(image)
                extracted_text += page_text + "\n"

            if extracted_text.strip():
                logger.info(f"✅ OCR extraction successful! Extracted {len(extracted_text)} characters")
                return extracted_text.lower()
            else:
                logger.warning("⚠️ OCR completed but no text extracted")
                return None

        except Exception as e:
            logger.error(f"❌ OCR extraction failed: {e}")
            return None

    def _extract_pdf(self, file_content: bytes) -> Optional[str]:
        """
        Extract text from PDF. If normal extraction fails or returns minimal text,
        try OCR for image-based PDFs.
        """
        try:
            text = ""
            has_text = False

            with pdfplumber.open(io.BytesIO(file_content)) as pdf:
                for i, page in enumerate(pdf.pages):
                    if i >= 5:  # only first 5 pages
                        break
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text
                        has_text = True

            # Check if we got meaningful text
            if has_text and len(text.strip()) > 100:
                logger.info(f"✅ Extracted {len(text)} characters using pdfplumber")
                return text.lower()

            # If text extraction failed or returned minimal text, try OCR
            logger.info("⚠️ Minimal or no text extracted with pdfplumber. Trying OCR...")
            ocr_text = self._extract_text_with_ocr(file_content)

            if ocr_text:
                return ocr_text

            # If OCR also failed, return what we have (might be empty)
            return text.lower() if text else None

        except Exception as e:
            logger.error(f"❌ PDF extraction failed. File likely corrupt, encrypted, or malformed. Details: {e}")

            # Try OCR as last resort
            logger.info(" Attempting OCR as fallback...")
            return self._extract_text_with_ocr(file_content)

    def _extract_docx(self, file_content: bytes) -> Optional[str]:

        if 'docx' not in sys.modules:
            return None  # Skip if library is missing

        try:
            document = docx.Document(io.BytesIO(file_content))
            text = ""

            for para in document.paragraphs:
                text += para.text + "\n"

            return text.lower()

        except Exception as e:

            logger.error(f"DOCX extraction failed. Details: {e}")

            return None

    def _extract_csv(self, file_content: bytes) -> Optional[str]:

        try:
            # Decode bytes to string, then use io.StringIO for csv reader
            content_str = file_content.decode('utf-8')

            reader = csv.reader(io.StringIO(content_str))

            # Combine all cell data into a single string for keyword search
            text = " ".join([" ".join(row) for row in reader])

            return text.lower()

        except Exception as e:

            logger.error(f"CSV extraction failed. Details: {e}")

            return None

    def _get_file_text(self, file_content: bytes, file_name: str) -> Optional[str]:

        name_lower = file_name.lower()

        if name_lower.endswith('.pdf'):
            return self._extract_pdf(file_content)

        elif name_lower.endswith(('.doc', '.docx')):
            return self._extract_docx(file_content)

        elif name_lower.endswith('.csv'):
            return self._extract_csv(file_content)

        logger.info(f"File type not supported for content extraction: {file_name}")

        return None

    def _check_filename(self, file_name: str) -> bool:

        name_lower = file_name.lower()

        if not name_lower.endswith(('.pdf', '.doc', '.docx', '.csv')):
            return False

        if any(k in name_lower for k in ["statement", "bank", "account_summary"]):
            return True

        if re.search(r'(bank|statement|account)', name_lower, re.IGNORECASE):
            return True

        return False

    def _check_content_keywords(self, file_content: bytes, file_name: str) -> tuple[bool, str]:

        text_content = self._get_file_text(file_content, file_name)

        if not text_content:
            logger.info(f"Content check skipped for {file_name}: Failed to extract text.")
            return (False, None)

        found_count = 0
        for keyword in self._required_keywords:
            if keyword in text_content:
                found_count += 1

        is_match = found_count >= self._min_keyword_threshold
        if is_match:
            logger.info(f"✅ Content matched {found_count} keywords (Threshold: {self._min_keyword_threshold}).")

        return is_match, text_content

    def detect(self, file_content: bytes, file_name: str):
        logger.info(f"🔍 Starting detection for file: {file_name}")

        is_match_content, text_content = self._check_content_keywords(file_content, file_name)
        is_match_filename = self._check_filename(file_name)

        # Extract company name if text exists
        bank_name = self.extract_company_name(text_content) if text_content else "UNKNOWN_COMPANY"

        # Strategy 1: Strong Content Match
        if is_match_content:
            logger.info(f"✅ PASS: Confirmed by strong content keywords for {file_name}.")
            return True, bank_name

        # Strategy 2: Filename Fallback
        if is_match_filename:
            logger.warning(
                f"PASS (FILENAME FALLBACK): Confirmed by strong filename '{file_name}' despite poor content match.")
            return True, bank_name

        logger.info(f" FAIL: File {file_name} does not meet bank statement criteria.")
        return False, bank_name

    def extract_company_name(self, text: str) -> str:
        """
        Extract a generalized Bank or Company Name from bank statement text.
        Prioritizes extraction of the customer entity name as the unique ID.
        """
        if not text:
            return "UNKNOWN_BANK_DEFAULT"

        # Define generalized patterns focusing on structure and common endings
        patterns = [
            # NEW Priority 0: Mixed-case Bank Names (e.g., KeyBank, JP Morgan Bank)
            r"([A-Z][a-zA-Z]*(?:\s+[A-Z][a-zA-Z]*)*\s*(?:Bank|BANK|Trust|TRUST))",

            # Priority 1: All-caps or uppercase block names ending in BANK or TRUST
            r"([A-Z\s,.-]+(BANK|TRUST))\s*\n",

            # Priority 2: Customer Company Name (e.g., ROBERT WEED PLYWOOD CORPORATION)
            r"([A-Z0-9\s,-]+ (LLC|INC|CORP|CO|GROUP|COLLECTIVE))\s*\n",

            # Priority 3: Full Legal Bank Name near address
            r"([A-Z\s,.-]+ (BANK|CREDIT UNION|TRUST|N\.A\.|FINANCIAL))\n",

            # Priority 4: Account name label
            r"Account name:\s*(.+)\n",

            # Priority 5: Bank name followed by address line
            r"([A-Z\s,]+)\s*(BANK|TRUST)\n\s*(\d+\s*[A-Z][a-z]+ Street)",

            # Priority 6: Short word + Bank (fallback)
            r"(\w+)\s*Bank"
        ]

        # Use the initial part of the text for context
        search_text = text[:1000]

        for pattern in patterns:
            # Use re.DOTALL to allow '.' to match newlines, helping capture names across lines
            match = re.search(pattern, search_text, re.IGNORECASE | re.DOTALL | re.MULTILINE)

            if match:
                # Determine which group contains the name based on the pattern structure

                # --- Simplification for the new pattern and existing bank patterns ---
                if len(match.groups()) >= 2 and any(term in pattern for term in ["BANK|TRUST", "FINANCIAL"]):
                    # For patterns with multiple groups (like the new Priority 1 and old Priority 2),
                    # Group 1 is usually the name part.
                    bank_name = match.group(1).strip()

                # --- Handling the original complex logic ---
                elif len(match.groups()) == 3 and ("Street" in pattern):
                    # Original Pattern 4 (Name + Street): Name is usually group 1
                    bank_name = match.group(1).strip()
                elif pattern == r"Account name:\s*(.+)\n":
                    # Original Pattern 3 (Account name): Name is group 1
                    bank_name = match.group(1).strip().split('\n')[0]
                elif len(match.groups()) > 0:
                    # Default to group 1 for most single-capture patterns (like old Priority 1)
                    bank_name = match.group(1).strip()
                else:
                    # Use the whole match if no groups were defined
                    bank_name = match.group(0).strip()

                # Final cleaning: take the first line and ensure it's not junk
                bank_name = bank_name.split('\n')[0].strip()

                if len(bank_name) > 3 and "STATEMENT" not in bank_name.upper() and "BOX" not in bank_name.upper():
                    logger.info(f"📋 Extracted Bank/Company ID: {bank_name}")
                    return bank_name

        # If all regex fails, use a safe, unique fallback derived from the input text hash
        logger.warning("Failed to extract specific bank/company name. Using safe fallback.")
        return "UNKNOWN_BANK_DEFAULT"