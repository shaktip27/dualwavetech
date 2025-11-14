import requests
import re
from adapters.utils.logger import get_logger
import base64
import time
import json
import os

logger = get_logger("heron_service")


class HeronService:
    BASE_URL = "https://app.herondata.io/api"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.headers = {
            "x-api-key": self.api_key,
            "Content-Type": "application/json"
        }

    def generate_user_id(self, company_name: str) -> str:
        """Convert company name to safe Heron user ID"""
        clean = re.sub(r'[^a-zA-Z0-9]', '', company_name).upper()
        return f"ene_{clean}"

    def check_user_exists(self, end_user_id: str):
        """Check if end_user exists on Heron"""
        try:
            url = f"{self.BASE_URL}/end_users/{end_user_id}"
            response = requests.get(url, headers=self.headers, timeout=10)

            if response.status_code == 200:
                logger.info(f"Heron user exists: {end_user_id}")
                return response.json()
            elif response.status_code == 404:
                logger.info(f"Heron user NOT found: {end_user_id}")
                return None
            else:
                logger.error(f"Error checking user ({end_user_id}): {response.status_code} {response.text}")
                return None

        except requests.RequestException as e:
            logger.error(f"RequestException while checking user {end_user_id}: {e}")
            return None

    def create_user(self, end_user_id: str, company_name: str):
        """Create new end_user in Heron"""
        try:
            url = f"{self.BASE_URL}/end_users"
            payload = {
                "end_user": {
                    "end_user_id": end_user_id,
                    "name": "Test" + company_name
                }
            }
            response = requests.post(url, headers=self.headers, json=payload, timeout=10)

            if response.status_code in [200, 201]:
                logger.info(f"Heron user created: {end_user_id}")
                return response.json()
            else:
                logger.error(f"Failed to create user {end_user_id}: {response.status_code} {response.text}")
                return None

        except requests.RequestException as e:
            logger.error(f"RequestException while creating user {end_user_id}: {e}")
            return None

    def ensure_user(self, company_name: str) -> str:
        """Ensure user exists (Check → Create if not exists)"""
        try:
            user_id = self.generate_user_id(company_name)
            existing = self.check_user_exists(user_id)

            if existing:
                return user_id

            created = self.create_user(user_id, company_name)
            if created:
                return user_id

            raise Exception(f"Heron user could not be created for {company_name}")

        except Exception as e:
            logger.error(f"Error ensuring user for {company_name}: {e}")
            raise

    def upload_pdf(self, heron_user_id: str, file_path: str):
        """Upload PDF file to Heron for a user"""
        try:
            with open(file_path, "rb") as f:
                file_bytes = f.read()
            file_b64 = base64.b64encode(file_bytes).decode("utf-8")

            payload = {
                "file_base64": file_b64,
                "file_class": "bank_statement",
                "filename": os.path.basename(file_path),
                "reference_id": f"file_{int(time.time())}"
            }
            url = f"{self.BASE_URL}/end_users/{heron_user_id}/files"
            response = requests.post(url, headers=self.headers, json=payload, timeout=30)

            if response.status_code in [200, 201]:
                file_heron_id = response.json().get("heron_id")
                logger.info(f"PDF uploaded successfully → Heron file_id: {file_heron_id}")
                return file_heron_id
            else:
                logger.error(f"PDF upload failed: {response.status_code} {response.text}")
                return None

        except Exception as e:
            logger.error(f"Exception uploading PDF: {e}")
            return None

    def parse_all_pdfs(self, heron_user_id: str):
        """Trigger PDF parsing"""
        try:
            url = f"{self.BASE_URL}/end_users/{heron_user_id}/pdfs/parse"
            response = requests.post(url, headers={"x-api-key": self.api_key}, timeout=30)

            if response.status_code in [200, 201]:
                logger.info(f"Parse started successfully for {heron_user_id}")
                return response.json()
            else:
                logger.error(f"Heron parse failed for {heron_user_id}: {response.status_code} {response.text}")
                return None

        except Exception as e:
            logger.error(f"Exception while triggering parse for {heron_user_id}: {e}")
            return None

    def check_file_status(self, heron_user_id: str):
        """Gets the list of files and their processing status"""
        try:
            url = f"{self.BASE_URL}/end_users/{heron_user_id}/files"
            response = requests.get(url, headers={"x-api-key": self.api_key}, timeout=10)

            if response.status_code == 200:
                try:
                    return response.json()
                except json.JSONDecodeError as e:
                    logger.error(f"JSON Decode Error for {heron_user_id} status check: {e}")
                    return None
            else:
                logger.warning(f"File status API returned status {response.status_code} for user {heron_user_id}")
                return None

        except requests.RequestException as e:
            logger.error(f"RequestException during file status check: {e}")
            return None

    def wait_for_parsing(self, heron_user_id: str, max_retries: int = 30, delay: int = 10):
        """
        Polls file status until parsing completes successfully or fails.
        """
        logger.info(f"Starting to poll parsing status for {heron_user_id}...")

        SUCCESS_STATES = {"transactions_loaded", "parsed", "completed"}
        PENDING_STATES = {"new", "processing", "parsing", "human_reviewing"}
        FAILED_STATES = {"failed", "error", "rejected"}

        for attempt in range(max_retries):
            try:
                file_data = self.check_file_status(heron_user_id)

                if not file_data:
                    logger.warning(f"Attempt {attempt + 1}/{max_retries}: No response or invalid format. Retrying...")
                    time.sleep(delay)
                    continue

                if not isinstance(file_data, list):
                    logger.warning(f"Attempt {attempt + 1}/{max_retries}: Unexpected data type {type(file_data)}. Retrying...")
                    time.sleep(delay)
                    continue

                all_done = True  # assume done unless a pending one is found

                for f in file_data:
                    bank_statement = f.get("bank_statement")
                    if not bank_statement or not isinstance(bank_statement, dict):
                        continue

                    status = bank_statement.get("status", "unknown")
                    logger.info(f"Attempt {attempt + 1}/{max_retries}: File status = {status}")

                    if status in SUCCESS_STATES:
                        logger.info(f"✓ File successfully parsed ({status}) for {heron_user_id}")
                        return True
                    elif status in PENDING_STATES:
                        all_done = False
                    elif status in FAILED_STATES:
                        logger.error(f"File parsing failed ({status}) for {heron_user_id}")
                        return False

                if not all_done:
                    logger.info(f"Attempt {attempt + 1}/{max_retries}: Parsing still in progress. Waiting {delay}s...")
                    time.sleep(delay)
                    continue

                logger.warning(f"Attempt {attempt + 1}/{max_retries}: No valid status found. Retrying...")
                time.sleep(delay)

            except Exception as e:
                logger.error(f"Exception during status polling (Attempt {attempt + 1}): {e}")
                time.sleep(delay)

        logger.error(f"Parsing timed out after {max_retries * delay}s for {heron_user_id}.")
        return False

    def upload_and_parse_with_retry(self, heron_user_id: str, file_path: str, max_retries: int = 30, delay: int = 10):
        """
        Full workflow: upload → parse → wait → auto re-upload if stuck

        Returns:
            tuple: (success: bool, file_id: str or None)
        """
        file_id = self.upload_pdf(heron_user_id, file_path)
        if not file_id:
            logger.error("Initial upload failed. Aborting.")
            return False, None  # ← FIXED: Return tuple

        self.parse_all_pdfs(heron_user_id)
        success = self.wait_for_parsing(heron_user_id, max_retries=max_retries, delay=delay)

        if not success:
            # Check if all files are stuck at 'new'
            file_data = self.check_file_status(heron_user_id)
            if file_data and all(f.get("bank_statement", {}).get("status") == "new" for f in file_data):
                logger.warning("All files stuck at 'new'. Re-uploading PDF to retry...")
                new_file_id = self.upload_pdf(heron_user_id, file_path)
                if new_file_id:
                    file_id = new_file_id  # Update file_id with new upload
                    self.parse_all_pdfs(heron_user_id)
                    success = self.wait_for_parsing(heron_user_id, max_retries=max_retries, delay=delay)
                else:
                    logger.error("Re-upload failed. Manual check needed.")
                    return False, None  # ← FIXED: Return tuple

        return success, file_id  # ← FIXED: Return tuple (bool, str)

    def get_enriched_transactions(self, heron_user_id: str):
        """Retrieves all enriched transactions for a specific end user."""
        try:
            url = f"{self.BASE_URL}/end_users/{heron_user_id}/transactions"
            response = requests.get(url, headers={"x-api-key": self.api_key}, timeout=30)

            if response.status_code == 200:
                try:
                    data = response.json()
                    logger.info("Successfully retrieved enriched transactions.")
                    return data
                except json.JSONDecodeError as e:
                    logger.error(f"JSON decode error when retrieving transactions: {e}")
                    return None
            else:
                logger.error(f"Failed to retrieve transactions: {response.status_code} {response.text}")
                return None

        except requests.RequestException as e:
            logger.error(f"Exception retrieving transactions: {e}")
            return None
