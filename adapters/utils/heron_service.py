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
        """Gets the list of files and their processing status, with robust error handling."""
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
        Polls the file status until a file reaches 'transactions_loaded'.
        FIXED VERSION: Handles None responses properly.
        """
        logger.info(f"Starting to poll status for {heron_user_id}...")

        for attempt in range(max_retries):
            try:
                # Get the status check data
                file_data = self.check_file_status(heron_user_id)

                # CRITICAL FIX: Check if file_data is None BEFORE trying to iterate
                if file_data is None:
                    logger.warning(
                        f"Attempt {attempt + 1}/{max_retries}: API returned None or failed. Retrying in {delay}s...")
                    if attempt < max_retries - 1:
                        time.sleep(delay)
                        continue
                    else:
                        logger.error(f"Max retries ({max_retries}) reached. API consistently failed.")
                        return False

                # Check if file_data is a list before iterating
                if not isinstance(file_data, list):
                    logger.warning(
                        f"Attempt {attempt + 1}/{max_retries}: Unexpected data type: {type(file_data)}. Expected list.")
                    if attempt < max_retries - 1:
                        time.sleep(delay)
                        continue
                    else:
                        logger.error(f"Max retries ({max_retries}) reached. Data format issue.")
                        return False

                # Now safe to iterate - check for success
                for f in file_data:
                    # Defensive chaining with .get()
                    bank_statement = f.get('bank_statement')
                    if bank_statement and isinstance(bank_statement, dict):
                        status = bank_statement.get('status')
                        if status == 'transactions_loaded':
                            logger.info(f"✓ Parsing complete! Status: {status}")
                            return True
                        else:
                            logger.info(f"Attempt {attempt + 1}/{max_retries}: Current status = {status}")

                # If we reach here, parsing is still in progress
                if attempt < max_retries - 1:
                    logger.info(f"Attempt {attempt + 1}/{max_retries}: Parsing still in progress. Waiting {delay}s...")
                    time.sleep(delay)
                else:
                    logger.error(f"Max retries ({max_retries}) reached. Parsing timed out.")
                    return False

            except Exception as e:
                logger.error(f"Exception in wait_for_parsing loop (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(delay)
                else:
                    logger.error(f"Max retries ({max_retries}) reached after exception.")
                    return False

        return False

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