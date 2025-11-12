import yaml
from adapters.utils.logger import setup_logger, get_logger
from adapters.auth.outlook_authenticator import OutlookAuthenticator
from adapters.email.outlook_email_adapter import OutlookEmailAdapter
from adapters.storage.sharepoint_uploader import SharePointAdapter
from adapters.detector.bank_statement_detector import BankStatementDetector
from adapters.email_processor import EmailProcessor
from adapters.utils.sharepoint_metadata_service import SharePointMetadataService
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from adapters.utils.heron_service import HeronService
import time
import os
from adapters.utils.config import config
from dotenv import load_dotenv

load_dotenv()

# -------------------- Setup Logger --------------------
setup_logger("app.log")
logger = get_logger("main")


# -------------------- Load Config --------------------
filters = config["email"]["filters"]
heron = HeronService(api_key=config["heron"]["api_key"])

# -------------------- Initialize Dependencies --------------------
logger.info("Initializing dependencies...")

authenticator = OutlookAuthenticator(
    client_id=os.getenv("CLIENT_ID"),
    client_secret=os.getenv("CLIENT_SECRET"),
    tenant_id=os.getenv("TENANT_ID")
)
detector = BankStatementDetector()

# SharePoint Adapter
sharepoint_config = config["sharepoint"]
uploader = SharePointAdapter(
    client_id=sharepoint_config["client_id"],
    client_secret=sharepoint_config["client_secret"],
    tenant_id=sharepoint_config["tenant_id"],
    site_name=sharepoint_config["site_name"],
)

lists = requests.get(
    f"https://graph.microsoft.com/v1.0/sites/{uploader.site_id}/lists",
    headers={"Authorization": f"Bearer {uploader.access_token}"}
).json().get("value", [])

# Use "Shared Documents" library
list_id = next(lst["id"] for lst in lists if lst["name"] == "Shared Documents")

# SharePoint Metadata Service
sp_metadata_service = SharePointMetadataService(
    site_id=uploader.site_id,
    list_id=list_id,
    access_token=uploader.access_token
)

# Email Processor
email_processor = EmailProcessor(
    storage_adapter=uploader,
    detector=detector,
    base_download_dir=config["storage"]["base_download_dir"],
    sp_metadata_service=sp_metadata_service,
    heron_service=heron
)

# Email Adapter
adapter = OutlookEmailAdapter(authenticator, download_dir="downloads", filters=filters)
adapter.connect()
logger.info("Connected to Outlook mailbox.")

# -------------------- Scheduler Job Function --------------------
def process_emails_job():
    logger.info("Scheduler triggered: Checking new emails...")
    try:
        for email in adapter.fetch_emails():
            logger.info(f"Email received: {email['subject']} | Attachments: {len(email['attachments'])}")
            email_processor.process_email(email)
    except Exception as e:
        logger.critical(f"Error in scheduled email processing: {e}")
    logger.info("Scheduler run finished.")

# -------------------- Start Scheduler --------------------
if __name__ == "__main__":
    scheduler = BackgroundScheduler()
    # Run every 5 minutes
    scheduler.add_job(process_emails_job, 'interval', seconds=5)
    scheduler.start()
    logger.info("Scheduler started. Running every 1 minutes...")

    try:
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopping...")
        scheduler.shutdown()