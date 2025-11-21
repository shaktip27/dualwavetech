
import time
import requests
from dotenv import load_dotenv

from adapters.utils.logger import setup_logger, get_logger
from adapters.auth.outlook_authenticator import OutlookAuthenticator
from adapters.email.outlook_email_adapter import OutlookEmailAdapter
from adapters.storage.sharepoint_uploader import SharePointAdapter
from adapters.detector.bank_statement_detector import BankStatementDetector
from adapters.email_processor import EmailProcessor
from adapters.utils.sharepoint_metadata_service import SharePointMetadataService
from adapters.utils.heron_service import HeronService
from adapters.utils.config import config

from apscheduler.schedulers.background import BackgroundScheduler

# ---------------------------------------------------------
# Load Env
# ---------------------------------------------------------
load_dotenv()

# ---------------------------------------------------------
# Logger Setup
# ---------------------------------------------------------
setup_logger("app.log")
logger = get_logger("main")

logger.info("=" * 120)
logger.info(" Application Starting...")
logger.info("=" * 120)

# ---------------------------------------------------------
# Load Configurations
# ---------------------------------------------------------
filters = config["email"]["filters"]
sharepoint_config = config["sharepoint"]

logger.info("Configuration loaded successfully.")
logger.info(f"SharePoint -> Site: {sharepoint_config['site_name']} | Tenant: {sharepoint_config['tenant_id'][:8]}...")
logger.info(f"Heron API key present: {'YES' if config['heron']['api_key'] else 'NO'}")
logger.info("NEW FLOW ENABLED: TEMP → DETECT → LLM → COMPANY → HERON")
logger.info("-" * 120)

# ---------------------------------------------------------
# Initialize Heron
# ---------------------------------------------------------
try:
    heron = HeronService(api_key=config["heron"]["api_key"])
    logger.info("HeronService initialized.")
except Exception as e:
    logger.error(f" Failed to initialize HeronService: {e}", exc_info=True)
    raise

# ---------------------------------------------------------
# Initialize SharePoint Auth + Adapter
# ---------------------------------------------------------
logger.info("Initializing SharePoint Adapter...")
try:
    authenticator = OutlookAuthenticator(
        client_id=sharepoint_config["client_id"],
        client_secret=sharepoint_config["client_secret"],
        tenant_id=sharepoint_config["tenant_id"],
    )

    uploader = SharePointAdapter(
        client_id=sharepoint_config["client_id"],
        client_secret=sharepoint_config["client_secret"],
        tenant_id=sharepoint_config["tenant_id"],
        site_name=sharepoint_config["site_name"],
    )

    logger.info("SharePoint Adapter initialized.")

except Exception as e:
    logger.critical(f" Failed to initialize SharePoint adapter: {e}", exc_info=True)
    raise

# ---------------------------------------------------------
# Load SharePoint Lists
# ---------------------------------------------------------
logger.info("Fetching SharePoint lists...")
try:
    lists_response = requests.get(
        f"https://graph.microsoft.com/v1.0/sites/{uploader.site_id}/lists",
        headers={"Authorization": f"Bearer {uploader.access_token}"}
    )

    if lists_response.status_code != 200:
        raise ValueError(f"Graph API Error: {lists_response.status_code}, {lists_response.text}")

    lists = lists_response.json().get("value", [])
    list_id = next(lst["id"] for lst in lists if lst["name"] == "Shared Documents")

    logger.info(f"Using SharePoint Library: Shared Documents (ID: {list_id})")

except Exception as e:
    logger.error(f" Failed to fetch SharePoint lists: {e}", exc_info=True)
    raise

# ---------------------------------------------------------
# Initialize Metadata Service
# ---------------------------------------------------------
sp_metadata_service = SharePointMetadataService(
    site_id=uploader.site_id,
    list_id=list_id,
    sharepoint_adapter=uploader
)
logger.info("Metadata service initialized.")

# ---------------------------------------------------------
# Initialize Detector + Processor
# ---------------------------------------------------------
detector = BankStatementDetector()

email_processor = EmailProcessor(
    storage_adapter=uploader,
    detector=detector,
    base_download_dir=config["storage"]["base_download_dir"],
    sp_metadata_service=sp_metadata_service,
    heron_service=heron
)
logger.info("Email Processor initialized.")

# ---------------------------------------------------------
# Email Adapter
# ---------------------------------------------------------
adapter = OutlookEmailAdapter(authenticator, download_dir="downloads", filters=filters)

try:
    adapter.connect()
    logger.info(" Connected to Outlook mailbox.")
except Exception as e:
    logger.critical(f" Failed to connect to Outlook mailbox: {e}")
    raise

# ---------------------------------------------------------
# Scheduler Job
# ---------------------------------------------------------
def process_emails_job():
    logger.info("=" * 80)
    logger.info(" Scheduler Triggered: Checking for new emails...")
    logger.info("=" * 80)

    try:
        email_count = 0

        for email in adapter.fetch_emails():
            email_count += 1
            logger.info(
                f" Processing Email #{email_count}: "
                f"Subject={email['subject']} | Attachments={len(email['attachments'])}"
            )

            # NEW FLOW PROCESS
            email_processor.process_email(email)

            logger.info(f" Email #{email_count} processing completed.")
            logger.info("-" * 80)

        if email_count == 0:
            logger.info(" No new emails found.")

    except Exception as e:
        logger.error(f" Error during email processing job: {e}", exc_info=True)

    logger.info("Scheduler cycle finished.")
    logger.info("=" * 80)


# ---------------------------------------------------------
# Start Scheduler
# ---------------------------------------------------------
if __name__ == "__main__":

    scheduler = BackgroundScheduler()
    scheduler.add_job(process_emails_job, 'interval', seconds=30)
    scheduler.start()

    logger.info("=" * 80)
    logger.info(" Scheduler Started — Running Every 30 Seconds")
    logger.info("=" * 80)

    logger.info(" Running Initial Email Check...")
    process_emails_job()

    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        logger.info(" Shutdown signal received. Stopping scheduler...")
        scheduler.shutdown()
        logger.info(" Application shutdown complete.")