# import imapclient
# import pyzmail
# import os
# from email.header import decode_header
# from imapclient.exceptions import LoginError, IMAPClientError
# from dotenv import load_dotenv
#
# load_dotenv()
#
# class IMAPConnectionError(Exception):
#     """Custom exception raised for IMAP connection or login failures."""
#     pass
#
# class IMAPEmailAdapter:
#     def __init__(self, host, port, username, password, folder, filters):
#         self.host = host
#         self.port = port
#         self.username = username
#         self.password = password
#         self.folder = folder
#         self.filters = filters
#         self.server = None
#
#     def connect(self,ssl):
#         """Connects, logs in, and selects the folder. Includes robust error handling."""
#         try:
#             # Add a timeout for better network control
#             self.server = imapclient.IMAPClient(self.host, ssl=True, ssl_context=ssl, timeout=30)
#             self.server.login(self.username, self.password)
#             self.server.select_folder(self.folder)
#
#         except LoginError as e:
#             # Handle specific authentication failures
#             raise IMAPConnectionError(f"IMAP Login failed for user {self.username}: {e}")
#         except IMAPClientError as e:
#             # Handle server-level or general connection failures
#             raise IMAPConnectionError(f"IMAP Connection failed to {self.host}:{self.port}: {e}")
#         except Exception as e:
#             # Catch all other unexpected errors during connection setup
#             raise IMAPConnectionError(f"An unknown error occurred during IMAP connection: {e}")
#
#     def disconnect(self):
#         """Logs out and closes the connection to the IMAP server."""
#         if self.server:
#             try:
#                 self.server.logout()
#             except Exception as e:
#                 pass
#             finally:
#                 self.server = None
#
#     def fetch_emails(self):
#         # Search all unseen (new) messages
#         messages = self.server.search(['UNSEEN'])
#         # messages = self.server.search(['SINCE', '01-Nov-2025'])
#         for msgid, data in self.server.fetch(messages, ['RFC822']).items():
#             msg = pyzmail.PyzMessage.factory(data[b'RFC822'])
#             sender = msg.get_addresses('from')[0][1]
#             subject = msg.get_subject()
#
#             if not self.is_relevant(sender, subject):
#                 continue
#
#             attachments = []
#             for part in msg.mailparts:
#                 if part.filename:
#                     filename = part.filename
#                     payload = part.get_payload()
#                     filepath = os.path.join("downloads", filename)
#                     os.makedirs("downloads", exist_ok=True)
#                     with open(filepath, "wb") as f:
#                         f.write(payload)
#                     attachments.append(filepath)
#
#             yield {
#                 "message_id": msgid,
#                 "sender": sender,
#                 "subject": subject,
#                 "attachments": attachments
#             }
#
#     def is_relevant(self, sender, subject):
#         # Filter logic
#         if any(keyword.lower() in subject.lower() for keyword in self.filters["subject_keywords"]):
#             return True
#         if any(sender.endswith(domain) for domain in self.filters["sender_domains"]):
#             return True
#         return False


# import imapclient
# import pyzmail
# import os
# import ssl
# from email.header import decode_header
# from imapclient.exceptions import LoginError, IMAPClientError
# from dotenv import load_dotenv
#
# load_dotenv()
#
#
# class IMAPConnectionError(Exception):
#     """Custom exception raised for IMAP connection or login failures."""
#     pass
#
#
# class IMAPEmailAdapter:
#     # Added 'use_ssl' to the constructor
#     def __init__(self, host, port, username, password, folder, filters, use_ssl=True):
#         self.host = host
#         self.port = port
#         self.username = username
#         self.password = password
#         self.folder = folder
#         self.filters = filters
#         self.use_ssl = use_ssl  # Controls whether to use implicit SSL/TLS
#         self.server = None
#
#     # Made 'ssl_context' optional in connect method
#     def connect(self, ssl_context=None):
#         """Connects, logs in, and selects the folder. Includes robust error handling."""
#         try:
#             # Dynamically set ssl based on the adapter's configuration (self.use_ssl)
#             self.server = imapclient.IMAPClient(
#                 self.host,
#                 port=self.port,
#                 ssl=self.use_ssl,  # Use implicit SSL if configured
#                 ssl_context=ssl_context,
#                 timeout=30
#             )
#
#             # If not using implicit SSL (e.g., connected on port 143), try STARTTLS
#             if not self.use_ssl and self.server.has_capability('STARTTLS'):
#                 try:
#                     self.server.starttls(ssl_context=ssl_context)
#                 except Exception as e:
#                     # Log but continue if STARTTLS fails/is unnecessary
#                     pass
#
#             self.server.login(self.username, self.password)
#             self.server.select_folder(self.folder)
#
#         except LoginError as e:
#             # Handle specific authentication failures
#             raise IMAPConnectionError(f"IMAP Login failed for user {self.username}: {e}")
#         except IMAPClientError as e:
#             # Handle server-level or general connection failures
#             raise IMAPConnectionError(f"IMAP Connection failed to {self.host}:{self.port}: {e}")
#         except Exception as e:
#             # Catch all other unexpected errors during connection setup
#             raise IMAPConnectionError(f"An unknown error occurred during IMAP connection: {e}")
#
#     def disconnect(self):
#         """Logs out and closes the connection to the IMAP server."""
#         if self.server:
#             try:
#                 self.server.logout()
#             except Exception as e:
#                 pass
#             finally:
#                 self.server = None
#
#     def fetch_emails(self):
#         """Searches for UNSEEN messages, fetches, processes attachments, and yields data."""
#         messages = self.server.search(['UNSEEN'])
#
#         if not messages:
#             return
#
#         for msgid, data in self.server.fetch(messages, ['RFC822']).items():
#             if b'RFC822' not in data:
#                 continue
#
#             msg = pyzmail.PyzMessage.factory(data[b'RFC822'])
#
#             from_addresses = msg.get_addresses('from')
#             sender = from_addresses[0][1] if from_addresses else "unknown@example.com"
#
#             subject = msg.get_subject()
#
#             if not self.is_relevant(sender, subject):
#                 continue
#
#             attachments = []
#             for part in msg.mailparts:
#                 if part.filename:
#                     filename = part.filename
#
#                     try:
#                         payload = part.get_payload()
#                     except Exception:
#                         continue
#
#                     filepath = os.path.join("downloads", filename)
#                     os.makedirs("downloads", exist_ok=True)
#
#                     if isinstance(payload, bytes):
#                         with open(filepath, "wb") as f:
#                             f.write(payload)
#                         attachments.append(filepath)
#
#             yield {
#                 "message_id": msgid,
#                 "sender": sender,
#                 "subject": subject,
#                 "attachments": attachments
#             }
#
#     def is_relevant(self, sender, subject):
#         """Checks if the email is relevant based on defined filters."""
#         # Check for subject keywords
#         if self.filters.get("subject_keywords"):
#             if any(keyword.lower() in subject.lower() for keyword in self.filters["subject_keywords"]):
#                 return True
#
#         # Check for sender domains
#         if self.filters.get("sender_domains"):
#             if any(sender.lower().endswith(domain.lower()) for domain in self.filters["sender_domains"]):
#                 return True
#
#         return False


import imapclient
import pyzmail
import os
import ssl
from email.header import decode_header
from adapters.outlook_auth_utility import get_outlook_credentials
from imapclient.exceptions import LoginError, IMAPClientError
from dotenv import load_dotenv
# Import the custom OAuth utility to get credentials

load_dotenv()


class IMAPConnectionError(Exception):
    pass


class IMAPEmailAdapter:
    """IMAP client with OAuth2 authentication.
    Handles email retrieval and processing with secure authentication.
    """
    
    def __init__(self, host, port, folder, filters, use_ssl=True):
        """Set up IMAP client configuration.
        Initializes connection parameters and filter settings.
        """

        self.host = host
        self.port = port
        self.folder = folder
        self.filters = filters or {}
        self.use_ssl = use_ssl
        self.server = None
        self.username = None  # Will be set after OAuth authentication

    def connect(self, ssl_context=None):
        """Establish secure connection to IMAP server.
        Handles OAuth2 authentication and server handshake.
        """
        # 1. Get OAuth Credentials (This is the interactive part)
        auth_data = get_outlook_credentials()

        if not auth_data:
            # get_outlook_credentials prints the failure reason
            raise IMAPConnectionError("Failed to retrieve Outlook OAuth credentials.")

        # Unpack the username and access token
        self.username, access_token = auth_data

        try:
            # 2. Establish connection to IMAP server
            self.server = imapclient.IMAPClient(
                self.host,
                port=self.port,
                ssl=self.use_ssl,
                ssl_context=ssl_context,
                timeout=30
            )

            # Optional: Try STARTTLS if not using implicit SSL
            if not self.use_ssl and self.server.has_capability('STARTTLS'):
                try:
                    self.server.starttls(ssl_context=ssl_context)
                except Exception:
                    pass

            # 3. Authenticate using the access token
            # Note: imapclient handles the SASL XOAUTH2 formatting internally
            self.server.oauth2_login(self.username, access_token)

            # 4. Select the target folder
            self.server.select_folder(self.folder)

        except LoginError as e:
            raise IMAPConnectionError(f"IMAP Login failed for user {self.username} (XOAUTH2): {e}")
        except IMAPClientError as e:
            raise IMAPConnectionError(f"IMAP Connection failed to {self.host}:{self.port}: {e}")
        except Exception as e:
            raise IMAPConnectionError(f"An unknown error occurred during IMAP connection: {e}")

    def disconnect(self):
        """Safely close the IMAP connection.
        Ensures proper cleanup of resources.
        """
        if self.server:
            try:
                self.server.logout()
            except Exception as e:
                # Log the error but don't raise, as this is typically called in cleanup
                pass
            finally:
                self.server = None

    def fetch_emails(self):
        """Retrieve and process unread emails.
        Yields email data with attachments and metadata.
        """
        # Search all unseen (new) messages
        messages = self.server.search(['UNSEEN'])

        if not messages:
            return

        for msgid, data in self.server.fetch(messages, ['RFC822']).items():
            if b'RFC822' not in data:
                continue

            msg = pyzmail.PyzMessage.factory(data[b'RFC822'])

            from_addresses = msg.get_addresses('from')
            sender = from_addresses[0][1] if from_addresses else "unknown@example.com"

            subject = msg.get_subject()

            if not self.is_relevant(sender, subject):
                continue

            attachments = []
            for part in msg.mailparts:
                if part.filename:
                    filename = part.filename

                    try:
                        payload = part.get_payload()
                    except Exception:
                        continue

                    filepath = os.path.join("downloads", filename)
                    os.makedirs("downloads", exist_ok=True)

                    if isinstance(payload, bytes):
                        with open(filepath, "wb") as f:
                            f.write(payload)
                        attachments.append(filepath)

            yield {
                "message_id": msgid,
                "sender": sender,
                "subject": subject,
                "attachments": attachments
            }

    def is_relevant(self, sender, subject):
        """Check if email matches filter criteria.
        Returns True if email passes subject or sender filters.
        """
        if not self.filters:
            return True  # No filters means all emails are relevant

        # Normalize inputs for case-insensitive comparison
        subject_lower = (subject or '').lower()
        sender_lower = (sender or '').lower()
        
        # Check subject keywords
        if self.filters.get("subject_keywords"):
            if any(keyword.lower() in subject_lower 
                  for keyword in self.filters["subject_keywords"] 
                  if keyword):
                return True

        # Check sender domains
        if self.filters.get("sender_domains"):
            if any(domain.lower() and sender_lower.endswith(domain.lower()) 
                  for domain in self.filters["sender_domains"] 
                  if domain):
                return True

        return False