import os
import zipfile
from adapters.utils.logger import get_logger

logger = get_logger("zip_handler")

class ZipHandler:

    @staticmethod
    def is_zip_file(file_path: str) -> bool:
        """
        Check if a file is a valid ZIP file.

        Args:
            file_path (str): Path to the file to check

        Returns:
            bool: True if file is a valid ZIP, False otherwise
        """
        try:
            return zipfile.is_zipfile(file_path)
        except Exception as e:
            logger.error(f"Error checking if {file_path} is a ZIP file: {e}")
            return False

    @staticmethod
    def extract_zip(zip_path: str, extract_to: str) -> list:
        """
        Extract all files from a ZIP archive, excluding MacOS metadata files.

        Args:
            zip_path (str): Path to the ZIP file
            extract_to (str): Directory to extract files to

        Returns:
            list: List of extracted file paths (excluding system/metadata files)
        """
        extracted_files = []

        try:
            os.makedirs(extract_to, exist_ok=True)

            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # Get list of files in the ZIP
                file_list = zip_ref.namelist()
                logger.info(f"ZIP contains {len(file_list)} files: {file_list}")

                # Extract all files
                zip_ref.extractall(extract_to)
                logger.info(f"Extracted ZIP to: {extract_to}")

                # Build full paths of extracted files, filtering out system files
                for file_name in file_list:
                    # Skip directories
                    if file_name.endswith('/'):
                        logger.debug(f"Skipped directory: {file_name}")
                        continue

                    # CRITICAL FIX: Skip MacOS hidden files and metadata
                    # Check for __MACOSX folder or ._ prefix
                    if '__MACOSX' in file_name:
                        logger.info(f"Skipped MacOS metadata folder file: {file_name}")
                        continue

                    # Check if basename starts with ._
                    basename = os.path.basename(file_name)
                    if basename.startswith('._'):
                        logger.info(f"Skipped MacOS metadata file: {file_name}")
                        continue

                    # Skip other common system files
                    if basename in ['.DS_Store', 'Thumbs.db', 'desktop.ini']:
                        logger.info(f"Skipped system file: {file_name}")
                        continue

                    full_path = os.path.join(extract_to, file_name)
                    if os.path.exists(full_path):
                        extracted_files.append(full_path)
                        logger.info(f"Extracted: {file_name}")

                logger.info(
                    f"Successfully extracted {len(extracted_files)} valid files from ZIP (excluded {len(file_list) - len(extracted_files)} system/metadata files)")

        except zipfile.BadZipFile:
            logger.error(f"Bad ZIP file: {zip_path}")
        except Exception as e:
            logger.error(f"Error extracting ZIP file {zip_path}: {e}")

        return extracted_files

    @staticmethod
    def get_supported_files(file_paths: list) -> list:
        """
        Filter files to only include supported document types.

        Args:
            file_paths (list): List of file paths to filter

        Returns:
            list: Filtered list containing only supported file types
        """
        supported_extensions = ['.pdf', '.doc', '.docx', '.csv', '.xls', '.xlsx']
        supported_files = []

        for file_path in file_paths:
            # Additional check: skip if basename starts with ._ (in case it slipped through)
            basename = os.path.basename(file_path)
            if basename.startswith('._'):
                logger.warning(f"Skipping MacOS metadata file: {basename}")
                continue

            _, ext = os.path.splitext(file_path)
            if ext.lower() in supported_extensions:
                supported_files.append(file_path)
                logger.info(f"Supported file: {basename}")
            else:
                logger.warning(f"Unsupported file type skipped: {basename} ({ext})")

        return supported_files