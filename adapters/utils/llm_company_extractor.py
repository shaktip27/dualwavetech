# Perplexity API
# ----------------------------------------------------------------------------------------------------------------------------------------
import os
import json
import re
import logging
import requests
import pdfplumber
from pdf2image import convert_from_path
import pytesseract

logger = logging.getLogger(__name__)

class PDFAnalyzerGenAI:

    def __init__(self):
        self.api_key = os.getenv("PPLX_KEY")
        self.url = "https://api.perplexity.ai/chat/completions"

        self.prompt = """Extract:
        - Company or Person name (owner of the bank statement)
        - Bank Name
        - Address

        Return JSON:
        {
            "owner": "",
            "bank_name": "",
            "address": ""
        }
        """

    # -------------------------------------------------------
    # Extract text from first page only (text OR scanned PDF)
    # -------------------------------------------------------
    def read_first_page_text(self, file_path: str) -> str:
        """
        Extracts text from the first page.
        Falls back to OCR automatically if text is empty or useless.
        """
        extracted_text = ""
        try:
            with pdfplumber.open(file_path) as pdf:
                first_page = pdf.pages[0]
                extracted_text = first_page.extract_text() or ""
        except Exception as e:
            logger.error(f"PDF text extraction error: {e}")

        HEADER_KEYWORDS = [
            "Account Statement",
            "Issue Date",
            "Period",
            "Account Activity",
            "Payment Type",
            "Paid In",
            "Paid Out",
            "Balance",
            "Date",
            "Detail",
        ]

        def looks_like_header_only(text: str) -> bool:
            lines = [line.strip() for line in text.split("\n") if line.strip()]
            header_count = sum(
                any(keyword.lower() in line.lower() for keyword in HEADER_KEYWORDS)
                for line in lines
            )
            return len(lines) > 0 and (header_count / len(lines)) > 0.7

        def is_valid_text(text: str) -> bool:
            if not text:
                return False
            clean = text.strip()
            if len(clean) < 30:
                return False
            if sum(c.isalpha() for c in clean) < 10:
                return False
            if looks_like_header_only(clean):
                return False
            return True

        if is_valid_text(extracted_text):
            return extracted_text

        logger.info("Text invalid or header-only → using OCR fallback")

        try:
            images = convert_from_path(file_path, first_page=1, last_page=1)
            if images:
                ocr_text = pytesseract.image_to_string(images[0]).strip()
                return ocr_text
        except Exception as e:
            logger.error(f"OCR extraction error: {e}")

        return ""

    # -------------------------------------------------------
    # Analyze PDF using Perplexity AI
    # -------------------------------------------------------
    def analyze_pdf(self, file_path: str, model="sonar-pro"):
        # Extract only first page text
        first_page_text = self.read_first_page_text(file_path)

        if not first_page_text:
            logger.warning(f"No text extracted from first page: {file_path}")

        payload = {
            "model": model,
            "temperature": 0.0,
            "messages": [
                {
                    "role": "system",
                    "content": "You are an AI that extracts structured data only."
                },
                {
                    "role": "user",
                    "content": f"{self.prompt}\n\nPDF First Page:\n{first_page_text}"
                }
            ]
        }

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        try:
            response = requests.post(self.url, headers=headers, json=payload)
            result = response.json()

            if "choices" not in result:
                return None

            text_output = result["choices"][0]["message"]["content"].strip()

            try:
                return json.loads(text_output)
            except:
                text_output = parse_llm_output(text_output)
                return text_output

        except Exception as e:
            logger.error(f"Perplexity API Error: {e}")
            return None


def parse_llm_output(text_output):
    """
    Parse LLM output and ALWAYS return a dictionary of key/value pairs.
    If JSON cannot be extracted, return an empty dict instead of raw text.
    """

    # If already a dict, return as is
    if isinstance(text_output, dict):
        return text_output

    # Must be string otherwise
    if not isinstance(text_output, str):
        return {}

    raw = text_output.strip()

    code_match = re.search(r"```(?:json)?\s*(.*?)```", raw, re.DOTALL)
    if code_match:
        cleaned = code_match.group(1).strip()
    else:
        cleaned = raw

    try:
        return json.loads(cleaned)
    except:
        pass

    json_only = re.search(r"\{.*\}", cleaned, re.DOTALL)
    if json_only:
        try:
            return json.loads(json_only.group(0))
        except:
            pass

    return {}
# ----------------------------------------------------------------------------------------------------------------------------------------------



# Gemini LLM Integration
# -----------------------------------------------------------------------------------------------------------------------------------------------
# import os
# import json
# import re
# import logging
# from dotenv import load_dotenv
# from PyPDF2 import PdfReader

# # Load environment variables
# load_dotenv()

# import google.genai as genai
# from google.genai import types

# logger = logging.getLogger(__name__)

# class PDFAnalyzerGenAI:

#     def __init__(self):
#         api_key = os.getenv("GEMINI_KEY")
#         if not api_key:
#             raise ValueError("Missing GEMINI_KEY in environment variables")

#         self.client = genai.Client(api_key=api_key)

#         self.prompt = '''Extract:
#             - Company or Person name (owner of the bank statement)
#             - Bank Name
#             - Address
            
#             Return JSON:
#             {
#                 "owner": "",
#                 "bank_name": "",
#                 "address": ""
#             }'''

#     # -------------------------------------------------------
#     # Read ONLY first page of PDF and return bytes
#     # -------------------------------------------------------
#     def get_first_page_bytes(self, file_path: str) -> bytes:
#         try:
#             reader = PdfReader(file_path)

#             if len(reader.pages) == 0:
#                 raise ValueError("PDF has no pages")

#             first_page = reader.pages[0]

#             # Create a new PDF containing ONLY first page
#             from PyPDF2 import PdfWriter
#             writer = PdfWriter()
#             writer.add_page(first_page)

#             output_path = "/tmp/_first_page.pdf"
#             with open(output_path, "wb") as f:
#                 writer.write(f)

#             with open(output_path, "rb") as f:
#                 return f.read()

#         except Exception as e:
#             logger.error(f"Error extracting first page: {e}")
#             raise e

#     # -------------------------------------------------------
#     # Analyze First Page using Gemini
#     # -------------------------------------------------------
#     def analyze_pdf(self, file_path: str, model="gemini-2.0-flash"):

#         # Extract ONLY first-page bytes
#         pdf_bytes = self.get_first_page_bytes(file_path)

#         # Create document part for Gemini
#         document_part = types.Part.from_bytes(
#             mime_type="application/pdf",
#             data=pdf_bytes
#         )

#         # Gemini config
#         generation_config = types.GenerateContentConfig(
#             response_mime_type="application/json",
#             thinking_config=types.ThinkingConfig(thinking_budget=0)
#         )

#         # Call Gemini API
#         response = self.client.models.generate_content(
#             model=model,
#             contents=[
#                 document_part,
#                 self.prompt
#             ],
#             config=generation_config,
#         )

#         # Validate
#         if not response.candidates:
#             logger.warning(f"No Gemini response for file: {file_path}")
#             return None

#         # Best candidate
#         candidate = response.candidates[0]

#         # Parse JSON
#         try:
#             output = json.loads(candidate.content.parts[0].text)[0]
#         except Exception:
#             output = parse_llm_output(candidate.content.parts[0].text)
#             print(type(output), "type")

#         return output


# def parse_llm_output(text_output):
#     """
#     Parse LLM output and ALWAYS return a dictionary of key/value pairs.
#     If JSON cannot be extracted, return an empty dict instead of raw text.
#     """

#     # If already a dict, return as is
#     if isinstance(text_output, dict):
#         return text_output

#     # Must be string otherwise
#     if not isinstance(text_output, str):
#         return {}

#     raw = text_output.strip()

#     # -----------------------------------------
#     # 1. Extract JSON inside ```json ... ```
#     # -----------------------------------------
#     code_match = re.search(r"```(?:json)?\s*(.*?)```", raw, re.DOTALL)
#     if code_match:
#         cleaned = code_match.group(1).strip()
#     else:
#         cleaned = raw

#     # -----------------------------------------
#     # 2. Attempt to parse JSON directly
#     # -----------------------------------------
#     try:
#         return json.loads(cleaned)
#     except:
#         pass

#     # -----------------------------------------
#     # 3. Extract JSON object using regex
#     # -----------------------------------------
#     json_only = re.search(r"\{.*\}", cleaned, re.DOTALL)
#     if json_only:
#         try:
#             return json.loads(json_only.group(0))
#         except:
#             pass

#     # -----------------------------------------
#     # 4. Final fallback → return EMPTY dict
#     # -----------------------------------------
#     return {}
# -----------------------------------------------------------------------------------------------------------------------------------------
