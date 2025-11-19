import os
import json
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
import google.genai as genai
from google.genai import types

logger = logging.getLogger(__name__)
class PDFAnalyzerGenAI:

    def __init__(self):
        api_key = os.getenv("GEMINI_KEY")
        if not api_key:
            raise ValueError("Missing GEMINI_KEY in environment variables")

        self.client = genai.Client(api_key=os.getenv("GEMINI_KEY"))
        self.prompt = '''Extract:
            - Company or Person name (owner of the bank statement)
            - Bank Name
            - Address
            
            Return JSON:
            {
                "owner": "",
                "bank_name": "",
                "address": ""
            }'''
    # -------------------------------------------------------
    # Save output JSON per file
    # -------------------------------------------------------


    # -------------------------------------------------------
    # Read PDF and process page-wise
    # -------------------------------------------------------
    def analyze_pdf(self, file_path: str, model="gemini-2.0-flash"):
        # Read PDF bytes
        with open(file_path, "rb") as f:
            pdf_bytes = f.read()

        # Create document part
        document_part = types.Part.from_bytes(
            mime_type="application/pdf",
            data=pdf_bytes
        )

        # Gemini config
        generation_config = types.GenerateContentConfig(
            response_mime_type="application/json",
            thinking_config=types.ThinkingConfig(thinking_budget=0)
        )

        # Call Gemini API
        response = self.client.models.generate_content(
            model=model,
            contents=[
                document_part,
                self.prompt
            ],
            config=generation_config,
        )

        # Validate
        if not response.candidates:
            logger.warning(f"No Gemini response for file: {file_path}")
            return None

        # Best candidate
        candidate = response.candidates[0]
        # Parse JSON
        try:
            # try JSON parse
            output = json.loads(candidate.content.parts[0].text)

        except Exception:
            # raw fallback
            output = {
                "raw_text": candidate.content.parts[0].text
            }
        # Save output locally

        return output



