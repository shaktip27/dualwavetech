from fpdf import FPDF
import os
from datetime import datetime
import html2text
from adapters.utils.logger import get_logger

logger = get_logger("pdf_generator")

def generate_email_pdf(email_data: dict, output_dir: str) -> str:
    """
    Generates a PDF for the email content with full details.
    Returns the PDF file path.
    """
    try:
        pdf = FPDF()
        pdf.add_page()
        pdf.set_auto_page_break(auto=True, margin=15)
        pdf.set_font("Arial", "B", 14)
        pdf.cell(0, 10, "Email Summary", ln=True)

        pdf.set_font("Arial", "", 12)
        pdf.ln(5)

        # Basic Info
        pdf.cell(0, 8, f"From: {email_data.get('sender', 'N/A')}", ln=True)

        recipients = email_data.get('to', []) or email_data.get('recipients', [])
        if isinstance(recipients, list):
            recipients_str = ', '.join(recipients)
        else:
            recipients_str = str(recipients)
        pdf.cell(0, 8, f"To: {recipients_str}", ln=True)

        pdf.cell(0, 8, f"Subject: {email_data.get('subject', 'N/A')}", ln=True)

        # Date
        date_str = email_data.get('date') or email_data.get('receivedDateTime') or 'N/A'
        if date_str != 'N/A':
            try:
                dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                date_str = dt.strftime('%Y-%m-%d %H:%M:%S')
            except Exception:
                pass
        pdf.cell(0, 8, f"Date: {date_str}", ln=True)
        pdf.ln(5)

        # Body
        body = email_data.get('body', '') or email_data.get('body_preview', '')
        if body.startswith("<"):
            body = html2text.html2text(body)
        pdf.multi_cell(0, 8, f"Body:\n{body}")

        # Attachments
        attachments = email_data.get('attachments', [])
        if attachments:
            pdf.ln(5)
            pdf.cell(0, 8, "Attachments:", ln=True)
            for att in attachments:
                att_name = os.path.basename(att) if isinstance(att, str) else str(att)
                pdf.cell(0, 8, f"- {att_name}", ln=True)

        # ------------------- UNIQUE PDF FILE NAME -------------------
        subject_safe = email_data.get('subject', 'email')[:50].replace('/', '_')
        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        pdf_file_name = f"{subject_safe}_{timestamp}.pdf"

        os.makedirs(output_dir, exist_ok=True)
        pdf_path = os.path.join(output_dir, pdf_file_name)

        pdf.output(pdf_path)

        logger.info(f"✅ PDF generated successfully: {pdf_path}")
        return pdf_path

    except Exception as e:
        logger.error(f"Error generating email PDF: {e}", exc_info=True)
        raise Exception(f"Failed to generate email PDF: {e}")