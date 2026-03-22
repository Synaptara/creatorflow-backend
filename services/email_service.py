import os
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from firebase_admin import auth

logger = logging.getLogger(__name__)

class EmailDispatcher:
    def __init__(self):
        # We use Gmail's SMTP server
        self.smtp_server = "smtp.gmail.com"
        self.smtp_port = 587
        self.sender_email = os.getenv("EMAIL_USER")  # Your Gmail address
        self.sender_password = os.getenv("EMAIL_PASS")  # Your Gmail App Password

    def send_notification(self, user_id: str, subject: str, body_text: str):
        """Fetches the user's email from Firebase and sends them an alert."""
        if not self.sender_email or not self.sender_password:
            logger.warning("[Email] Missing EMAIL_USER or EMAIL_PASS in .env. Skipping email.")
            return

        try:
            # 1. Fetch the user's actual email address from Firebase Auth
            user_record = auth.get_user(user_id)
            recipient_email = user_record.email

            if not recipient_email:
                logger.warning(f"[Email] No email found for user '{user_id}'")
                return

            # 2. Build the email payload
            msg = MIMEMultipart()
            msg['From'] = f"CreatorFlow Alerts <{self.sender_email}>"
            msg['To'] = recipient_email
            msg['Subject'] = subject
            msg.attach(MIMEText(body_text, 'plain'))

            # 3. Fire it off via Gmail SMTP
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.sender_email, self.sender_password)
                server.send_message(msg)

            logger.info(f"[Email] ✓ Alert sent to {recipient_email}: '{subject}'")

        except Exception as e:
            logger.error(f"[Email] ✗ Failed to send email to user '{user_id}': {e}")
