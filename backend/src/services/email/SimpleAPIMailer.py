import os
import requests
import logging
from concurrent.futures import ThreadPoolExecutor
import atexit
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

_email_executor = ThreadPoolExecutor(max_workers=2)

def _shutdown_mailer():
    logger.info("Gracefully flushing pending background email tasks...")
    _email_executor.shutdown(wait=True)

atexit.register(_shutdown_mailer)

class SimpleAPIMailer:

    mailer = {}

    def __init__(self):
        self.api_key = os.getenv("MAILGUN_API_KEY")
        self.domain = os.getenv("MAILGUN_DOMAIN")
        self.from_email = os.getenv("FROM_EMAIL")        
        self.api_url = f"https://api.mailgun.net/v3/{self.domain}/messages"


    def _send_sync(self, to_email: str, subject: str, html_content: str):

        if not all([self.api_key, self.domain, self.from_email]):
            logger.error("Email dispatch aborted: Missing initialization components (API key, domain, or sender).")
            return

        payload = { "from": self.from_email, "to": to_email.replace(' ',''), "subject": subject, "html": html_content }

        try:
            response = requests.post( self.api_url, auth=("api", self.api_key), data=payload, timeout=10 )
            
            if response.status_code == 200:
                logger.info(f"Email successfully delivered via API to {to_email} (ID: {response.json().get('id', 'N/A')})")
            else:
                logger.error(f"Mailgun API rejected message to {to_email}. Status: {response.status_code}. Details: {response.text}")
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Network transport error encountered while hitting Mailgun API for {to_email}: {e}")


    def _dispatch_mail(self, to_email: str, subject: str, html_content: str):
        _email_executor.submit(self._send_sync, to_email, subject, html_content)


    @staticmethod
    def send_email(
        email: str, 
        username: str, 
        html_content = '<h1>No email content was set!</h1>',
        subject = 'No subject set'
    ):

        if not('instance' in SimpleAPIMailer.mailer):
            SimpleAPIMailer.mailer['instance'] = SimpleAPIMailer()
        
        mailer: SimpleAPIMailer = SimpleAPIMailer.mailer['instance']
        to = f"{username} <{email}>"
        mailer._dispatch_mail( to_email=to, subject=subject, html_content=html_content )



if __name__ == "__main__":
    # Test email send
    SimpleAPIMailer.send_email("nakassony@gmail.com", "Nakassony Bernardo")
    time.sleep(3)