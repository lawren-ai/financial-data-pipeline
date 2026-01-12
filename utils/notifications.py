import os
from slack_sdk import WebhookClient
from slack_sdk.errors import SlackApiError
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime


def send_slack_alert(message: str, severity: str = 'info'):
    """
    Send alert to slack
    Args: 
        message: Alert message
        severity: 'info', 'warning', or 'critical'
        """
    webhook_url = os.getenv('SLACK_WEBHOOK_URL')

    if not webhook_url:
        print(f"Slack webhook not configured. Message: {message}")
        return
    
    # color codes
    colors = {
        'info': '#36a64f',
        'warning': '#ff9900',
        'critical': '#ff0000'
    }

    # Emoji icons
    icons = {
        'info': ':information_source:',
        'warning': ':warning:',
        'critical': ':rotating_light:'
    }

    payload = {
        'attachments': [
            {
                "color": colors.get(severity, '#36a64f'),
                "title": f"{icons.get(severity, '')} Pipeline Alert",
                "text": message,
                "footer": "Financial Data Pipeline",
                "ts": int(datetime.now().timestamp())
            }
        ]
    }

    try:
        webhook = WebhookClient(webhook_url)
        response = webhook.send(
            text=message,
            attachments=payload['attachments']
        )

        if response.status_code != 200:
            print(f"Failed to send Slack alert: {response.status_code}")

    except SlackApiError as e:
        print(f"Slack API error: {e}")

def send_email_alert(subject: str, body: str, to_emails: list):
    """
    Send email alert
    
    Args:
        subject: Email subject
        body: Email body (can be HTML)
        to_emails: List of recipient emails
        """
    smtp_server = os.getenv('SMTP_SERVER')
    smtp_port = int(os.getenv('SMTP_PORT', 587))
    smtp_username = os.getenv('SMTP_USERNAME')
    smtp_password = os.getenv('SMTP_PASSWORD')

    if not all([smtp_server, smtp_username, smtp_password]):
        print(f"Email not configured. SUbject: {subject}")
        return
    
    msg = MIMEMultipart('alternative')
    msg['SUbject'] = f"['Pipeline Alert] {subject}"
    msg["From"] = smtp_username
    msg['To'] = ', '.join(to_emails)

    # Add HTML body
    html_body = f"""
    <html>
      <body>
        <h2>{subject}</h2>
        <p>{body}</p>
        <hr>
        <p><small>Sent from Financial Data Pipeline at {datetime.now()}</small></p>
      </body>
    </html>
"""
    msg.attach(MIMEText(html_body, 'html'))

    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_username, smtp_password)
            server.send_message(msg)
            print(f"Email sent successfully to {to_emails}")

    except Exception as e:
        print(f"Failed to send email: {e}")
    