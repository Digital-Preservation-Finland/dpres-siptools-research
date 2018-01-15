"""Mail interface class."""

from smtplib import SMTP
from email.mime.text import MIMEText


def send(sender, receiver, subject, message):
    msg = MIMEText(message, 'plain', 'utf-8')
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = receiver

    smtpObj = SMTP('localhost')
    try:
        smtpObj.sendmail(sender, receiver, msg.as_string())
    finally:
        smtpObj.quit()
