"""Email send module"""

from smtplib import SMTP
from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def send(sender, receiver, subject, message, attachments=None):
    """Send email.

    :param sender: email sender
    :param receiver: email receiver
    :param subject: email subject
    :param message: email message
    :param attachments: email attachment file paths
    :returns: ``None``
    """
    if attachments is None:
        attachments = []

    msg = MIMEMultipart()
    msg['From'] = sender
    msg['To'] = receiver
    msg['Subject'] = subject
    msg.attach(MIMEText(message))

    for attachment in attachments:
        with open(attachment, "rb") as fil:
            part = MIMEApplication(
                fil.read(),
                Name=basename(attachment)
            )
        # After the file is closed
        part['Content-Disposition'] \
            = 'attachment; filename="%s"' % basename(attachment)
        msg.attach(part)

    smtp = SMTP('localhost')
    try:
        smtp.sendmail(sender, receiver, msg.as_string())
    finally:
        smtp.quit()
