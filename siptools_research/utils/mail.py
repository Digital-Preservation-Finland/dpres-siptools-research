"""Mail interface class."""

from smtplib import SMTP
from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def send(sender, receiver, subject, message, attachments=None):
    msg = MIMEMultipart()
    msg['From'] = sender
    msg['To'] = receiver
    msg['Subject'] = subject
    msg.attach(MIMEText(message))
    for f in attachments or []:
        with open(f, "rb") as fil:
            part = MIMEApplication(
                fil.read(),
                Name=basename(f)
            )
        # After the file is closed
        part['Content-Disposition'] = 'attachment; filename="%s"' % basename(f)
        msg.attach(part)
    smtpObj = SMTP('localhost')
    try:
        smtpObj.sendmail(sender, receiver, msg.as_string())
    finally:
        smtpObj.quit()


def main():
    send('test.sender@tpas.fi', 'esa.bister@csc.fi', 'Postia',
         'Postia pukkaa')


if __name__ == '__main__':
    main()
