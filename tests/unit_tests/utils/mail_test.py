"""Tests for :mod:`siptools_research.utils.mail` module"""
try:
    import mock
except ImportError:
    from unittest import mock
import unittest
from smtplib import SMTPSenderRefused
from siptools_research.utils import mail


class BasicMailTest(unittest.TestCase):
    """Test class for testing ``siptools_research.utils.mail`` functions"""
    message = 'Jotain potaskaa'
    sender = 'test.sender@tpas.fi'
    receiver = 'test.receiver@tpas.fi'
    subject = 'Otsikko'

    def test_sendmail_with_correct_parameter(self):
        with mock.patch('siptools_research.utils.mail.SMTP.sendmail') as mock_smtp_sendmail, \
            mock.patch('siptools_research.utils.mail.SMTP.quit') as mock_smtp_quit, \
                mock.patch('siptools_research.utils.mail.MIMEMultipart.as_string') as mock_mimemultipart_as_string:
            mock_mimemultipart_as_string.return_value = BasicMailTest.message
            mail.send(BasicMailTest.sender, BasicMailTest.receiver, BasicMailTest.subject, BasicMailTest.message)
            mock_smtp_sendmail.assert_called_once_with(BasicMailTest.sender, BasicMailTest.receiver , BasicMailTest.message)
            mock_smtp_quit.assert_called_once()

    def test_release_resource_in_error(self):
        exceptionThrown=False
        with mock.patch('siptools_research.utils.mail.SMTP.sendmail') as mock_smtp_sendmail, \
            mock.patch('siptools_research.utils.mail.SMTP.quit') as mock_smtp_quit, \
                mock.patch('siptools_research.utils.mail.MIMEMultipart.as_string') as mock_mimemultipart_as_string:
            mock_mimemultipart_as_string.return_value = BasicMailTest.message
            mock_smtp_sendmail.side_effect = SMTPSenderRefused(1, BasicMailTest.message, BasicMailTest.sender)
            try:
                mail.send(BasicMailTest.sender, BasicMailTest.receiver, BasicMailTest.subject, BasicMailTest.message)
            except SMTPSenderRefused:
                exceptionThrown = True
            assert exceptionThrown is True
            mock_smtp_sendmail.assert_called_once_with(BasicMailTest.sender, BasicMailTest.receiver , BasicMailTest.message)
            mock_smtp_quit.assert_called_once()
