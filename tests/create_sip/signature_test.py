"""Test the :mod:`siptools_research.create_sip.signature` module"""

import os
import shutil
import getpass
from uuid import uuid4

import pytest

from tests.assertions import task_ok

from siptools_research.target import mongo_settings

from siptools_research.create_sip.signature import check_signature


@pytest.mark.parametrize(
        'signature_filename', ['signature.sig', 'varmiste.sig'])
def test_check_signature_ok(popen_fx, testpath, signature_filename):
    """Test signature validation with valid signatures"""

    signature_path = os.path.join(testpath, signature_filename)
    open(signature_path, 'w').close()

    popen_fx.returncode = 0
    popen_fx.stdout = 'signature ok'

    (outcome, detail_note, filename) = check_signature(testpath)

    assert outcome == 'success'
    assert 'signature ok' in detail_note
    assert filename == signature_filename


@pytest.mark.parametrize(
        'signature_filename', ['signature.sig', 'varmiste.sig'])
def test_check_signature_invalid(popen_fx, testpath, signature_filename):
    """Test signature validation with invalid signatures"""

    signature_path = os.path.join(testpath, signature_filename)
    open(signature_path, 'w').close()

    popen_fx.returncode = 117
    popen_fx.stdout = 'invalid signature'

    (outcome, detail_note, filename) = check_signature(testpath)

    assert outcome == 'failure'
    assert 'invalid signature' in detail_note
    assert filename == signature_filename


def test_check_signature_error(popen_fx):
    """Test valiator failure"""

    popen_fx.returncode = 1
    popen_fx.stderr = 'error'

    with pytest.raises(OSError):
        check_signature("foobarpath")
