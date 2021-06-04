"""Tests for :mod:`siptools_research.utils.mimetypes` module"""
import pytest

import siptools_research.utils.mimetypes


@pytest.mark.parametrize(("mimetype", "version", "expected"),
                         [("text/csv", "", True),
                          ("image/tiff", "6.0", True),
                          ("image/tiff", "6.X", False),
                          ("text/garbage", "1.0", False)])
def test_text_csv_is_supported(mimetype, version, expected):
    """Test that file format ``mimetype`` with version ``version`` is supported

    :param mimetype: file format mimetype
    :param version: file format version
    :param expected: expected response from `is_supported`
    :returns: ``None``
    """
    assert siptools_research.utils.mimetypes.is_supported(
        mimetype, version
    ) is expected
