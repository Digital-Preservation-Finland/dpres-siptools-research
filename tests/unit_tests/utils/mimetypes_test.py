"""Tests for ``siptools_research.utils.mimetypes`` module"""
import siptools_research.utils.mimetypes
import pytest


@pytest.mark.parametrize(("mimetype", "version", "expected"),
                         [("text/csv", "", True),
                          ("image/tiff", "6.0", True),
                          ("image/tiff", "6.X", False),
                          ("text/garbage", "1.0", False)])
def test_text_csv_is_supported(mimetype, version, expected, monkeypatch):
    """Test that file format ``mimetype`` with version ``version`` is supported
    """
    monkeypatch.setattr(siptools_research.utils.mimetypes,
                        "DEFAULT_CONFIG",
                        'include/etc/dpres_mimetypes.json')

    assert siptools_research.utils.mimetypes.is_supported(
        mimetype, version, "include/etc/dpres_mimetypes.json"
    ) is expected
