"""Tests for ``siptools_research.validate_metadata`` module"""

import pytest
import tests.conftest
from siptools_research import validate_metadata
from siptools_research.workflowtask import InvalidMetadataError


@pytest.mark.usefixtures('testmetax')
def test_validate_metadata():
    """Test that validate_metadata function returns ``True`` for a valid
    dataset.
    """
    assert validate_metadata('validate_metadata_test_dataset',
                             tests.conftest.UNIT_TEST_CONFIG_FILE) is True


@pytest.mark.usefixtures('testmetax')
def test_validate_metadata_invalid():
    """Test that validate_metadata function raises exception with correct error
    message for invalid dataset.
    """
    # Try to validate invalid dataset
    with pytest.raises(InvalidMetadataError) as exc_info:
        validate_metadata('validate_metadata_test_dataset_invalid_metadata',
                          tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Check exception message
    exc = exc_info.value
    assert exc.message.startswith("'contract' is a required property")


@pytest.mark.usefixtures('testmetax')
def test_validate_metadata_missing_xml():
    """Test that validate_metadata function raises exception if dataset
    contains image file but not XML metadata.
    """
    with pytest.raises(InvalidMetadataError) as exc:
        validate_metadata('validate_metadata_test_dataset_metadata_missing',
                          tests.conftest.UNIT_TEST_CONFIG_FILE)

    assert exc.value[0] == \
        "Missing XML metadata for file: pid:urn:validate_metadata_test_image"


@pytest.mark.usefixtures('testmetax')
def test_validate_metadata_audiovideo():
    """Test that validate_metadata function validates AudioMD and VideoMD
    metadata.
    """
    assert validate_metadata(
        'validate_metadata_test_dataset_audio_video_metadata',
        tests.conftest.UNIT_TEST_CONFIG_FILE) is True


@pytest.mark.usefixtures('testmetax')
def test_validate_metadata_invalid_datacite():
    """Test that validate_metadata function raises exception with correct error
    message for invalid datacite where required attribute identifier is
    missing.
    """
    # Try to validate invalid dataset
    with pytest.raises(InvalidMetadataError) as exc_info:
        validate_metadata('validate_metadata_test_dataset_invalid_datacite',
                          tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Check exception message
    exc = exc_info.value
    assert exc.message.startswith('Datacite (id=validate_metadata_test_data' +
                                  'set_invalid_datacite) validation failed')
