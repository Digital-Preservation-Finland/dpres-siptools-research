import pytest

from siptools_research.utils.locale import (
    get_localized_value, get_dataset_languages
)

DATASET = {
    "testa": {
        "en": "Test in English",
        "fi": "Testi suomeksi"
    },
    "testb": {
        "und": "#1,234,567"
    },
    "research_dataset": {
        "language": [
            {
                "title": {
                    "en": "English language", "fi": "Englannin kieli"
                },
                "identifier": "http://lexvo.org/id/iso639-3/eng"
            },
            {
                "title": {
                    "en": "Finnish language", "fi": "Suomen kieli"
                },
                "identifier": "http://lexvo.org/id/iso639-3/fin"
            },
        ]
    }
}


def test_get_localized_value():
    """
    Test that correct localized value is selected depending on priority,
    or 'und' or 'zxx' is used as a fallback if nothing else matches
    """
    assert get_localized_value(
        DATASET["testa"], languages=["en", "fi"]) == "Test in English"
    assert get_localized_value(
        DATASET["testa"], languages=["fi", "en"]) == "Testi suomeksi"

    # No localized value exists for 'fi' or 'en'
    assert get_localized_value(
        DATASET["testb"], languages=["en", "fi"]) == "#1,234,567"


def test_get_dataset_languages():
    """
    Test that a list of languages are retrieved correctly from dataset
    metadata
    """
    assert get_dataset_languages(DATASET) == ["en", "fi"]

    # If research_dataset/language doesn't exist, use 'en' as a default
    assert get_dataset_languages({"research_dataset": {}}) == ["en"]
