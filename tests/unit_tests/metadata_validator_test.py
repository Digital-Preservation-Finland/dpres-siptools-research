"""Tests for :mod:`siptools_research.metadata_validator` module"""
import copy

import pytest
import lxml.etree

from metax_access import Metax

import siptools_research
from siptools_research import validate_metadata
import siptools_research.metadata_validator as metadata_validator
from siptools_research.workflowtask import InvalidMetadataError
from siptools_research.config import Configuration
import tests.conftest

BASE_AUDIO_MD = """<mets:mets xmlns:mets="http://www.loc.gov/METS/"
xmlns:mix="http://www.loc.gov/audioMD"
xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
OBJID="38acf0b3-7edd-4a73-9dbd-ea67fd3c9c21"
PROFILE="http://www.kdk.fi/kdk-mets-profile" fi:CATALOG="1.6.0"
fi:SPECIFICATION="1.6.1"
xmlns:fi="http://www.kdk.fi/standards/mets/kdk-extensions"
xmlns:xlink="http://www.w3.org/1999/xlink">
<mets:amdSec>
  <mets:techMD CREATED="2018-01-01T07:21:27.005383" ID="123456789098765421">
   <mets:mdWrap MDTYPE="OTHER" OTHERMDTYPE="AudioMD" MDTYPEVERSION="2.0">
    <mets:xmlData>
     <amd:AUDIOMD xmlns:amd="http://www.loc.gov/audioMD/"
         ANALOGDIGITALFLAG="FileDigital">
      <amd:fileData>
       <amd:audioDataEncoding>PCM</amd:audioDataEncoding>
       <amd:bitsPerSample>8</amd:bitsPerSample>
       <amd:compression>
        <amd:codecCreatorApp>SoundForge</amd:codecCreatorApp>
        <amd:codecCreatorAppVersion>10</amd:codecCreatorAppVersion>
        <amd:codecName>(:unap)</amd:codecName>
        <amd:codecQuality>lossy</amd:codecQuality>
       </amd:compression>
       <amd:dataRate>256</amd:dataRate>
       <amd:dataRateMode>Fixed</amd:dataRateMode>
       <amd:samplingFrequency>44100</amd:samplingFrequency>
      </amd:fileData>
      <amd:audioInfo>
       <amd:duration>00:08:37.9942</amd:duration>
       <amd:numChannels>1</amd:numChannels>
      </amd:audioInfo>
     </amd:AUDIOMD>
    </mets:xmlData>
   </mets:mdWrap>
  </mets:techMD>
 </mets:amdSec>
</mets:mets>
"""

BASE_VIDEO_MD = """<mets:mets xmlns:mets="http://www.loc.gov/METS/"
xmlns:mix="http://www.loc.gov/videoMD"
xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
OBJID="38acf0b3-7edd-4a73-9dbd-ea67fd3c9c21"
PROFILE="http://www.kdk.fi/kdk-mets-profile" fi:CATALOG="1.6.0"
fi:SPECIFICATION="1.6.1"
xmlns:fi="http://www.kdk.fi/standards/mets/kdk-extensions"
xmlns:xlink="http://www.w3.org/1999/xlink">
<mets:amdSec>
  <mets:techMD CREATED="2018-01-01T07:21:27.005383" ID="123456789098765421">
   <mets:mdWrap MDTYPE="OTHER" OTHERMDTYPE="AudioMD" MDTYPEVERSION="2.0">
    <mets:xmlData>
     <vmd:VIDEOMD xmlns:vmd="http://www.loc.gov/videoMD/"
         ANALOGDIGITALFLAG="FileDigital">
      <vmd:fileData>
       <vmd:duration>01:31:37</vmd:duration>
       <vmd:dataRate>8</vmd:dataRate>
       <vmd:bitsPerSample>24</vmd:bitsPerSample>
       <vmd:color>Color</vmd:color>
       <vmd:compression>
        <vmd:codecCreatorApp>SoundForge</vmd:codecCreatorApp>
        <vmd:codecCreatorAppVersion>10</vmd:codecCreatorAppVersion>
        <vmd:codecName>(:unav)</vmd:codecName>
        <vmd:codecQuality>lossy</vmd:codecQuality>
       </vmd:compression>
       <vmd:dataRateMode>Fixed</vmd:dataRateMode>
       <vmd:frame>
        <vmd:pixelsHorizontal>640</vmd:pixelsHorizontal>
        <vmd:pixelsVertical>480</vmd:pixelsVertical>
        <vmd:PAR>1.0</vmd:PAR>
        <vmd:DAR>4/3</vmd:DAR>
       </vmd:frame>
       <vmd:frameRate>24</vmd:frameRate>
       <vmd:sampling>4:2:2</vmd:sampling>
       <vmd:signalFormat>PAL</vmd:signalFormat>
       <vmd:sound>No</vmd:sound>
      </vmd:fileData>
     </vmd:VIDEOMD>
    </mets:xmlData>
   </mets:mdWrap>
  </mets:techMD>
 </mets:amdSec>
</mets:mets>
"""

BASE_PROVENANCE = {
    "preservation_event": {
        "pref_label": {
            "en": "creation"
        }
    },
    "temporal": {
        "end_date": "2014-12-31T08:19:58Z",
        "start_date": "2014-01-01T08:19:58Z"
    },
    "description": {
        "en": "Description of provenance"
    },
    "event_outcome": {
        "pref_label": {
            "en": "success"
        }
    },
    "outcome_description": {
        "en": "This is a detail of an successful event"
    }
}

BASE_DATACITE = """<resource
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xmlns="http://datacite.org/schema/kernel-4"
        xsi:schemaLocation="http://datacite.org/schema/kernel-4
        http://schema.datacite.org/meta/kernel-4.1/metadata.xsd">
    <identifier identifierType="DOI">10.0/0</identifier>
    <creators>
        <creator>
            <creatorName>Teppo Testaaja</creatorName>
            <affiliation>Mysteeriorganisaatio</affiliation>
        </creator>
    </creators>
    <titles>
        <title xml:lang="en">Wonderful Title</title>
    </titles>
    <publisher>Teppo Testaaja</publisher>
    <publicationYear>2014</publicationYear>
    <dates>
        <date dateType="Updated">2014-01-17T08:19:58Z</date>
    </dates>
    <language>eng</language>
    <resourceType resourceTypeGeneral="Dataset"/>
    <alternateIdentifiers>
        <alternateIdentifier alternateIdentifierType="URN">
            urn:nbn:fi:att:1955e904-e3dd-4d7e-99f1-3fed446f96d1
        </alternateIdentifier>
    </alternateIdentifiers>
    <sizes>
        <size>300</size>
    </sizes>
    <descriptions>
        <description descriptionType="Abstract">
            A descriptive description describing the contents of this dataset.
            Must be descriptive.
        </description>
    </descriptions>
</resource>"""

BASE_DATASET = {
    "identifier": "dataset_identifier",
    "preservation_identifier": "doi:test",
    "contract": {
        "identifier": "contract_identifier"
    },
    "research_dataset": {
        "provenance": [
            {
                "preservation_event": {
                    "pref_label": {
                        "en": "creation"
                    }
                },
                "temporal": {
                    "end_date": "2014-12-31T08:19:58Z",
                    "start_date": "2014-01-01T08:19:58Z"
                },
                "description": {
                    "en": "Description of provenance"
                },
                'event_outcome': {
                    "pref_label": {
                        "en": "outcome"
                    }
                },
                "outcome_description": {
                    "en": "outcome_description"
                }
            }
        ],
        "files": [],
        "directories": []
    },
    "preservation_state": 0
}

BASE_CONTRACT = {
    "contract_json": {
        "title": "Testisopimus",
        "identifier": "contract_identifier",
        "organization": {
            "name": "Testiorganisaatio"
        }
    }
}

BASE_FILE = {
    "identifier": "pid:urn:identifier",
    "file_path": "path/to/file",
    "file_storage": {
        "identifier": "urn:nbn:fi:att:file-storage-ida"
    },
    "parent_directory": {
        "identifier": "pid:urn:dir:wf1"
    },
    "checksum": {
        "algorithm": "md5",
        "value": "58284d6cdd8deaffe082d063580a9df3"
    },
    "file_characteristics": {
        "file_format": "text/plain",
    }
}


def add_files_to_dataset(files, dataset):
    """Add files to dataset.
    :param: files: file identifier to be added
    :param: dataset
    :returns: ``None``
    """
    for _file in files:
        files = dataset["research_dataset"]["files"]
        files.append({
            "identifier": _file,
            "use_category": {
                "pref_label": {
                    "en": "label2"
                }
            }
        })


def add_provenance_to_dataset(lang, text, dataset):
    """Adds attributes and values to provenance.
    :param: lang: array of languages
    :param: text: array of values
    :param: dataset
    :returns: ``None``
    """
    for index, _lang in enumerate(lang):
        prov = dataset['research_dataset']['provenance'][0]
        prov['preservation_event']['pref_label'][lang[index]] = text[index]
        prov['description'][lang[index]] = text[index]
        prov['event_outcome']['pref_label'][lang[index]] = text[index]
        prov['outcome_description'][lang[index]] = text[index]


def get_bad_audiomd():
    """Creates and return invalid audio metadata xml.
    :returns: Audio MD as string
    """
    root = lxml.etree.fromstring(BASE_AUDIO_MD)
    element = root.xpath('/mets:mets/mets:amdSec/mets:techMD/mets:mdWrap/'
                         'mets:xmlData/amd:AUDIOMD/amd:audioInfo/amd:duration',
                         namespaces={'mets': "http://www.loc.gov/METS/",
                                     'amd': "http://www.loc.gov/audioMD/"})
    element[0].getparent().remove(element[0])
    return lxml.etree.tostring(root, pretty_print=True)


def get_invalid_datacite():
    """Creates and returns invalid datacite.
    :returns: Datacite as string
    """
    root = lxml.etree.fromstring(BASE_DATACITE)
    element = root.xpath('/ns:resource/ns:identifier',
                         namespaces={'ns': "http://datacite.org/schema/"
                                           "kernel-4"}
                        )
    element[0].getparent().remove(element[0])
    return lxml.etree.tostring(root, pretty_print=True)


def get_very_invalid_datacite():
    """Creates and returns very invalid datacite.
    :returns: Datacite as string
    """
    root = lxml.etree.fromstring(BASE_DATACITE)
    element = root.xpath('/ns:resource/ns:resourceType',
                         namespaces={'ns': "http://datacite.org/schema/"
                                           "kernel-4"}
                        )
    element[0].attrib['resourceTypeGeneral'] = 'INVALID_RESOURCE_TYPE'
    return lxml.etree.tostring(root, pretty_print=True)


def mock_metax_get_dir(mocker):
    """Mocks Metax get directory.
    :returns: ``None``
    """
    mocker.get(
        "https://metaksi/rest/v1/directories/pid:urn:dir:wf1",
        json={
            "identifier": "pid:urn:dir:wf1",
            "directory_path": "/access"
            }
    )


def test_validate_metadata(requests_mock):
    """Test that validate_metadata function returns ``True`` for a valid
    dataset.
    :returns: ``None``
    """
    mock_metax_get_dir(requests_mock)
    dataset = copy.deepcopy(BASE_DATASET)
    add_files_to_dataset(["pid:urn:wf_test_1a_ida", "pid:urn:wf_test_1b_ida"],
                         dataset)
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier",
                      json=dataset)
    requests_mock.get("https://metaksi/rest/v1/contracts/contract_identifier",
                      json=BASE_CONTRACT)
    requests_mock.post("https://metaksi/rpc/datasets/set_preservation_"
                       "identifier?identifier=dataset_identifier")
    file1 = copy.deepcopy(BASE_FILE)
    file1['identifier'] = "pid:urn:wf_test_1a_ida"
    file2 = copy.deepcopy(BASE_FILE)
    file2['identifier'] = "pid:urn:wf_test_1b_ida"
    files = [file1, file2]
    requests_mock.get(
        "https://metaksi/rest/v1/datasets/dataset_identifier/files",
        json=files)
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier?"
                      "dataset_format=datacite",
                      text=BASE_DATACITE)
    assert validate_metadata(
        'dataset_identifier', tests.conftest.UNIT_TEST_CONFIG_FILE)


@pytest.mark.parametrize(
    "lang,text", [(["en"], ["Something in english"]),
                  (["fi"], ["Jotain suomeksi"]),
                  (["en", "fi"], ["Something in english", "Jotain suomeksi"])]
)
# pylint: disable=invalid-name
def test_validate_metadata_languages(lang, text, monkeypatch, requests_mock):
    """Test that validate_metadata function returns ``True`` when English,
    Finnish, or both translations are provided.

    :returns: ``None``
    """
    # Datacite validation is patched to only test dataset schema validation.
    # Datacite validation is tested in test_validate_metadata.
    monkeypatch.setattr(
        metadata_validator, "_validate_datacite",
        lambda dataset_id, client: None
    )
    dataset = copy.deepcopy(BASE_DATASET)
    add_provenance_to_dataset(lang, text, dataset)
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier",
                      json=dataset)
    requests_mock.get("https://metaksi/rest/v1/contracts/contract_identifier",
                      json=BASE_CONTRACT)
    requests_mock.get(
        "https://metaksi/rest/v1/datasets/dataset_identifier/files",
        json=[])
    add_provenance_to_dataset(lang, text, dataset)
    assert validate_metadata(
        'dataset_identifier', tests.conftest.UNIT_TEST_CONFIG_FILE)


# pylint: disable=invalid-name
def test_validate_metadata_language_missing(monkeypatch, requests_mock):
    """Test that metadata validation fails if localization is missing on a
    required field.
    """
    monkeypatch.setattr(
        metadata_validator, "_validate_datacite",
        lambda dataset_id, client: None
    )
    dataset = copy.deepcopy(BASE_DATASET)
    dataset['research_dataset']['provenance'][0]['description'] = {}
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier",
                      json=dataset)
    requests_mock.get("https://metaksi/rest/v1/contracts/contract_identifier",
                      json=BASE_CONTRACT)

    with pytest.raises(InvalidMetadataError) as error:
        validate_metadata(
            'dataset_identifier',
            tests.conftest.UNIT_TEST_CONFIG_FILE
        )

    field = "'research_dataset/provenance/description'"
    assert str(error.value) == "No localization provided in field: %s" % field


# pylint: disable=invalid-name
def test_validate_metadata_invalid(requests_mock):
    """Test that validate_metadata function raises exception with correct error
    message for invalid dataset.

    :returns: ``None``
    """
    dataset = copy.deepcopy(BASE_DATASET)
    del dataset['contract']
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier",
                      json=dataset)

    # Try to validate invalid dataset
    with pytest.raises(InvalidMetadataError) as exc_info:
        validate_metadata('dataset_identifier',
                          tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Check exception message
    exc = exc_info.value
    assert str(exc).startswith("'contract' is a required property")


@pytest.mark.parametrize('format_version', ["1.0", ""])
# pylint: disable=invalid-name
def test_validate_invalid_file_type(format_version, requests_mock):
    """Test that validate_metadata function raises exception with correct error
    message for unsupported file type.

    :returns: ``None``
    """
    mock_metax_get_dir(requests_mock)
    dataset = copy.deepcopy(BASE_DATASET)
    add_files_to_dataset(["pid:urn:wf_test_1a_ida", "pid:urn:wf_test_1b_ida"],
                         dataset)
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier",
                      json=dataset)
    requests_mock.get("https://metaksi/rest/v1/contracts/contract_identifier",
                      json=BASE_CONTRACT)
    file1 = copy.deepcopy(BASE_FILE)
    file1['identifier'] = "pid:urn:wf_test_1a_ida"
    file1['file_characteristics']['file_format'] = "application/unsupported"
    if format_version:
        file1['file_characteristics']['format_version'] = "1.0"
    file2 = copy.deepcopy(BASE_FILE)
    file2['identifier'] = "pid:urn:wf_test_1b_ida"
    file2['file_characteristics']['file_format'] = "application/unsupported"
    if format_version:
        file2['file_characteristics']['format_version'] = "1.0"
    files = [file1, file2]
    requests_mock.get(
        "https://metaksi/rest/v1/datasets/dataset_identifier/files",
        json=files
    )
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier?"
                      "dataset_format=datacite",
                      text=BASE_DATACITE)
    # Try to validate dataset with a file that has an unsupported file_format
    with pytest.raises(InvalidMetadataError) as error:
        validate_metadata('dataset_identifier',
                          tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Check exception message
    message = (
        "Validation error in file path/to/file:"
        " Incorrect file format: application/unsupported"
    )
    if format_version:
        message += ", version 1.0"

    assert str(error.value) == message


# pylint: disable=invalid-name
def test_validate_metadata_invalid_contract_metadata(requests_mock):
    """Test that validate_metadata function raises exception with correct error
    message for invalid dataset.

    :returns: ``None``
    """
    dataset = copy.deepcopy(BASE_DATASET)
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier",
                      json=dataset)
    contract = copy.deepcopy(BASE_CONTRACT)
    del contract['contract_json']['organization']['name']
    requests_mock.get("https://metaksi/rest/v1/contracts/contract_identifier",
                      json=contract)
    # Try to validate invalid dataset
    with pytest.raises(InvalidMetadataError) as exc_info:
        validate_metadata(
            'dataset_identifier',
            tests.conftest.UNIT_TEST_CONFIG_FILE
        )

    # Check exception message
    assert str(exc_info.value).startswith(
        "'name' is a required property\n\nFailed validating 'required' in "
        "schema"
    )


# pylint: disable=invalid-name
def test_validate_metadata_invalid_file_path(requests_mock):
    """Test that validate_metadata function raises exception if some of the
    file paths point outside SIP.

    :returns: ``None``
    """
    mock_metax_get_dir(requests_mock)
    dataset = copy.deepcopy(BASE_DATASET)
    add_files_to_dataset(["pid:urn:invalidpath"], dataset)
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier",
                      json=dataset)
    requests_mock.get("https://metaksi/rest/v1/contracts/contract_identifier",
                      json=BASE_CONTRACT)
    file_ = copy.deepcopy(BASE_FILE)
    file_['identifier'] = "pid:urn:invalidpath"
    file_['file_path'] = "../../file_in_invalid_path"
    files = [file_]
    requests_mock.get(
        "https://metaksi/rest/v1/datasets/dataset_identifier/files",
        json=files
    )
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier?"
                      "dataset_format=datacite",
                      text=BASE_DATACITE)
    # Try to validate invalid dataset
    with pytest.raises(InvalidMetadataError) as exception_info:
        validate_metadata('dataset_identifier',
                          tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Check exception message
    assert str(exception_info.value) \
        == ("The file path of file pid:urn:invalidpath is invalid: "
            "../../file_in_invalid_path")


# pylint: disable=invalid-name
def test_validate_metadata_missing_xml(requests_mock):
    """Test that validate_metadata function raises exception if dataset
    contains image file but not XML metadata.

    :returns: ``None``
    """
    mock_metax_get_dir(requests_mock)
    dataset = copy.deepcopy(BASE_DATASET)
    add_files_to_dataset(["pid:urn:validate_metadata_test_image"], dataset)
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier",
                      json=dataset)
    requests_mock.get("https://metaksi/rest/v1/contracts/contract_identifier",
                      json=BASE_CONTRACT)
    file_ = copy.deepcopy(BASE_FILE)
    file_['identifier'] = "pid:urn:validate_metadata_test_image"
    file_['file_characteristics'] = {
        "file_created": "2018-01-17T08:19:31Z",
        "file_format": "image/tiff",
        "format_version": "6.0"
    }
    files = [file_]
    requests_mock.get(
        "https://metaksi/rest/v1/datasets/dataset_identifier/files",
        json=files)
    requests_mock.get(
        "https://metaksi/rest/v1/datasets/dataset_identifier/files",
        json=files)
    requests_mock.get("https://metaksi/rest/v1/files/"
                      "pid:urn:validate_metadata_test_image/xml",
                      json={})
    with pytest.raises(InvalidMetadataError) as exc:
        validate_metadata('dataset_identifier',
                          tests.conftest.UNIT_TEST_CONFIG_FILE)

    assert str(exc.value) == ("Missing technical metadata XML for file: "
                              "pid:urn:validate_metadata_test_image")


# pylint: disable=invalid-name
def test_validate_metadata_audiovideo(requests_mock):
    """Test that validate_metadata function validates AudioMD and VideoMD
    metadata.

    :returns: ``None``
    """
    mock_metax_get_dir(requests_mock)
    dataset = copy.deepcopy(BASE_DATASET)
    add_files_to_dataset(["pid:urn:891", "pid:urn:892"],
                         dataset)
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier",
                      json=dataset)
    requests_mock.get("https://metaksi/rest/v1/contracts/contract_identifier",
                      json=BASE_CONTRACT)
    requests_mock.post("https://metaksi/rpc/datasets/set_preservation_"
                       "identifier?identifier=dataset_identifier")
    file1 = copy.deepcopy(BASE_FILE)
    file1['identifier'] = "pid:urn:891"
    file1['file_characteristics'] = {
        "file_created": "2018-01-17T08:19:31Z",
        "file_format": "audio/mp4"
    }
    file2 = copy.deepcopy(BASE_FILE)
    file2['identifier'] = "pid:urn:892"
    file2['file_characteristics'] = {
        "file_created": "2018-01-17T08:19:31Z",
        "file_format": "video/mp4"
    }
    files = [file1, file2]
    requests_mock.get(
        "https://metaksi/rest/v1/datasets/dataset_identifier/files",
        json=files
    )
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier?"
                      "dataset_format=datacite",
                      text=BASE_DATACITE)
    requests_mock.get("https://metaksi/rest/v1/files/pid:urn:891/xml",
                      json=["http://www.loc.gov/audioMD/"])
    requests_mock.get("https://metaksi/rest/v1/files/pid:urn:891/xml?"
                      "namespace=http://www.loc.gov/audioMD/",
                      text=BASE_AUDIO_MD)
    requests_mock.get("https://metaksi/rest/v1/files/pid:urn:892/xml",
                      json=["http://www.loc.gov/videoMD/"])
    requests_mock.get("https://metaksi/rest/v1/files/pid:urn:892/xml?"
                      "namespace=http://www.loc.gov/videoMD/",
                      text=BASE_VIDEO_MD)
    assert validate_metadata(
        'dataset_identifier',
        tests.conftest.UNIT_TEST_CONFIG_FILE
    )


# pylint: disable=invalid-name
def test_validate_metadata_invalid_audiomd(requests_mock):
    """Test that validate_metadata function raises exception if AudioMD is
    invalid (missing required audiomd:duration element).

    :returns: ``None``
    """
    mock_metax_get_dir(requests_mock)
    dataset = copy.deepcopy(BASE_DATASET)
    add_files_to_dataset(["pid:urn:testaudio"],
                         dataset)
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier",
                      json=dataset)
    requests_mock.get("https://metaksi/rest/v1/contracts/contract_identifier",
                      json=BASE_CONTRACT)
    requests_mock.post("https://metaksi/rpc/datasets/set_preservation_"
                       "identifier?identifier=dataset_identifier")
    file_ = copy.deepcopy(BASE_FILE)
    file_['identifier'] = "pid:urn:testaudio"
    file_['file_characteristics'] = {
        "file_created": "2018-01-17T08:19:31Z",
        "file_format": "audio/mp4"
    }
    files = [file_]
    requests_mock.get(
        "https://metaksi/rest/v1/datasets/dataset_identifier/files",
        json=files
    )
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier?"
                      "dataset_format=datacite",
                      text=BASE_DATACITE)
    requests_mock.get("https://metaksi/rest/v1/files/pid:urn:testaudio/xml",
                      json=["http://www.loc.gov/audioMD/"])
    requests_mock.get("https://metaksi/rest/v1/files/pid:urn:testaudio/xml?"
                      "namespace=http://www.loc.gov/audioMD/",
                      content=get_bad_audiomd())
    # Try to validate invalid dataset
    with pytest.raises(InvalidMetadataError) as exc_info:
        validate_metadata('dataset_identifier',
                          tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Check exception message
    exc = exc_info.value
    assert str(exc).startswith(
        "Technical metadata XML of file pid:urn:testaudio is invalid: Element "
        "'audiomd:duration' is required in element 'amd:audioInfo'."
    )


# pylint: disable=invalid-name
def test_validate_metadata_corrupted_mix(requests_mock):
    """Test that validate_metadata function raises exception if MIX metadata in
    Metax is corrupted (invalid XML).

    :returns: ``None``
    """
    mock_metax_get_dir(requests_mock)
    dataset = copy.deepcopy(BASE_DATASET)
    add_files_to_dataset(["pid:urn:testimage"],
                         dataset)
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier",
                      json=dataset)
    requests_mock.get("https://metaksi/rest/v1/contracts/contract_identifier",
                      json=BASE_CONTRACT)
    requests_mock.post("https://metaksi/rpc/datasets/set_preservation_"
                       "identifier?identifier=dataset_identifier")
    file_ = copy.deepcopy(BASE_FILE)
    file_['identifier'] = "pid:urn:testimage"
    file_['file_characteristics'] = {
        "file_created": "2018-01-17T08:19:31Z",
        "file_format": "image/tiff",
        "format_version": "6.0"
    }
    files = [file_]
    requests_mock.get(
        "https://metaksi/rest/v1/datasets/dataset_identifier/files",
        json=files
    )
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier?"
                      "dataset_format=datacite",
                      text=BASE_DATACITE)
    requests_mock.get("https://metaksi/rest/v1/files/pid:urn:testimage/xml",
                      json=["http://www.loc.gov/mix/v20"])
    requests_mock.get("https://metaksi/rest/v1/files/pid:urn:testimage/xml?"
                      "namespace=http://www.loc.gov/mix/v20",
                      text="<mix:mix\n")
    # Try to validate invalid dataset
    with pytest.raises(InvalidMetadataError) as exc_info:
        validate_metadata(
            'dataset_identifier',
            tests.conftest.UNIT_TEST_CONFIG_FILE
        )

    # Check exception message
    exc = exc_info.value
    assert str(exc).startswith(
        'Technical metadata XML of file pid:urn:testimage is invalid: '
        'Namespace prefix mix on mix is not defined, line 2, column 1'
    )


# pylint: disable=invalid-name
def test_validate_metadata_invalid_datacite(requests_mock):
    """Test that validate_metadata function raises exception with correct error
    message for invalid datacite where required attribute identifier is
    missing.

    :returns: ``None``
    """
    mock_metax_get_dir(requests_mock)
    dataset = copy.deepcopy(BASE_DATASET)
    add_files_to_dataset(["pid:urn:wf_test_1a_ida", "pid:urn:wf_test_1b_ida"],
                         dataset)
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier",
                      json=dataset)
    requests_mock.get("https://metaksi/rest/v1/contracts/contract_identifier",
                      json=BASE_CONTRACT)
    requests_mock.post("https://metaksi/rpc/datasets/set_preservation_"
                       "identifier?identifier=dataset_identifier")
    file1 = copy.deepcopy(BASE_FILE)
    file1['identifier'] = "pid:urn:wf_test_1a_ida"
    file2 = copy.deepcopy(BASE_FILE)
    file2['identifier'] = "pid:urn:wf_test_1b_ida"
    files = [file1, file2]
    requests_mock.get(
        "https://metaksi/rest/v1/datasets/dataset_identifier/files",
        json=files
    )
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier?"
                      "dataset_format=datacite",
                      content=get_invalid_datacite())
    # Try to validate invalid dataset
    with pytest.raises(InvalidMetadataError) as exc_info:
        validate_metadata(
            'dataset_identifier',
            tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Check exception message
    exc = exc_info.value
    assert str(exc).startswith(
        "Datacite metadata is invalid: Element "
        "'{http://datacite.org/schema/kernel-4}resource': Missing child "
        "element(s)."
    )


# pylint: disable=invalid-name
def test_validate_metadata_corrupted_datacite(requests_mock):
    """Test that validate_metadata function raises exception with correct error
    message for corrupted datacite XML.

    :returns: ``None``
    """
    dataset = copy.deepcopy(BASE_DATASET)
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier",
                      json=dataset)
    requests_mock.get("https://metaksi/rest/v1/contracts/contract_identifier",
                      json=BASE_CONTRACT)
    requests_mock.post("https://metaksi/rpc/datasets/set_preservation_"
                       "identifier?identifier=dataset_identifier")
    requests_mock.get(
        "https://metaksi/rest/v1/datasets/dataset_identifier/files",
        json=[])
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier?"
                      "dataset_format=datacite",
                      text="<resource\n")
    # Try to validate invalid dataset
    with pytest.raises(InvalidMetadataError) as exc_info:
        validate_metadata(
            'dataset_identifier',
            tests.conftest.UNIT_TEST_CONFIG_FILE
        )

    # Check exception message
    exc = exc_info.value
    assert str(exc).startswith(
        "Datacite metadata is invalid: Couldn't find end of Start Tag "
        "resource line 1, line 2, column 1"
    )


# pylint: disable=invalid-name
def test_validate_metadata_publisher_missing(requests_mock):
    """Test that validate_metadata function raises exception with correct error
    message if Metax fails to generate datacite for dataset that is missing
    `publisher` attribute.

    :returns: ``None``
    """
    mock_metax_get_dir(requests_mock)
    dataset = copy.deepcopy(BASE_DATASET)
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier",
                      json=dataset)
    requests_mock.get("https://metaksi/rest/v1/contracts/contract_identifier",
                      json=BASE_CONTRACT)
    requests_mock.post("https://metaksi/rpc/datasets/set_preservation_"
                       "identifier?identifier=dataset_identifier")
    requests_mock.get(
        "https://metaksi/rest/v1/datasets/dataset_identifier/files",
        json=[]
    )
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier?"
                      "dataset_format=datacite",
                      content=get_invalid_datacite())

    # Mock datacite request response. Mocked response has status code 400, and
    # response body contains error information.
    response = \
        {
            "detail": "Dataset does not have a publisher (field: "
                      "research_dataset.publisher), which is a required value "
                      "for datacite format",
            "error_identifier": "2019-03-28T12:39:01-f0a7e3ae"
        }
    requests_mock.get(
        tests.conftest.METAX_URL + '/datasets/dataset_identifier?'
        'dataset_format=datacite',
        json=response,
        status_code=400
    )

    # Try to validate invalid dataset
    with pytest.raises(InvalidMetadataError) as exc_info:
        validate_metadata(
            'dataset_identifier',
            tests.conftest.UNIT_TEST_CONFIG_FILE
        )

    # Check exception message
    exc = exc_info.value
    assert str(exc) == (
        "Datacite metadata is invalid: Dataset does not have a publisher "
        "(field: research_dataset.publisher), which is a required value for "
        "datacite format"
    )


def test_validate_file_metadata(requests_mock):
    """Check that dataset directory caching is working correctly in
    DatasetConsistency when the files have common root directory
    in dataset.directories property.

    :returns: ``None``
    """
    dataset = copy.deepcopy(BASE_DATASET)
    dataset['research_dataset']['directories'] = [{'identifier': 'root_dir'}]
    file_1 = copy.deepcopy(BASE_FILE)
    file_1['identifier'] = 'file_identifier1'
    file_2 = copy.deepcopy(BASE_FILE)
    file_2['identifier'] = 'file_identifier2'
    requests_mock.get(
        tests.conftest.METAX_URL + '/directories/pid:urn:dir:wf1',
        json={'identifier': 'first_par_dir',
              'directory_path': '',
              'parent_directory': {
                  'identifier': 'second_par_dir'
                  }
             },
        status_code=200
    )
    requests_mock.get(
        tests.conftest.METAX_URL + '/directories/second_par_dir',
        json={'identifier': 'second_par_dir',
              'directory_path': '',
              'parent_directory': {
                  'identifier': 'root_dir'
                  }
             },
        status_code=200
    )
    requests_mock.get(
        tests.conftest.METAX_URL + '/directories/root_dir',
        json={'identifier': 'second_par_dir',
              'directory_path': '/'
             },
        status_code=200
    )
    files_adapter = requests_mock.get(
        tests.conftest.METAX_URL + '/datasets/dataset_identifier/files',
        json=[file_1, file_2],
        status_code=200
    )
    # Init metax client
    configuration = Configuration(tests.conftest.UNIT_TEST_CONFIG_FILE)
    client = Metax(
        configuration.get('metax_url'),
        configuration.get('metax_user'),
        configuration.get('metax_password'),
        verify=configuration.getboolean('metax_ssl_verification')
    )
    # pylint: disable=protected-access
    siptools_research.metadata_validator._validate_file_metadata(
        dataset,
        client, configuration
    )
    assert files_adapter.call_count == 1


# pylint: disable=invalid-name
def test_validate_file_metadata_invalid_metadata(requests_mock):
    """Check that ``_validate_file_metadata`` raises exceptions with
    descriptive error messages.

    :returns: ``None``
    """
    # Init metax client
    configuration = Configuration(tests.conftest.UNIT_TEST_CONFIG_FILE)
    client = Metax(
        configuration.get('metax_url'),
        configuration.get('metax_user'),
        configuration.get('metax_password'),
        verify=configuration.getboolean('metax_ssl_verification')
    )
    dataset = copy.deepcopy(BASE_DATASET)
    add_files_to_dataset(["pid:urn:wf_test_1a_ida_missing_file_format"],
                         dataset)
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier",
                      json=dataset)
    requests_mock.get("https://metaksi/rest/v1/contracts/contract_identifier",
                      json=BASE_CONTRACT)
    requests_mock.post("https://metaksi/rpc/datasets/set_preservation_"
                       "identifier?identifier=dataset_identifier")
    file_ = copy.deepcopy(BASE_FILE)
    file_['identifier'] = "pid:urn:wf_test_1a_ida_missing_file_format"
    file_['file_characteristics'] = {
        "file_created": "2014-01-17T08:19:31Z"
    }
    files = [file_]
    requests_mock.get(
        "https://metaksi/rest/v1/datasets/dataset_identifier/files",
        json=files)
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier?"
                      "dataset_format=datacite",
                      text=BASE_DATACITE)
    requests_mock.get("https://metaksi/rest/v1/files/pid:urn:testimage/xml",
                      json=["http://www.loc.gov/mix/v20"])
    requests_mock.get("https://metaksi/rest/v1/files/pid:urn:testimage/xml?"
                      "namespace=http://www.loc.gov/mix/v20",
                      text="<mix:mix\n")

    with pytest.raises(InvalidMetadataError) as exc_info:
        # pylint: disable=protected-access
        siptools_research.metadata_validator._validate_file_metadata(
            {
                'identifier': 'dataset_identifier'
            },
            client, configuration
        )

    assert str(exc_info.value).startswith(
        "Validation error in metadata of path/to/file: 'file_format' is"
        " a required property\n\n"
        "Failed validating 'required' in schema['properties']"
        "['file_characteristics']:\n"
    )


def test_validate_xml_file_metadata():
    """Test that _validate_xml_file_metadata function raises exception with
    readable error message when validated XML contains multiple errors.

    :returns: ``None``
    """
    xml = lxml.etree.parse('tests/data/invalid_audiomd.xml')
    # pylint: disable=protected-access
    with pytest.raises(InvalidMetadataError) as exception_info:
        metadata_validator._validate_with_schematron(
            'audio', xml, 'foo'
        )

    assert str(exception_info.value) == (
        "Technical metadata XML of file foo is invalid: The following errors "
        "were detected:\n\n"
        "1. Element 'audiomd:samplingFrequency' is required in element "
        "'amd:fileData'.\n"
        "2. Element 'audiomd:duration' is required in element 'amd:audioInfo'."
    )


def test_validate_datacite(requests_mock):
    """Test that _validate_datacite function raises exception with readable
    error message when datacite XML contains multiple errors.

    :returns: ``None``
    """

    # Init metax client
    configuration = Configuration(tests.conftest.UNIT_TEST_CONFIG_FILE)
    metax_client = Metax(
        configuration.get('metax_url'),
        configuration.get('metax_user'),
        configuration.get('metax_password'),
        verify=configuration.getboolean('metax_ssl_verification')
    )

    dataset = copy.deepcopy(BASE_DATASET)
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier",
                      json=dataset)
    requests_mock.get("https://metaksi/rest/v1/contracts/contract_identifier",
                      json=BASE_CONTRACT)
    requests_mock.post("https://metaksi/rpc/datasets/set_preservation_"
                       "identifier?identifier=dataset_identifier")
    requests_mock.get(
        "https://metaksi/rest/v1/datasets/dataset_identifier/files",
        json=[])
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier?"
                      "dataset_format=datacite",
                      content=get_very_invalid_datacite())
    # Validate datacite
    # pylint: disable=protected-access
    with pytest.raises(InvalidMetadataError) as exception_info:
        metadata_validator._validate_datacite(
            'dataset_identifier',
            metax_client
        )

    # Check error message
    assert str(exception_info.value) == (
        "Datacite metadata is invalid: The following errors were detected:\n\n"
        "1. Element '{http://datacite.org/schema/kernel-4}resourceType', "
        "attribute 'resourceTypeGeneral': [facet 'enumeration'] The value "
        "'INVALID_RESOURCE_TYPE' is not an element of the set {'Audiovisual', "
        "'Collection', 'DataPaper', 'Dataset', 'Event', 'Image', "
        "'InteractiveResource', 'Model', 'PhysicalObject', 'Service', "
        "'Software', 'Sound', 'Text', 'Workflow', 'Other'}.\n"
        "2. Element '{http://datacite.org/schema/kernel-4}resourceType', "
        "attribute 'resourceTypeGeneral': 'INVALID_RESOURCE_TYPE' is not a "
        "valid value of the atomic type "
        "'{http://datacite.org/schema/kernel-4}resourceType'."
    )


# pylint: disable=invalid-name
def test_validate_metadata_invalid_directory_metadata(requests_mock):
    """Test that validate_metadata function raises exception if directory
    metadata is not valid (directory_path attribute is missing).

    :returns: ``None``
    """
    requests_mock.get(
        "https://metaksi/rest/v1/directories/pid:urn:dir:wf1",
        json={
            "identifier": "pid:urn:dir:wf1"
            }
    )
    dataset = copy.deepcopy(BASE_DATASET)
    add_files_to_dataset(["file_id"], dataset)
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier",
                      json=dataset)
    requests_mock.get("https://metaksi/rest/v1/contracts/contract_identifier",
                      json=BASE_CONTRACT)
    file_ = copy.deepcopy(BASE_FILE)
    file_['identifier'] = "file_id"
    files = [file_]
    requests_mock.get(
        "https://metaksi/rest/v1/datasets/dataset_identifier/files",
        json=files
    )
    # Try to validate invalid dataset
    with pytest.raises(InvalidMetadataError) as exception_info:
        validate_metadata('dataset_identifier',
                          tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Check exception message
    assert str(exception_info.value).startswith(
        "Validation error in metadata of pid:urn:dir:wf1: 'directory_path' is"
        " a required property")
