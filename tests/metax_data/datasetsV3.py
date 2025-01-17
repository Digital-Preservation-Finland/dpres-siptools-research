"""Sample Metax dataset metadata."""
import lxml.etree

BASE_DATACITE = lxml.etree.parse('tests/data/datacite_sample.xml')

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
    'event_outcome': {
        "url":
        "http://uri.suomi.fi/codelist/fairdata/event_outcome/code/success",
    },
    'outcome_description': {
        "en": "outcome_description"
    }
}

QVAIN_PROVENANCE = {
    "title": {
        "en": "Title"
    },
    "lifecycle_event": {
        "pref_label": {
            "en": "creation"
        }
    }
}

BASE_DATASET = {
    "id": "dataset_identifier",
    "created": None,
    "title": None,
    "description": None,
    "modified": None,
    "fileset": {
        "csc_project": None,
        "total_files_size": 0
    },
    "preservation": {
        "state": -1,
        "description": None,
        "reason_description": None,
        "dataset_version": {
            "id": None,
            "persistent_identifier": None,
            "preservation_state": -1
        },
        "contract": "contract_identifier",
    },
    "access_rights": None,
    "version": None,
    "language": [],
    "persistent_identifier": None,
    "issued": None,
    "actors": [],
    "keyword": [],
    "theme": [],
    "spatial": [],
    "field_of_science": [],
    "provenance": [],
    "metadata_owner": None,
    "data_catalog": None
}
