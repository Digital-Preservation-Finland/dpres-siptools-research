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
        "identifier":
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
    "identifier": "dataset_identifier",
    "data_catalog": {
        "identifier": "urn:nbn:fi:att:data-catalog-pas"
    },
    "preservation_identifier": "doi:test",
    "contract": {
        "identifier": "contract_identifier"
    },
    "research_dataset": {
        "files": [
            {
                "details": {
                    "project_identifier": "foo"
                }
            }
        ]
    },
    "preservation_state": 0
}
