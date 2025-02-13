"""Sample Metax dataset metadata."""
import lxml.etree

BASE_DATACITE = lxml.etree.parse('tests/data/datacite_sample.xml')

# TODO: It Looks like we only need to support life-cycle-events, so
# support for preservation_events could be removed. See TPASPKT-1434.
BASE_PROVENANCE = {
    "preservation_event": {
        "pref_label": {
            "en": "creation"
        }
    },
    "lifecycle_event": None,
    "temporal": {
        "temporal_coverage": None,  # Is this used somewhere?
        "end_date": "2014-12-31T08:19:58Z",  # Is this used somewhere?
        "start_date": "2014-01-01T08:19:58Z"
    },
    "description": {
        "en": "Description of provenance"
    },
    "event_outcome": {
        "url":
        "http://uri.suomi.fi/codelist/fairdata/event_outcome/code/success",
        "pref_label": {
            "en": "Success"
        }
    },
    "outcome_description": {
        "en": "outcome_description"
    },
    "title": None,
    "is_associated_with": [],  # TODO: is this used somewhere?
}

QVAIN_PROVENANCE = {
    "title": {
        "en": "Title"
    },
    "lifecycle_event": {
        "pref_label": {
            "en": "creation"
        }
    },
    "description": None,
    "spatial": None,
    "temporal": None,
    "preservation_event": None,
    "event_outcome": None,
    "outcome_description": None,
    "is_associated_with": [],  # TODO: is this used somewhere?
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
    "data_catalog": "urn:nbn:fi:att:data-catalog-pas",
}
