"""Sample Metax dataset metadata."""
import copy

import lxml.etree

BASE_DATACITE = lxml.etree.parse('tests/data/datacite_sample.xml')

BASE_PROVENANCE = {
    "title": {
        "en": "Title of provenance"
    },
    "lifecycle_event": {
        "pref_label": {
            "en": "generated"
        }
    },
    "description": None,
    "temporal": None,
    "event_outcome": None,
    "outcome_description": None,
    "is_associated_with": [],  # TODO: is this used somewhere?
}

FULL_PROVENANCE = copy.deepcopy(BASE_PROVENANCE)
FULL_PROVENANCE["temporal"] = {
        "temporal_coverage": None,  # Is this used somewhere?
        "end_date": "2014-12-31T08:19:58Z",  # Is this used somewhere?
        "start_date": "2014-01-01T08:19:58Z"
}
FULL_PROVENANCE["description"] = {"en": "Description of provenance"}
FULL_PROVENANCE["event_outcome"] = {
    "url":
    "http://uri.suomi.fi/codelist/fairdata/event_outcome/code/success",
    "pref_label": {
        "en": "Success"
    }
}
FULL_PROVENANCE["outcome_description"] = {"en": "outcome_description"}
