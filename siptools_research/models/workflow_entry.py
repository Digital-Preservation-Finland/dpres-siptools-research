"""Model for workflow."""

from mongoengine import BooleanField, Document, StringField


class WorkflowEntry(Document):
    """Worflow status document."""

    id = StringField(db_field="_id", primary_key=True)

    enabled = BooleanField(default=False)

    target = StringField()

    metadata_confirmed = BooleanField(default=False)

    proposed = BooleanField(default=False)

    meta = {
        "collection": "workflow",
        "db_alias": "siptools_research",
    }
