"""Model for workflow."""

from mongoengine import BooleanField, Document, StringField


class WorkflowEntry(Document):
    """Worflow status document."""

    id = StringField(db_field="_id", primary_key=True)

    enabled: bool = BooleanField(default=False)

    target = StringField()

    metadata_confirmed: bool = BooleanField(default=False)

    proposed: bool = BooleanField(default=False)

    meta = {
        "collection": "workflow",
        "db_alias": "siptools_research",
    }
