"""Model for workflow."""
from __future__ import annotations

from mongoengine import BooleanField, Document, StringField


class WorkflowEntry(Document):
    """Workflow status document."""

    id = StringField(db_field="_id", primary_key=True)

    paused: bool = BooleanField(default=False)

    target: str | None = StringField()

    metadata_confirmed: bool = BooleanField(default=False)

    proposed: bool = BooleanField(default=False)

    meta = {
        "collection": "workflow",
        "db_alias": "siptools_research",
    }
