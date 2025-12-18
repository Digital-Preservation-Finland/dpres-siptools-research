"""Mongoengine connection."""
from __future__ import annotations

from mongoengine import register_connection


def connect_mongoengine(host: str, port: int | str = 27017) -> None:
    """Register connection to MongoDB.

    :param host: MongoDB host
    :param port: MongoDB port
    """
    register_connection(
        host=f"mongodb://{host}:{port}/siptools-research",
        tz_aware=True,
        # Connect on first operation instead of instantly.
        # This is to prevent MongoClient from being created before a fork,
        # which leads to unexpected behavior.
        connect=False,
        alias="siptools_research",
        uuidRepresentation="standard"  # Added to avoid DeprecationWarnings
    )
