"""Mongoengine connection."""

from mongoengine import register_connection


def connect_mongoengine(host, port=27017):
    """Register connection to MongoDB."""
    register_connection(
        host=f"mongodb://{host}:{port}/siptools-research",
        tz_aware=True,
        # Connect on first operation instead of instantly.
        # This is to prevent MongoClient from being created before a fork,
        # which leads to unexpected behavior.
        connect=False,
        alias="siptools_research"
    )
