"""Workflow database interface."""

from mongoengine import register_connection

from siptools_research.config import Configuration


def connect_mongoengine(config_path):
    config = Configuration(config_path)
    mongodb_host = config.get("mongodb_host")

    register_connection(
        host=f"mongodb://{mongodb_host}:27017/siptools-research",
        tz_aware=True,
        # Connect on first operation instead of instantly.
        # This is to prevent MongoClient from being created before a fork,
        # which leads to unexpected behavior.
        connect=False,
        alias="siptools_research"
    )
