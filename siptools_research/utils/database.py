"""Metadata database"""

from pymongo import MongoClient


def connection(host):
    """Return database connection for given `host:port`"""
    if not hasattr(connection, 'client_cache'):
        connection.client_cache = MongoClient(host)
    return connection.client_cache
