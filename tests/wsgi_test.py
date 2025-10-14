import importlib

def test_application(monkeypatch):
    """Test that wsgi module defines WSGI application.

    The wsgi module should provide function with name "application",
    which is the default function that the uWSGI Python loader will
    search for.
    """
    monkeypatch.setenv("SIPTOOLS_RESEARCH_CONF", "include/etc/siptools_research.conf")
    wsgi = importlib.import_module("siptools_research.wsgi")
    assert callable(wsgi.application)
