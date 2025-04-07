"""Module that allows deployment using WSGI"""

import research_rest_api.app

application = research_rest_api.app.create_app()
