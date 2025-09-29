"""Module that allows deployment using WSGI"""

import siptools_research.app

application = siptools_research.app.create_app()
