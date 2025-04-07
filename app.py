"""Simple script to run a development server"""

from research_rest_api.app import create_app

if __name__ == "__main__":
    create_app().run(debug=True, host="0.0.0.0", port=5001)
