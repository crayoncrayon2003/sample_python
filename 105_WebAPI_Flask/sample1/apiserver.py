from connexion import FlaskApp
import os
import sys

DEFAULT_HOST = '0.0.0.0'  # bind to all available network interfaces
DEFAULT_PORT = 8080

ROOT = os.path.dirname(os.path.abspath(__file__))
SPEC_DIR = os.path.join(ROOT,"specification")
SPEC_YML = os.path.join(SPEC_DIR,"api-spec.yml")

def create_app():
    app = FlaskApp(__name__, specification_dir=SPEC_DIR)
    app.add_api(SPEC_YML, validate_responses=True)
    return app

app = create_app()

def main():
    app.run(host=DEFAULT_HOST,  port=DEFAULT_PORT)

if __name__=='__main__':
    main()
