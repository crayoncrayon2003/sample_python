import os
import sys
import server
from flask import request
from connexion import FlaskApp
from connexion.resolver import RestyResolver

PATH_SERVER  = os.path.dirname(os.path.abspath(__file__))
PATH_SRC     = os.path.dirname(PATH_SERVER)
PATH_ROOT    = os.path.dirname(PATH_SRC)
CTRL_DIR = os.path.join(PATH_SRC ,"controller")
SPEC_DIR = os.path.join(PATH_ROOT,"specification")
SPEC_YML = os.path.join(SPEC_DIR,"api-spec.yml")

sys.path.append(PATH_SERVER)
sys.path.append(CTRL_DIR)
sys.path.append(SPEC_DIR)

def create_app():
    app = FlaskApp(__name__, specification_dir=SPEC_DIR)
    app.add_api(SPEC_YML, resolver=RestyResolver(CTRL_DIR), validate_responses=True)
    app.debug = True

    @app.app.before_request
    def log_request_info():
        print(f"Handling request: {request.method} {request.url}")

    return app

apiserver = create_app()
application = apiserver

def run():
    apiserver.run(host=server.HOST,  port=server.PORT)

if __name__=='__main__':
    run()
