import os
import sys

from gunicorn.app.base import Application
from gunicorn.config import make_settings

import server
import apiserver_gconfig
from apiserver_wsgi import application

PATH_SERVER  = os.path.dirname(os.path.abspath(__file__))
PATH_UP_LVL1 = os.path.dirname(PATH_SERVER)
PATH_UP_LVL2 = os.path.dirname(PATH_UP_LVL1)
CTRL_DIR = os.path.join(PATH_UP_LVL1,"controller")
SPEC_DIR = os.path.join(PATH_UP_LVL2,"specification")
SPEC_YML = os.path.join(SPEC_DIR,"api-spec.yml")
sys.path.append(SPEC_DIR)
sys.path.append(CTRL_DIR)

def base_config():
    gunicorn_setting_names = [k for k, v in make_settings().items()]
    server_config_vars = vars(apiserver_gconfig).items()
    # print("gunicorn_setting_names")
    # print(gunicorn_setting_names)
    # print("server_config_vars")
    # print(server_config_vars)

    temp = {
        k: v
        for k, v in server_config_vars if k in gunicorn_setting_names
    }
    # print("temp")
    # print(temp)

    return temp

class apiserver(Application):
    def init(self, parser, opts, args):
        return base_config()

    def load(self):
        return application

def run():
    gunicorn = apiserver('%(prog)s [OPTIONS] [APP_MODULE]')
    sys.argv[0] = 'application'
    sys.exit(gunicorn.run())

if __name__=='__main__':
    run()
