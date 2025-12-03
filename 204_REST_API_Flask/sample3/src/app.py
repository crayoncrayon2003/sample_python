from enum import Enum
import server.apiserver_wsgi as flask
import server.apiserver_gunicorn as gunicorn

class E_MODE(Enum):
    FLASK = 1
    GUNICORN = 2

#MODE = E_MODE.FLASK
MODE = E_MODE.GUNICORN

if __name__ == '__main__':
    if MODE==E_MODE.FLASK:
        flask.run()
    elif MODE==E_MODE.GUNICORN:
        gunicorn.run()
    else:
        raise
