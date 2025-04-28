import os
import server

bind = f"{server.HOST}:{server.PORT}"
#bind = f"0.0.0.0:8080"

workers = os.getenv('WORKERS', 2)
#worker_class = 'gthread'
worker_class = 'uvicorn.workers.UvicornWorker'
threads = os.getenv('THREADS', 1)
loglevel = 'error'
worker_tmp_dir = '/dev/shm'
