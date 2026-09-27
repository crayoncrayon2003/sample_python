$ python -m venv env
$ source env/bin/activate
(env) $ pip install --upgrade pip setuptools
(env) $ pip install pypdf reportlab fonttools otf2ttf
(env) $ python add_watermark.py a.pdf b.pdf
