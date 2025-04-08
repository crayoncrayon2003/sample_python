from setuptools import setup, find_packages

setup(
    name='myapp',                       # packages name. this is displayed in "pip list"
    version="0.0.1",                    # packages version
    description="this is description",  # packages description
    author='anonymous',                 # packages author
    packages=find_packages(),           # the list of modules to use
    license='MIT'                       # packages license
)