import os
from enodo.version import __version__
os.system(
    "python3 setup.py sdist")
os.system(
    f"twine upload --repository pypi dist/python-enodo-{__version__}.tar.gz")
