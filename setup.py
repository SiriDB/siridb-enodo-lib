
"""
Upload to PyPI
python3 setup.py sdist
twine upload --repository pypitest dist/python-enodo-X.X.X.tar.gz
twine upload --repository pypi dist/python-enodo-X.X.X.tar.gz
"""
from setuptools import setup, find_packages
from enodo import __version__
try:
    with open('README.md', 'r') as f:
        long_description = f.read()
except IOError:
    long_description = '''
    The Endodo HUB connector can be used to communicate with the SiriDB Enodo HUB.
    '''.strip()

setup(
    name='python-enodo',
    version=__version__,
    description='SiriDB Enodo Connector',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/SiriDB/siridb-enodo-lib',
    author='Timo Janssen',
    author_email='timo@transceptor.technology',
    license='GPLv3',
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 3 - Alpha',

        # Indicate who your project is intended for
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',

        # Pick your license as you wish (should match "license" above)
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],

    # What does your project relate to?
    keywords='data communication connector enodo siridb library',

    # You can just specify the packages manually here if your project is
    # simple. Or you can use find_packages().
    packages=find_packages(exclude=['docs', 'tests*']),

    # List additional groups of dependencies here (e.g. development
    # dependencies). You can install these using the following syntax,
    # for example:
    # $ pip install -e .[dev,test]
    # extras_require={
    #     'dev': ['check-manifest'],
    #     'test': ['coverage'],
    # },
)