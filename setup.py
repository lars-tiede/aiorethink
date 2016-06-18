from setuptools import setup, find_packages
from aiorethink import __version__


# Get the long description from the README file
from codecs import open
from os import path
here = path.abspath(path.dirname(__file__))
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()


setup(
    name = "aiorethink",
    version = __version__,
    description = "Asynchronous asyncio compatible ODM for RethinkDB",
    long_description = long_description,
    url = "https://github.com/lars-tiede/aiorethink",

    author = "Lars Tiede",
    author_email = "lars.tiede@gmail.com",

    license = "MIT",

    packages = find_packages(exclude=["docs", "tests"]),
    install_requires = ["rethinkdb", "inflection"],

    classifiers = [
        'Development Status :: 2 - Pre-Alpha',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.5',
        'Topic :: Database',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        ]
    )
