#!/usr/bin/env python

from setuptools import setup

import versioneer

long_description = """\

pg8000
------

pg8000 is a Pure-Python interface to the PostgreSQL database engine.  It is \
one of many PostgreSQL interfaces for the Python programming language. pg8000 \
is somewhat distinctive in that it is written entirely in Python and does not \
rely on any external libraries (such as a compiled python module, or \
PostgreSQL's libpq library). pg8000 supports the standard Python DB-API \
version 2.0.

pg8000's name comes from the belief that it is probably about the 8000th \
PostgreSQL interface for Python."""

cmdclass = dict(versioneer.get_cmdclass())
version = versioneer.get_version()

setup(
    name="pg8000",
    version=version,
    cmdclass=cmdclass,
    description="PostgreSQL interface library",
    long_description=long_description,
    author="Mathieu Fenniak",
    author_email="biziqe@mathieu.fenniak.net",
    url="https://github.com/tlocke/pg8000",
    license="BSD",
    python_requires='>=3.5',
    install_requires=['scramp==1.2.0'],
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: Implementation",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: Jython",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Operating System :: OS Independent",
        "Topic :: Database :: Front-Ends",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    keywords="postgresql dbapi",
    packages=("pg8000",)
)
