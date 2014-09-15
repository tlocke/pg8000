#!/usr/bin/env python

import versioneer
versioneer.VCS = 'git'
versioneer.versionfile_source = 'pg8000/_version.py'
versioneer.versionfile_build = 'pg8000/_version.py'
versioneer.tag_prefix = ''
versioneer.parentdir_prefix = 'pg8000-'
from setuptools import setup

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

try:
    from sphinx.setup_command import BuildDoc
    cmdclass['build_sphinx'] = BuildDoc
except ImportError:
    pass

version=versioneer.get_version()

setup(
        name="pg8000",
        version=version,
        cmdclass=cmdclass,
        description="PostgreSQL interface library",
        long_description=long_description,
        author="Mathieu Fenniak",
        author_email="biziqe@mathieu.fenniak.net",
        url="https://github.com/mfenniak/pg8000",
        license="https://github.com/mfenniak/pg8000",
        classifiers = [
            "Development Status :: 4 - Beta",
            "Intended Audience :: Developers",
            "License :: OSI Approved :: BSD License",
            "Programming Language :: Python",
            "Programming Language :: Python :: 2",
            "Programming Language :: Python :: 2.5",
            "Programming Language :: Python :: 2.6",
            "Programming Language :: Python :: 2.7",
            "Programming Language :: Python :: 3",
            "Programming Language :: Python :: 3.2",
            "Programming Language :: Python :: 3.3",
            "Programming Language :: Python :: 3.4",
            "Programming Language :: Python :: Implementation",
            "Programming Language :: Python :: Implementation :: CPython",
            "Programming Language :: Python :: Implementation :: Jython",
            "Programming Language :: Python :: Implementation :: PyPy",
            "Operating System :: OS Independent",
            "Topic :: Database :: Front-Ends",
            "Topic :: Software Development :: Libraries :: Python Modules",
        ],
        keywords="postgresql dbapi",
        packages = ("pg8000",),
        command_options={
            'build_sphinx': {
                'version': ('setup.py', version),
                'release': ('setup.py', version)}},
    )
