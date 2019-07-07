#!/usr/bin/env python

import os.path as op
from setuptools import (
    setup,
    find_packages,
)


def get_version():
    """Load version of datalad from version.py without entailing any imports
    """
    # This might entail lots of imports which might not yet be available
    # so let's do ad-hoc parsing of the version.py
    with open(op.join(op.dirname(__file__), 'ria_remote', 'version.py')) as f:
        version_lines = list(filter(lambda x: x.startswith('__version__'), f))
    assert (len(version_lines) == 1)
    return version_lines[0].split('=')[1].strip(" '\"\t\n")


setup(
    name="ria_remote",
    author="Michael Hanke",
    author_email="michael.hanke@gmail.com",
    version=get_version(),
    description="Git-annex special remote implementation for (remote) indexed archives",
    long_description="""""",
    install_requires=[
        'datalad>=0.12.0rc4',
        'annexremote',
        'future',
    ],
    packages=find_packages(),
    scripts=[
        op.join('bin', 'git-annex-remote-ria'),
    ],
)
