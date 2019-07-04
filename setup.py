#!/usr/bin/env python

import os.path as op
from setuptools import (
    setup,
    find_packages,
)


setup(
    name="ria_remote",
    author="Michael Hanke",
    author_email="michael.hanke@gmail.com",
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
