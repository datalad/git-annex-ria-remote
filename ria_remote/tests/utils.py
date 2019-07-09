import os
import inspect
from glob import glob
from pathlib import Path
from functools import wraps
from six import iteritems

from datalad.tests.utils import (
    create_tree,
)

from nose import SkipTest
from nose.plugins.attrib import attr

example_payload = {
    'one.txt': 'content1',
    'subdir': {
        'two': 'content2',
    },
}


def get_all_files(path):
    return sorted([
        Path(p).relative_to(path)
        for p in glob(str(Path(path) / '**'), recursive=True)
        if not Path(p).is_dir()
    ])


# TODO think about migrating to AnnexRepo
def fsck(repo, paths=None, remote=None, fast=False, incremental=False,
         limit=None, annex_options=None, git_options=None):
    """
    Parameters
    ----------
    paths : list
    remote : str
    fast : bool
    incremental : bool or {'continue'} or SCHEDULE
    limit : str or all
    """
    args = [] if annex_options is None else list(annex_options)
    if fast:
        args.append('--fast')
    if limit:
        # looks funky, but really is a test if the `all` function was passed
        # alternatives would have been 1) a dedicated argument (would need
        # a check for mutual exclusivity with --branch), or 2) a str-type
        # special values that has no meaning in Git and is less confusing
        if limit is all:
            args.append('--all')
        else:
            args.append('--branch={}'.format(limit))
    if remote:
        args.append('--from={}'.format(remote))
    if incremental == 'continue':
        args.append('--more')
    elif incremental:
        args.append('--incremental')
        if not (incremental is True):
            args.append('--incremental-schedule={}'.format(incremental))
    return repo._run_annex_command_json(
        'fsck',
        files=paths,
        git_options=git_options,
        opts=args,
        # next two are to avoid warnings and errors due to an fsck
        # finding any issues -- those will be reported in the
        # JSON results, and can be acted upon
        expect_stderr=True,
        expect_fail=True,
    )


# TODO think about migrating to AnnexRepo
def initremote(repo, name, encryption=None, config=None):
    cfg = dict(config) if config else {}
    cfg['encryption'] = encryption if encryption else 'none'
    args = [name]
    args += ['{}={}'.format(k, v) for k, v in iteritems(cfg)]
    repo._run_annex_command(
        'initremote',
        annex_options=args,
    )


def initexternalremote(repo, name, type, encryption=None, config=None):
    config = dict(
        config if config else {},
        type='external',
        externaltype=type,
    )
    return initremote(repo, name, encryption=encryption, config=config)


def setup_archive_remote(repo, archive_path):
    cfg = {
        'base-path': archive_path,
    }
    if 'RIA_TESTS_SSH' in os.environ:
        cfg['ssh-host'] = 'datalad-test'
    initexternalremote(repo, 'archive', 'ria', config=cfg)


def populate_dataset(ds):
    create_tree(ds.path, example_payload)


def check_not_generatorfunction(func):
    """Internal helper to verify that we are not decorating generator tests"""
    if inspect.isgeneratorfunction(func):
        raise RuntimeError("{}: must not be decorated, is a generator test"
                           .format(func.__name__))


def skip_ssh(func):
    """Skips SSH-based tests if environment variable RIA_TESTS_SSH was not set
    """

    check_not_generatorfunction(func)

    @wraps(func)
    @attr('skip_ssh')
    def newfunc(*args, **kwargs):
        if 'RIA_TESTS_SSH' not in os.environ:
            raise SkipTest("Disabled, set RIA_TESTS_SSH to run")
        return func(*args, **kwargs)
    return newfunc


