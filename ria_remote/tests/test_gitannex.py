from datalad.api import (
    create,
)
from datalad.tests.utils import with_tempfile
from tempfile import TemporaryDirectory

from ria_remote.tests.utils import (
    skip_ssh,
)


@with_tempfile(mkdir=True)
@with_tempfile(mkdir=True)
def test_gitannex_localio(path, objtree):
    ds = create(path)
    ds.repo._run_annex_command(
        'initremote',
        annex_options=[
            'ria-local', 'type=external',
            'externaltype=ria', 'encryption=none',
            'base-path={}'.format(objtree)]
    )
    ds.repo._run_annex_command(
        'testremote',
        annex_options=['ria-local'],
        log_stdout=False,
    )


@skip_ssh
@with_tempfile(mkdir=True)
@with_tempfile(mkdir=True)
def test_gitannex_remoteio(path, objtree):
    ds = create(path)
    ds.repo._run_annex_command(
        'initremote',
        annex_options=[
            'ria-remote', 'type=external',
            'externaltype=ria', 'encryption=none',
            'ssh-host=datalad-test',
            'base-path={}'.format(objtree)]
    )
    ds.repo._run_annex_command(
        'testremote',
        annex_options=['ria-remote'],
        log_stdout=False,
    )
