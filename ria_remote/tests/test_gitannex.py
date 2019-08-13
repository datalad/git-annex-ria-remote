from datalad.api import (
    create,
)
from datalad.tests.utils import with_tempfile

from ria_remote.tests.utils import (
    skip_ssh,
    initexternalremote,
)


@with_tempfile(mkdir=True)
@with_tempfile()
def test_gitannex_localio(path, objtree):
    ds = create(path)
    initexternalremote(
        ds.repo, 'ria-local', 'ria',
        config={'base-path': objtree})
    ds.repo._run_annex_command(
        'testremote',
        annex_options=['ria-local'],
        log_stdout=False,
    )


@skip_ssh
@with_tempfile(mkdir=True)
@with_tempfile()
def test_gitannex_remoteio(path, objtree):
    ds = create(path)
    initexternalremote(
        ds.repo, 'ria-remote', 'ria',
        config={
            'base-path': objtree,
            'ssh-host': 'datalad-test',
        })
    ds.repo._run_annex_command(
        'testremote',
        annex_options=['ria-remote'],
        log_stdout=False,
    )
