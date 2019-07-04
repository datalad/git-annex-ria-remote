from datalad.api import (
    create,
)
from datalad.tests.utils import with_tempfile
from tempfile import TemporaryDirectory


with TemporaryDirectory() as objdir:
    @with_tempfile
    def test_gitannex_localio(path):
        ds = create(path)
        ds.repo._run_annex_command(
            'initremote',
            annex_options=[
                'ria-local', 'type=external',
                'externaltype=ria', 'encryption=none',
                'base-path={}'.format(path)]
        )
        ds.repo._run_annex_command(
            'testremote',
            annex_options=['ria-local'],
            log_stdout=False,
        )


with TemporaryDirectory() as objdir:
    @with_tempfile
    def test_gitannex_remoteio(path):
        ds = create(path)
        ds.repo._run_annex_command(
            'initremote',
            annex_options=[
                'ria-remote', 'type=external',
                'externaltype=ria', 'encryption=none',
                'ssh-host=datalad-test',
                'base-path={}'.format(path)]
        )
        ds.repo._run_annex_command(
            'testremote',
            annex_options=['ria-remote'],
            log_stdout=False,
        )
