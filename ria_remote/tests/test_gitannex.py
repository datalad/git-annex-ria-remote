import os
import os.path as op
import mock
from datalad.api import (
    create,
)
from datalad.tests.utils import with_tempfile
from tempfile import TemporaryDirectory
import ria_remote


with TemporaryDirectory() as objdir:
    @with_tempfile
    @mock.patch.dict(
        os.environ,
        {
            'PATH': ':'.join(
                os.environ['PATH'].split(':') + [
                    op.join(
                        op.dirname(ria_remote.__file__),
                        op.pardir,
                        'bin')
                ]),
        }
    )
    def test_gitannex(path):
        ds = create(path)
        ds.repo._run_annex_command(
            'initremote',
            annex_options=[
                'ria-dummy', 'type=external',
                'externaltype=ria', 'encryption=none',
                'base-path={}'.format(path)]
        )
        ds.repo._run_annex_command(
            'testremote',
            annex_options=['ria-dummy'],
            log_stdout=False,
        )
