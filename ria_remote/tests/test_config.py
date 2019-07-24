from datalad.api import (
    create,
)
import shutil
from datalad.tests.utils import (
    with_tempfile,
    assert_status,
    assert_raises,
)
from datalad.support.exceptions import CommandError

from ria_remote.tests.utils import (
    initexternalremote,
    populate_dataset,
    get_all_files,
)


@with_tempfile(mkdir=True)
@with_tempfile()
@with_tempfile()
def test_site_archive_location_config(path, objtree, objtree_alt):
    ds = create(path)
    # needs base-path under all circumstances
    assert_raises(
        CommandError,
        initexternalremote,
        ds.repo, 'archive', 'ria',
        config=None,
    )
    # specify archive location via config (could also be system-wide
    # config setting, done locally here for a simple test setup)
    ds.config.set('annex.ria-remote.archive.base-path', objtree, where='local')
    initexternalremote(
        ds.repo, 'archive', 'ria',
    )
    # put some stuff in and check if it flies
    populate_dataset(ds)
    ds.save()
    ds.repo.copy_to('.', 'archive')
    arxiv_files = get_all_files(objtree)
    assert len(arxiv_files) > 1

    # now simulate a site-wide reconfiguration (here done to the
    # local git-repos config, but nothing that is committed or
    # invokes 'enableremote'
    # drop everything locally
    assert_status('ok', ds.drop('.'))
    # relocate the archive on the system
    shutil.move(objtree, objtree_alt)
    # adjust the config -- doesn't touch committed content
    ds.config.set(
        'annex.ria-remote.archive.base-path', objtree_alt, where='local')
    # remote continues to function normally after system reconfiguration
    assert_status('ok', ds.get('.'))
