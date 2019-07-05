from datalad.api import (
    create,
)
from datalad.tests.utils import (
    with_tempfile,
    assert_repo_status,
    eq_,
)

from ria_remote.tests.utils import (
    initremote,
    setup_archive_remote,
    populate_dataset,
    get_all_files,
)


@with_tempfile(mkdir=True)
@with_tempfile(mkdir=True)
@with_tempfile(mkdir=True)
def test_archive_layout(path, objtree, dirremote):
    ds = create(path)
    setup_archive_remote(ds.repo, objtree)
    populate_dataset(ds)
    ds.save()
    assert_repo_status(ds.path)

    # copy files into the RIA archive
    ds.repo.copy_to('.', 'archive')

    # set up a directory-type remote for comparison
    initremote(ds.repo, 'dir', config={
        'type': 'directory',
        'directory': dirremote,
    })
    # and copy there too
    ds.repo.copy_to('.', 'dir')
    # we should see the exact same organization in both remotes
    arxiv_files = get_all_files(objtree)
    # anything went there at all?
    assert len(arxiv_files) > 1
    # minus the two layers for the archive path the content is identically
    # structured
    eq_(
        sorted([p.parts[-4:] for p in arxiv_files]),
        sorted([p.parts for p in get_all_files(dirremote)])
    )
