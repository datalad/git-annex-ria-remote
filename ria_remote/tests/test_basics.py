from pathlib import Path
import shutil
import subprocess
from datalad.interface.results import annexjson2result
from datalad.api import (
    create,
)
from datalad.tests.utils import (
    with_tempfile,
    assert_repo_status,
    assert_status,
    eq_,
)

from ria_remote.tests.utils import (
    initremote,
    initexternalremote,
    setup_archive_remote,
    populate_dataset,
    get_all_files,
    fsck,
)


@with_tempfile(mkdir=True)
@with_tempfile(mkdir=True)
@with_tempfile(mkdir=True)
@with_tempfile()
def test_archive_layout(path, objtree, dirremote, archivremote):
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

    # we can simply pack up the content of the directory remote into a
    # 7z archive and place it in the right location to get a functional
    # special remote
    whereis = ds.repo.whereis('one.txt')
    targetpath = Path(archivremote) / ds.id[:3] / ds.id[3:] / 'archives'
    targetpath.mkdir(parents=True)
    subprocess.run(
        ['7z', 'u', str(targetpath / 'archive.7z'), '.'],
        cwd=dirremote,
    )
    initexternalremote(ds.repo, '7z', 'ria', config={'base-path': archivremote})
    # now fsck the new remote to get the new special remote indexed
    fsck(ds.repo, remote='7z', fast=True)
    eq_(len(ds.repo.whereis('one.txt')), len(whereis) + 1)


@with_tempfile(mkdir=True)
@with_tempfile(mkdir=True)
@with_tempfile()
def test_backup_archive(path, objtree, archivremote):
    """Similar to test_archive_layout(), but not focused on
    compatibility with the directory-type special remote. Instead,
    it tests build a second RIA remote from an existing one, e.g.
    for backup purposes.
    """
    ds = create(path)
    setup_archive_remote(ds.repo, objtree)
    populate_dataset(ds)
    ds.save()
    assert_repo_status(ds.path)

    # copy files into the RIA archive
    ds.repo.copy_to('.', 'archive')

    targetpath = Path(archivremote) / ds.id[:3] / ds.id[3:]
    targetpath.mkdir(parents=True)
    subprocess.run(
        ['7z', 'u', str(targetpath / 'archive.7z'), '.'],
        cwd=str(Path(objtree) / ds.id[:3] / ds.id[3:]),
    )
    initexternalremote(ds.repo, '7z', 'ria', config={'base-path': archivremote})
    # wipe out the initial RIA remote (just for testing if the upcoming
    # one can fully take over)
    shutil.rmtree(objtree)
    # fsck to make git-annex aware of the loss
    assert_status(
        'error',
        [annexjson2result(r, ds)
         for r in fsck(ds.repo, remote='archive', fast=True)])
    # now only available "here"
    eq_(len(ds.repo.whereis('one.txt')), 1)

    # make the backup archive known
    initexternalremote(
        ds.repo, 'backup', 'ria', config={'base-path': archivremote})
    # now fsck the new remote to get the new special remote indexed
    assert_status(
        'ok',
        [annexjson2result(r, ds)
         for r in fsck(ds.repo, remote='backup', fast=True)])
    eq_(len(ds.repo.whereis('one.txt')), 2)

    # now we can drop all content locally, reobtain it, and survive an
    # fsck
    ds.drop('.')
    ds.get('.')
    assert_status('ok', [annexjson2result(r, ds) for r in fsck(ds.repo)])
