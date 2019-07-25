from pathlib import Path
import os.path as op
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
    assert_result_count,
    eq_,
    SkipTest,
    assert_raises
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
@with_tempfile()
def test_bare_git(origin, remote_base_path):

    remote_base_path = Path(remote_base_path)

    # This test should take a dataset and create a bare repository at the remote end from it.
    # Given, that it is placed correctly within a tree of dataset, that remote thing should then be usable as a
    # ria-remote as well as as a git-type remote

    ds = create(origin)
    populate_dataset(ds)
    ds.save()
    assert_repo_status(ds.path)

    # Use git to make sure the remote end is what git thinks a bare clone of it should look like
    bare_repo_path = remote_base_path / ds.id[:3] / ds.id[3:]
    subprocess.run(['git', 'clone', '--bare', origin, str(bare_repo_path)])

    # Now, let's have the bare repo as a git remote and use it with annex
    eq_(subprocess.run(['git', 'remote', 'add', 'bare-git', str(bare_repo_path)], cwd=origin).returncode,
        0)
    eq_(subprocess.run(['git', 'annex', 'enableremote', 'bare-git'], cwd=origin).returncode,
        0)
    eq_(subprocess.run(['git', 'annex', 'testremote', 'bare-git'], cwd=origin).returncode,
        0)
    # copy files to the remote
    ds.repo.copy_to('.', 'bare-git')
    eq_(len(ds.repo.whereis('one.txt')), 2)

    # now we can drop all content locally, reobtain it, and survive an
    # fsck
    ds.drop('.')
    ds.get('.')
    assert_status('ok', [annexjson2result(r, ds) for r in fsck(ds.repo)])

    # Since we created the remote this particular way instead of letting ria-remote create it, we need to put
    # ria-layout-version files into it. Then we should be able to also add it as a ria-remote.
    with open(str(remote_base_path / 'ria-layout-version'), 'w') as f:
        f.write('1')
    with open(str(bare_repo_path / 'ria-layout-version'), 'w') as f:
        f.write('1')

    # Now, add the ria remote:
    initexternalremote(ds.repo, 'riaremote', 'ria', config={'base-path': str(remote_base_path)})
    # fsck to make availability known
    assert_status(
        'ok',
        [annexjson2result(r, ds)
         for r in fsck(ds.repo, remote='riaremote', fast=True)])
    eq_(len(ds.repo.whereis('one.txt')), 3)

    # Now move content from git-remote to local and see it not being available via bare-git anymore
    eq_(subprocess.run(['git', 'annex', 'move', '--all', '--from=bare-git'], cwd=origin).returncode,
        0)
    # ria-remote doesn't know yet:
    eq_(len(ds.repo.whereis('one.txt')), 2)

    import pdb; pdb.set_trace()
    # But after fsck it does:
    assert_result_count([annexjson2result(r, ds) for r in fsck(ds.repo, remote='riaremote', fast=True)],
                        2,
                        status='error',
                        message='fixing location log')
    eq_(len(ds.repo.whereis('one.txt')), 1)


@with_tempfile
@with_tempfile
@with_tempfile
@with_tempfile
@with_tempfile(mkdir=True)
def test_create_as_bare(origin, remote_base_path, public, consumer, tmp_location):
    # Note/TODO: Do we need things like:
    #    git config receive.denyCurrentBranch updateInstead
    #    mv .hooks/post-update.sample hooks/post-update
    #    git update-server-info

    # Test how we build a riaremote from an existing dataset, that is a bare git repo and can be accessed as a git type
    # remote as well. This should basically outline how to publish to that kind of structure as a data store, that is
    # autoenabled, so we can publish to github/gitlab and make that storage known.

    remote_base_path = Path(remote_base_path)

    ds = create(origin)
    populate_dataset(ds)
    ds.save()
    assert_repo_status(ds.path)

    # add the ria remote:
    initexternalremote(ds.repo, 'riaremote', 'ria', config={'base-path': str(remote_base_path)})
    # pretty much any annex command that talks to that remote should now trigger the actual creation on the remote end:
    assert_status(
        'ok',
        [annexjson2result(r, ds)
         for r in fsck(ds.repo, remote='riaremote', fast=True)])

    remote_dataset_path = remote_base_path / ds.id[:3] / ds.id[3:]

    assert remote_base_path.exists()
    assert remote_dataset_path.exists()
    ds.repo.copy_to('.', 'riaremote')

    # Now, let's make the remote end a valid, bare git repository
    eq_(subprocess.run(['git', 'init', '--bare'], cwd=remote_dataset_path).returncode,
        0)
    # TODO: we might need "mv .hooks/post-update.sample hooks/post-update", "git update-server-info" as well
    # add as git remote and push everything
    eq_(subprocess.run(['git', 'remote', 'add', 'bare-git', str(remote_dataset_path)], cwd=origin).returncode,
        0)
    # Note: "--mirror" does the job for this test, while it might not be a good default some kind of
    # datalad-create-sibling. However those things need to be configurable for actual publish/creation routine anyway
    eq_(subprocess.run(['git', 'push', '--mirror', 'bare-git'], cwd=origin).returncode,
        0)

    # annex doesn't know the bare-git remote yet:
    eq_(len(ds.repo.whereis('one.txt')), 2)
    # But after enableremote and a fsck it does:
    eq_(subprocess.run(['git', 'annex', 'enableremote', 'bare-git'], cwd=origin).returncode,
        0)
    assert_status(
        'ok',
        [annexjson2result(r, ds)
         for r in fsck(ds.repo, remote='bare-git', fast=True)])
    eq_(len(ds.repo.whereis('one.txt')), 3)

    # we can drop and get again via 'bare-git' remote:
    ds.drop('.')
    eq_(len(ds.repo.whereis('one.txt')), 2)
    eq_(subprocess.run(['git', 'annex', 'get', 'one.txt', '--from', 'bare-git'], cwd=origin).returncode,
        0)
    eq_(len(ds.repo.whereis('one.txt')), 3)
    # let's get the other one from riaremote
    eq_(len(ds.repo.whereis(op.join('subdir', 'two'))), 2)
    eq_(subprocess.run(['git', 'annex', 'get', op.join('subdir', 'two'), '--from', 'riaremote'], cwd=origin).returncode,
        0)
    eq_(len(ds.repo.whereis(op.join('subdir', 'two'))), 3)

    # Now, let's try make it a data store for datasets available from elsewhere (like github or gitlab):

    raise SkipTest("NOT YET DONE")
