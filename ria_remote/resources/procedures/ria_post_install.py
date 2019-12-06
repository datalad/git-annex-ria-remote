"""

"""

import logging
import sys
import os
import posixpath

from datalad.distribution.dataset import require_dataset
from datalad.utils import rmtree
from ria_remote import RIARemote


lgr = logging.getLogger('datalad.procedure.ria_post_install')


def proc_ria_remote(ds, rm, origin, special_remotes, ephemeral, reckless):
    special_remote_info = special_remotes.get(rm['annex-uuid'], None)
    # base-path must be somewhere, otherwise it could not be active
    base_path = ds.config.get(
        'annex.ria-remote.{}.base-path'.format(rm['name']), None)
    if not base_path:
        base_path = special_remote_info.get('base-path', None)
    if not base_path:
        lgr.error(
            'ria_post_install logic error: active RIA special '
            'remote without base-path setting')
        return
    dspath_in_ria, _, _ = RIARemote.get_layout_locations(base_path, ds.id)
    if not (origin_remote['url'].endswith(posixpath.join(*dspath_in_ria)) or
            origin_remote['url'] == os.path.join(*dspath_in_ria)):
        # we have no business here. neither a local, nor a remote clone
        # from this RIA store
        return

    # we don't want annex copy-to origin
    ds.config.set(
        'remote.origin.annex-ignore', 'true',
        where='local')

    ds.siblings(
        'configure',
        name='origin',
        publish_depends=rm['name'],
        result_filter=None,
        result_renderer='disabled')

    if ephemeral:
        # with ephemeral we declare 'here' as 'dead' right away, whenever
        # we symlink origins annex. Because we want annex to copy to
        # the ria remote to get availability info correct for an eventual
        # git-push into the store
        # this will cause stuff like this for a locally present annexed file:
        # % git annex whereis d1
        # whereis d1 (0 copies) failed
        # BUT this works:
        # % git annex find . --not --in here
        # % git annex find . --in here
        # d1
        ds.repo._run_annex_command('dead', annex_options=['here'])

        if reckless and origin_remote['url'] == os.path.join(*dspath_in_ria):
            # cloned from a RIA store at a local path, symlink the annex
            # to avoid needless copies in an emphemeral clone
            annex_dir = ds.repo.dot_git / 'annex'
            rmtree(str(annex_dir)) \
                if not annex_dir.is_symlink() else annex_dir.unlink()
            annex_dir.symlink_to(
                os.path.join(origin_remote['url'], 'annex'),
                target_is_directory=True)


ds = require_dataset(
    sys.argv[1],
    check_installed=True,
    purpose='Post-installation setup for datasets installed from a RIA store')

ephemeral = len(sys.argv) > 2 and 'ephemeral' in sys.argv[2:]
reckless = len(sys.argv) > 2 and 'reckless' in sys.argv[2:]

ria_remotes = [
    s for s in ds.siblings('query', result_renderer='disabled')
    if s.get('annex-externaltype', None) == 'ria'
]

if not ria_remotes:
    lg = lgr.warning if any(
        # TODO also check for base-path and compare against clone URL
        s.get('externaltype', None) == 'ria'
        for s in ds.repo.get_special_remotes().values()) else lgr.debug
    lg('No active RIA remote found')
    # we don't want to fail, this could all be valid
    sys.exit(0)

origin_remote = ds.siblings(
    'query', name='origin', return_type='item-or-list',
    result_renderer='disabled')

special_remotes = ds.repo.get_special_remotes()

for rm in ria_remotes:
    proc_ria_remote(
        ds, rm, origin_remote, special_remotes, ephemeral, reckless)
