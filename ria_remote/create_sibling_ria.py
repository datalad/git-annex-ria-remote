# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Create a sibling in a configured RIA store"""

__docformat__ = 'restructuredtext'


import logging
import subprocess
from pathlib import Path

from datalad.interface.base import (
    Interface,
    build_doc,
)
from datalad.interface.results import (
    get_status_dict,
)
from datalad.interface.utils import eval_results
from datalad.support.param import Parameter
from datalad.support.constraints import (
    EnsureNone,
    EnsureStr,
)
from datalad.distribution.dataset import (
    EnsureDataset,
    datasetmethod,
    require_dataset,
)

from ria_remote.remote import RIARemote

lgr = logging.getLogger('datalad.ria_remote.create_sibling_ria')


@build_doc
class CreateSiblingRia(Interface):
    """Creates a sibling to a dataset in a RIA store

    This creates a representation of a dataset in a ria-remote compliant storage location. For access to it two
    siblings are configured for the dataset. A "regular" one and a storage-sibling (git-annex special remote).
    Furthermore, the former is configured to have a publication dependency on the latter.

    Note, that the RIA remote needs to be configured before, referring to the name of the storage-sibling.
    That is, access to it must be available via the 'annex.ria-remote.<STORAGE>.base-path' and optionally
    'annex.ria-remote.<STORAGE>.ssh-host' configs.
    """

    _params_ = dict(
        dataset=Parameter(
            args=("-d", "--dataset"),
            doc="""specify the dataset to process.  If
            no dataset is given, an attempt is made to identify the dataset
            based on the current working directory""",
            constraints=EnsureDataset() | EnsureNone()),
        sibling=Parameter(
            args=("sibling",),
            metavar="SIBLING",
            doc="""name of the to be created sibling""",
            constraints=EnsureStr() | EnsureNone()),
        storage_sibling=Parameter(
            args=("-s", "--storage-sibling"),
            metavar="STORAGE",
            doc="""name of the RIA storage sibling. Must not be identical to SIBLING. By default SIBLING is appended 
            with '-storage'""",
            constraints=EnsureStr() | EnsureNone()),
        force=Parameter(
            args=("-f", "--force"),
            doc="""don't fail on existing siblings. Use and possibly reconfigure them instead.""",
            action='store_true'),
        no_publish=Parameter(
            args=("--no-publish",),
            doc="""whether to publish the dataset's history (no data) to SIBLING after creation.""",
            action="store_true"),
    )

    @staticmethod
    @datasetmethod(name='create_sibling_ria')
    @eval_results
    def __call__(
            sibling,
            dataset=None,
            storage_sibling=None,
            force=False,
            no_publish=False):

        # TODO: is check_installed actually required?
        ds = require_dataset(dataset, check_installed=True, purpose='create sibling RIA')
        res_kwargs = dict(
            ds=ds,
            action="create-sibling-ria",
            logger=lgr,
        )

        if ds.repo.get_hexsha() is None or ds.id is None:
            raise RuntimeError(
                "Repository at {} is not a DataLad dataset, "
                "run 'datalad create' first.".format(ds.path))

        if not storage_sibling:
            storage_sibling = "{}-storage".format(sibling)

        if sibling == storage_sibling:
            # leads to unresolvable, circular dependency with publish-depends
            raise ValueError("sibling names must not be equal")

        # TODO: messages - this is "create-sibling". Don't confuse existence of local remotes with existence of the
        #       actual remote sibling in wording
        if not force and sibling in [r['name'] for r in ds.siblings(result_renderer=None)]:
            yield get_status_dict(
                status='error',
                message="a sibling '{}' is already configured. Use --force to overwrite it.".format(sibling),
                **res_kwargs,
            )
            return

        if not force and storage_sibling in [r['name'] for r in ds.siblings(result_renderer=None)]:
            yield get_status_dict(
                status='error',
                message="a storage-sibling '{}' is already configured. Use --force to overwrite it.".format(storage_sibling),
                **res_kwargs,
            )
            return

        # check special remote config:
        # TODO: consider annexconfig the same way the special remote does (in-dataset special remote config)
        base_path = ds.config.get("annex.ria-remote.{}.base-path".format(storage_sibling), None)
        if not base_path:
            yield get_status_dict(
                status='impossible',
                message="Missing required configuration 'annex.ria-remote.{}.base-path'".format(storage_sibling),
                **res_kwargs,
            )
            return

        base_path = Path(base_path)
        ssh_host = ds.config.get("annex.ria-remote.{}.ssh-host".format(storage_sibling), None)
        if not ssh_host:
            lgr.warning("No SSH-Host configured for {}. Assume local RIA store at {}.".format(storage_sibling, base_path))
        if ssh_host == '0':
            ssh_host = None
        lgr.info("create siblings '{}' and '{}' ...".format(sibling, storage_sibling))

        lgr.debug('init special remote {}'.format(storage_sibling))
        cmd = ['git', 'annex',
               'initremote', storage_sibling,
               'type=external',
               'externaltype=ria',
               'encryption=none',
               'autoenable=true'
               ]
        result = subprocess.run(cmd, cwd=str(ds.path), stderr=subprocess.PIPE)
        if result.returncode != 0:
            if force and result.stderr == b'git-annex: There is already a special remote named "inm7-storage".' \
                                                       b' (Use enableremote to enable an existing special remote.)\n':
                # run enableremote instead
                cmd[2] = 'enableremote'
                subprocess.run(cmd, cwd=str(ds.path))
            else:
                yield get_status_dict(
                    status='error',
                    message="initremote failed.\nstdout: %s\nstderr: %s" % (result.stdout, result.stderr),
                    **res_kwargs
                )
                return

        # determine layout locations
        repo_path, archive_path, objects_path = RIARemote.get_layout_locations(base_path, ds.id)

        # 1. create remote object store:
        # Note: All it actually takes is to trigger the special remote's `prepare` method once.
        # ATM trying to achieve that by invoking a minimal fsck.
        # TODO: - It's probably faster to actually talk to the special remote (i.e. pretending to be annex and use the
        #         protocol to send PREPARE)
        #       - Alternatively we can create the remote directory and ria version file directly, but this means code
        #         duplication that then needs to be kept in sync with ria-remote implementation.
        #       - this leads to the third option: Have that creation routine importable and callable from
        #         ria-remote package without the need to actually instantiate a RIARemote object
        lgr.debug("initializing object store")
        subprocess.run(['git', 'annex', 'fsck', '--from={}'.format(storage_sibling), '--fast', '--exclude=*/*'],
                       cwd=str(ds.path))

        # 2. create a bare repository in-store:
        lgr.debug("init bare repository")
        # TODO: we should prob. check whether it's there already. How?
        # Note: like the special remote itself, we assume local FS if no SSH host is specified
        if ssh_host:
            from datalad import ssh_manager
            ssh = ssh_manager.get_connection(ssh_host, use_remote_annex_bundle=False)
            ssh.open()
            ssh('cd {} && git init --bare'.format(repo_path))
        else:
            cmd = ['git', 'init', '--bare']
            subprocess.run(cmd, cwd=str(repo_path), check=True)

        # add a git remote to the bare repository
        # Note: needs annex-ignore! Otherwise we might push into default annex/object tree instead of
        # directory type tree with dirhash lower. This in turn would be an issue, if we want to pack the entire thing
        # into an archive. Special remote will then not be able to access content in the "wrong" place within the
        # archive
        lgr.debug("set up git remote")
        # TODO: This results in "[WARNING] Failed to determine if datastore carries annex.":
        ds.siblings(
            'configure',
            name=sibling,
            url='{}:{}'.format(ssh_host, str(repo_path))
            if ssh_host
            else str(repo_path),
            recursive=False,
            publish_depends=storage_sibling,
            result_renderer=None)

        ds.config.set("remote.{}.annex-ignore".format(sibling), value="true", where="local")
        yield get_status_dict(
            status='ok',
            **res_kwargs,
        )

        if not no_publish:
            # Publish to the git remote (without data)
            # This should prevent weird disconnected history situations
            # and give the remote end an idea who's dataset that is
            lgr.info("updating sibling {}".format(sibling))
            yield from ds.publish(to=sibling, transfer_data='none')

