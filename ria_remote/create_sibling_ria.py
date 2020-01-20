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

from datalad.interface.common_opts import (
    recursion_flag,
    recursion_limit
)
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
    EnsureBool
)
from datalad.distribution.dataset import (
    EnsureDataset,
    datasetmethod,
    require_dataset,
)

from datalad.utils import (
    quote_cmdlinearg
)

from datalad.support.exceptions import (
    CommandError
)
from datalad.support.gitrepo import (
    GitRepo
)
from ria_remote.remote import RIARemote

lgr = logging.getLogger('datalad.ria_remote.create_sibling_ria')


@build_doc
class CreateSiblingRia(Interface):
    """Creates a sibling to a dataset in a RIA store

    This creates a representation of a dataset in a ria-remote compliant storage location. For access to it two
    siblings are configured for the dataset by default. A "regular" one and a storage-sibling (git-annex special remote).
    Furthermore, the former is configured to have a publication dependency on the latter.

    Note, that the RIA remote needs to be configured before, referring to the name of the storage-sibling.
    That is, access to it must be available via the 'annex.ria-remote.<RIASIBLING>.base-path' and optionally
    'annex.ria-remote.<RIASIBLING>.ssh-host' configs. Please note, that RIASIBLING is the name of the storage sibling!

    The store's base path currently is expected to either:
      - not yet exist or
      - be empty or
      - have a valid `ria-layout-version` file and an `error_logs` directory.
    In the first two cases, said file and directory are created by this command. Alternatively you can manually create
    the third case, of course. Please note, that `ria-layout-version` needs to contain a line stating the version
    (currently '1') and optionally enable error logging (append '|l' in that case). Currently, this line MUST end with a
    newline!

    Error logging will create files in the `error_log` directory whenever the RIA special remote (storage sibling)
    raises an exception, storing the python traceback of it. The logfiles are named according to the scheme
    <dataset id>.<annex uuid of the remote>.log showing 'who' ran into this issue with what dataset. Since this logging
    can potentially leak personal data (like local file paths for example) it can be disabled from the client side via
    `annex.ria-remote.<RIASIBLING>.ignore-remote-config`.

    Todo
    ----
    Where to put the description of a RIA store (see below)?

    The targeted layout of such a store is a tree of datasets, starting at the configured base path. First level of
    subdirectories are named for the first three characters of the datasets' id, second level is the remainder of those
    ids. The thereby created dataset directories contain a bare git repository.
    Those bare repositories are slightly different from plain git-annex bare repositories in that they use the standard
    dirhashmixed layout beneath annex/objects as opposed to dirhashlower, which is git-annex's default for bare
    repositories. Furthermore, there is an additional directory 'archives' within the dataset directories, which may or
    may not contain archives with annexed content.
    Note, that this helps to reduce the number of inodes consumed (no checkout + potential archive) as well as it allows
    to resolve dependencies (that is (sub)datasets) merely by their id.
    Finally, there is a file `ria-layout-version` put beneath the store's base path, determining the version of the
    dataset tree layout and a file of the same name per each dataset directory determining object tree layout version
    (we already switch from dirhashlower to dirhashmixed for example) and an additional directory `error_logs` at the
    toplevel.
    """

    # TODO: option to skip existing remotes in case of recursive?

    _params_ = dict(
        dataset=Parameter(
            args=("-d", "--dataset"),
            doc="""specify the dataset to process.  If
            no dataset is given, an attempt is made to identify the dataset
            based on the current working directory""",
            constraints=EnsureDataset() | EnsureNone()),
        name=Parameter(
            args=('-s', '--name',),
            metavar='NAME',
            doc="""sibling name to create for this publication target.
                If `recursive` is set, the same name will be used to label all
                the subdatasets' siblings. When creating a target dataset fails,
                no sibling is added""",
            constraints=EnsureStr() | EnsureNone()),
        ria_sibling=Parameter(
            args=("--ria-sibling",),
            metavar="RIASIBLING",
            doc="""name of the RIA storage sibling (git-annex special remote). Must not be identical to NAME. 
            By default NAME is appended with '-ria'""",
            constraints=EnsureStr() | EnsureNone()),
        force=Parameter(
            args=("-f", "--force"),
            doc="""don't fail on existing siblings. Use and possibly reconfigure them instead.""",
            action='store_true'),
        post_update_hook=Parameter(
            args=("--post-update-hook",),
            doc="""Enable git's default post-update-hook on the remote end""",
            action="store_true"),
        shared=Parameter(
            args=("--shared",),
            metavar='{false|true|umask|group|all|world|everybody|0xxx}',
            doc="""if given, configures the access permissions on the server
        for multi-users (this could include access by a webserver!).
        Possible values for this option are identical to those of
        `git init --shared` and are described in its documentation.""",
            constraints=EnsureStr() | EnsureBool() | EnsureNone()),
        group=Parameter(
            args=("--group",),
            metavar="GROUP",
            doc="""Filesystem group for the repository. Specifying the group is
        particularly important when [CMD: --shared=group CMD][PY:
        shared="group" PY]""",
            constraints=EnsureStr() | EnsureNone()
        ),
        recursive=recursion_flag,
        recursion_limit=recursion_limit,
    )

    @staticmethod
    @datasetmethod(name='create_sibling_ria')
    @eval_results
    def __call__(name,
                 dataset=None,
                 ria_sibling=None,
                 force=False,
                 post_update_hook=False,
                 shared=None,
                 group=None,
                 recursive=False,
                 recursion_limit=None
                 ):

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

        if not ria_sibling:
            ria_sibling = "{}-ria".format(name)

        if name == ria_sibling:
            # leads to unresolvable, circular dependency with publish-depends
            raise ValueError("sibling names must not be equal")

        # TODO: messages - this is "create-sibling". Don't confuse existence of local remotes with existence of the
        #       actual remote sibling in wording
        ds_siblings = [r['name'] for r in ds.siblings(result_renderer=None)]
        if not force and name in ds_siblings:
            yield get_status_dict(
                status='error',
                message="a sibling '{}' is already configured. Use --force to overwrite it.".format(name),
                **res_kwargs,
            )
            return

        if not force and ria_sibling in ds_siblings:
            yield get_status_dict(
                status='error',
                message="a storage-sibling '{}' is already configured. Use --force to overwrite it.".format(ria_sibling),
                **res_kwargs,
            )
            return

        # check special remote config:
        # TODO: consider annexconfig the same way the special remote does (in-dataset special remote config)
        base_path = ds.config.get("annex.ria-remote.{}.base-path".format(ria_sibling), None)
        if not base_path:
            yield get_status_dict(
                status='impossible',
                message="Missing required configuration 'annex.ria-remote.{}.base-path'".format(ria_sibling),
                **res_kwargs,
            )
            return

        base_path = Path(base_path)
        ssh_host = ds.config.get("annex.ria-remote.{}.ssh-host".format(ria_sibling), None)
        if not ssh_host:
            lgr.warning("No SSH-Host configured for {}. Assume local RIA store at {}.".format(ria_sibling, base_path))
        if ssh_host == '0':
            ssh_host = None
        lgr.info("create siblings '{}' and '{}' ...".format(name, ria_sibling))

        lgr.debug('init special remote {}'.format(ria_sibling))
        ria_remote_options = ['type=external',
                              'externaltype=ria',
                              'encryption=none',
                              'autoenable=true']
        try:
            ds.repo.init_remote(ria_sibling, options=ria_remote_options)
        except CommandError as e:
            if force and e.stderr.startswith('git-annex: There is already a special remote named'):
                # run enableremote instead
                # TODO: Use AnnexRepo.enable_remote (which needs to get `options` first)
                cmd = ['git', 'annex', 'enableremote'] + ria_remote_options
                subprocess.run(cmd, cwd=quote_cmdlinearg(ds.path))
            else:
                yield get_status_dict(
                    status='error',
                    message="initremote failed.\nstdout: %s\nstderr: %s" % (e.stdout, e.stderr),
                    **res_kwargs
                )
                return

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
        ds.repo.fsck(remote=ria_sibling, fast=True, annex_options=['--exclude=*/*'])

        # 2. create a bare repository in-store:
        # determine layout locations
        repo_path, archive_path, objects_path = RIARemote.get_layout_locations(base_path, ds.id)

        lgr.debug("init bare repository")
        # TODO: we should prob. check whether it's there already. How?
        # Note: like the special remote itself, we assume local FS if no SSH host is specified
        disabled_hook = repo_path / 'hooks' / 'post-update.sample'
        enabled_hook = repo_path / 'hooks' / 'post-update'

        if group:
            chgrp_cmd = "chgrp -R {} {}".format(quote_cmdlinearg(str(group)), quote_cmdlinearg(str(repo_path)))

        if ssh_host:
            from datalad import ssh_manager
            ssh = ssh_manager.get_connection(ssh_host, use_remote_annex_bundle=False)
            ssh.open()
            ssh('cd {rootdir} && git init --bare{shared}'.format(
                rootdir=quote_cmdlinearg(str(repo_path)),
                shared=" --shared='{}'".format(quote_cmdlinearg(shared)) if shared else ''
            ))
            if post_update_hook:
                ssh('mv {} {}'.format(quote_cmdlinearg(str(disabled_hook)),
                                      quote_cmdlinearg(str(enabled_hook))))

            if group:
                # Either repository existed before or a new directory was created for it,
                # set its group to a desired one if was provided with the same chgrp
                ssh(chgrp_cmd)
        else:
            GitRepo(repo_path, create=True, bare=True,
                    shared=" --shared='{}'".format(quote_cmdlinearg(shared)) if shared else None)
            if post_update_hook:
                disabled_hook.rename(enabled_hook)
            if group:
                subprocess.run(chgrp_cmd, cwd=quote_cmdlinearg(ds.path))

        # add a git remote to the bare repository
        # Note: needs annex-ignore! Otherwise we might push into default annex/object tree instead of
        # directory type tree with dirhash lower. This in turn would be an issue, if we want to pack the entire thing
        # into an archive. Special remote will then not be able to access content in the "wrong" place within the
        # archive
        lgr.debug("set up git remote")
        # TODO: - This sibings call results in "[WARNING] Failed to determine if datastore carries annex."
        #         (see https://github.com/datalad/datalad/issues/4028)
        #         => for now have annex-ignore configured before. Evtl. Allow configure/add to include that option
        #       - additionally there's https://github.com/datalad/datalad/issues/3989, where datalad-siblings might
        #         hang forever
        ds.config.set("remote.{}.annex-ignore".format(name), value="true", where="local")
        # TODO: call siblings with fetch=False?
        ds.siblings(
            'configure',
            name=name,
            url='{}:{}'.format(ssh_host, str(repo_path))
            if ssh_host
            else str(repo_path),
            recursive=False,
            publish_depends=ria_sibling,
            result_renderer=None)

        yield get_status_dict(
            status='ok',
            **res_kwargs,
        )

        if recursive:
            # Note: subdatasets can be treated independently, so go full recursion when querying for them and _no_
            # recursion with the actual call. Theoretically this can be parallelized.
            for subds in ds.subdatasets(fulfilled=True,
                                        recursive=True,
                                        recursion_limit=recursion_limit,
                                        result_xfm='datasets'):
                yield from CreateSiblingRia.__call__(name,
                                                     dataset=subds,
                                                     ria_sibling=ria_sibling,
                                                     force=force,
                                                     post_update_hook=post_update_hook,
                                                     recursive=False)
