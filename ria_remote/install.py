# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Install a dataset from a RIA store"""

__docformat__ = 'restructuredtext'


import logging
import re
from pathlib import Path
import posixpath
from datalad import cfg as dlcfg
from datalad.interface.base import (
    build_doc,
)
from datalad.interface.utils import eval_results
from datalad.support.param import Parameter
from datalad.support.constraints import (
    EnsureNone,
    EnsureStr,
)
from datalad.distribution.dataset import (
    Dataset,
    datasetmethod,
)
from datalad.distribution.clone import Clone
from .remote import RIARemote

lgr = logging.getLogger('ria_remote.install')


@build_doc
class Install(Clone):
    """
    """
    _params_ = dict(
        Clone._params_,
        source=Parameter(
            args=("source",),
            metavar='SOURCE',
            doc="""Identifier for the RIA store and the dataset ID of the target
            dataset in the format: <storename>:<dataset_id>""",
            constraints=EnsureStr() | EnsureNone()),
        ephemeral=Parameter(
            args=('-e', '--ephemeral'),
            doc="""throw away""",
            action='store_true'),

    )

    @staticmethod
    @datasetmethod(name='ria_install')
    @eval_results
    def __call__(
            source,
            path=None,
            dataset=None,
            description=None,
            reckless=False,
            alt_sources=None,
            ephemeral=False):

        src_regex = re.compile(
            r'(?P<store>.*):(?P<dsid>[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})'
        )
        src = src_regex.match(source)

        if not src:
            raise ValueError(
                'Install source specification does not match required format')
        src = src.groupdict()

        cfg = None
        if dataset:
            # require_dataset
            ds = Dataset(dataset)
            cfg = ds.config
        if not cfg:
            cfg = dlcfg

        basepath_var = 'annex.ria-remote.{store}.base-path'.format(**src)
        sshhost_var = 'annex.ria-remote.{store}.ssh-host'.format(**src)

        if basepath_var not in cfg:
            raise ValueError(
                "RIA store '{store}' is not configured, "
                "'{var}' setting is required".format(var=basepath_var, **src))

        sshhost = cfg.get(sshhost_var, None)
        if sshhost == '0':
            # support this special value that indicated a default configuration
            # of an ssh host was disabled subsequently
            sshhost = None

        # only POSIX for now
        store_dspath, _, _ = RIARemote.get_layout_locations(Path(cfg.get(basepath_var)), src['dsid'])

        # build the actual clone source url
        clone_src = '{host}{delim}{path}'.format(
            host="ssh://{}".format(sshhost) if sshhost else '',
            delim=':' if sshhost else '',
            path=str(store_dspath))

        target_ds = None
        for r in Clone.__call__(
                source=clone_src,
                path=path,
                dataset=dataset,
                description=description,
                reckless=reckless,
                alt_sources=alt_sources,
                result_filter=None,
                result_renderer='disabled',
                on_failure='stop'):
            if r.get('status', None) == 'ok' \
                    and r.get('action', None) == 'install':
                target_ds = Dataset(r['path'])
            yield r

        if not target_ds:
            # we will have seen an error already
            return

        # all config is done in a procedure that is also available for
        # execution on subdatasets
        proc_args = ['ria_post_install']
        if ephemeral:
            proc_args.append('ephemeral')
        if reckless:
            proc_args.append('reckless')
        target_ds.run_procedure(proc_args)

        #
        # TODO configure dataset to run this procedure after
        # every get/install of a subdataset
        # TODO enhance datalad to trigger it whenever it seen
        # an action:install;status:ok result
        #
