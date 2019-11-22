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
import os
import re
import posixpath
from datalad import cfg as dlcfg
from datalad.interface.base import (
    build_doc,
)
from datalad.interface.utils import eval_results
from datalad.support.param import Parameter
from datalad.support.constraints import (
    EnsureNone,
    EnsureBool,
    EnsureStr,
)
from datalad.distribution.dataset import (
    Dataset,
    datasetmethod,
)
from datalad.distribution.clone import Clone
from datalad.utils import rmtree

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
        store_dspath = posixpath.join(
            cfg.get(basepath_var),
            src['dsid'][:3],
            src['dsid'][3:])

        # build the actual clone source url
        clone_src = '{host}{delim}{path}'.format(
            host=sshhost if sshhost else '',
            delim=':' if sshhost else '',
            path=posixpath.join(
                cfg.get(basepath_var),
                src['dsid'][:3],
                src['dsid'][3:]))

        target_ds = None
        for r in Clone.__call__(
                source=clone_src,
                path=path,
                dataset=dataset,
                description=description,
                reckless=ephemeral,
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

        #
        # This should all be a post-processing dataset procedure
        # that can be applied to any future subdataset too
        # TODO: procedure should check if dataset ID exists in store
        # and only then perform linking
        #
        # TODO enhance datalad to trigger it whenever it seen
        # an action:install;status:ok result

        # we don't want annex copy-to origin
        target_ds.config.set(
            'remote.origin.annex-ignore', 'true',
            where='local')

        for r in target_ds.siblings(
                'configure',
                name='origin',
                publish_depends=src['store'],
                result_filter=None,
                result_renderer='disabled'):
            yield r

        linked_annex = ephemeral and not sshhost

        # the RIA store is the primary store, untrust this clone by default
        # ephemeral or not
        # with ephemeral we declare 'here' as 'dead' right away, whenever
        # we symlink origins annex. Because we want annex to copy to
        # inm7-storage to get availability info correct for an eventual
        # git-push into the store
        # this will cause stull like this for a locally present annexed file:
        # % git annex whereis d1
        # whereis d1 (0 copies) failed
        # BUT this works:
        # % git annex find . --not --in here
        # % git annex find . --in here
        # d1
        target_ds.repo._run_annex_command(
            # untrust is already implied by --reckless above, but
            # we want to make sure here
            'dead' if linked_annex else 'untrust',
            annex_options=['here'])

        if linked_annex:
            annex_dir = str(target_ds.repo.dot_git / 'annex')
            rmtree(annex_dir)
            os.symlink(posixpath.join(store_dspath, 'annex'), annex_dir)
