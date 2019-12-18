from .remote import RIARemote

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions

# defines a datalad command suite
# this symbold must be identified as a setuptools entrypoint
# to be found by datalad
command_suite = (
    # description of the command suite, displayed in cmdline help
    "Helper for the remote indexed archive (RIA) special remote",
    [
        (
            'ria_remote.export_archive',
            'ExportArchive',
            'ria-export-archive',
            'ria_export_archive'
        ),
        (
            'ria_remote.install',
            'Install',
            'ria-install',
            'ria_install'
        ),
    ]
)
