from annexremote import SpecialRemote
from annexremote import RemoteError

import os.path as op
from six import (
    text_type,
)
from pathlib import(
    Path,
    PosixPath,
)
import shutil
import tempfile
from shlex import quote as sh_quote
import subprocess

import logging
lgr = logging.getLogger('ria_remote')

# TODO
# - make archive check optional


def _get_gitcfg(gitdir, key, cfgargs=None):
    cmd = [
        'git',
        '--git-dir', gitdir,
        'config',
    ]
    if cfgargs:
        cmd += cfgargs
    cmd += ['--get', key]
    try:
        return subprocess.check_output(
            cmd,
            # yield text
            universal_newlines=True)
    except Exception:
        lgr.debug(
            "Failed to obtain config '%s' at %s",
            key, gitdir,
        )
        return None


def _get_datalad_id(gitdir):
    """Attempt to determine a DataLad dataset ID for a given repo

    Returns
    -------
    str or None
      None in case no ID was found
    """
    dsid = _get_gitcfg(
        gitdir, 'datalad.dataset.id', ['--blob', ':.datalad/config']
    )
    if dsid is None:
        lgr.debug(
            "Cannot determine a DataLad ID for repository: %s",
            gitdir,
        )
    else:
        dsid = dsid.strip()
    return dsid


class IOBase(object):
    """Abstract class with the desired API for local/remote operations"""
    def mkdir(self, path):
        raise NotImplementedError

    def put(self, src, dst):
        raise NotImplementedError

    def get(self, src, dst):
        raise NotImplementedError

    def rename(self, src, dst):
        raise NotImplementedError

    def remove(self, path):
        raise NotImplementedError

    def exists(self, path):
        raise NotImplementedError

    def get_from_archive(self, archive, src, dst):
        """Get a file from an archive

        Parameters
        ----------
        archive_path : Path or str
          Must be an absolute path and point to an existing supported archive
        file_path : Path or str
          Must be a relative Path (relative to the root
          of the archive)
        """
        raise NotImplementedError

    def in_archive(self, archive_path, file_path):
        """Test whether a file is in an archive

        Parameters
        ----------
        archive_path : Path or str
          Must be an absolute path and point to an existing supported archive
        file_path : Path or str
          Must be a relative Path (relative to the root
          of the archive)
        """
        raise NotImplementedError


class LocalIO(IOBase):
    """IO operation if the object tree is local (e.g. NFS-mounted)"""
    def mkdir(self, path):
        path.mkdir(
            parents=True,
            exist_ok=True,
        )

    def put(self, src, dst):
        shutil.copy(
            str(src),
            str(dst),
        )

    def get(self, src, dst):
        shutil.copy(
            str(src),
            str(dst),
        )

    def get_from_archive(self, archive, src, dst):
        # this requires python 3.5
        with open(dst, 'wb') as target_file:
            subprocess.run([
                '7z', 'x', '-so',
                text_type(archive), text_type(src)],
                stdout=target_file,
            )

    def rename(self, src, dst):
        src.rename(dst)

    def remove(self, path):
        path.unlink()

    def remove_dir(self, path):
        path.rmdir()

    def exists(self, path):
        return path.exists()

    def in_archive(self, archive_path, file_path):
        loc = text_type(file_path)
        from datalad.cmd import Runner
        runner = Runner()
        # query 7z for the specific object location, keeps the output
        # lean, even for big archives
        out, err = runner(
            ['7z', 'l', text_type(archive_path),
             loc],
            log_stdout=True,
        )
        return loc in out


class NoSTDINSSHConnection(object):
    """Small wrapper that does not connect stdin to SSH"""
    def __init__(self, ssh):
        self.ssh = ssh

    def __call__(self, *args, **kwargs):
        with tempfile.TemporaryFile() as tempf:
            return self.ssh(*args, stdin=tempf, **kwargs)


class SSHRemoteIO(IOBase):
    """IO operation if the object tree is SSH-accessible

    It doesn't even think about a windows server.
    """
    def __init__(self, host):
        """
        Parameters
        ----------
        host : str
          SSH-accessible host(name) to perform remote IO operations
          on.
        """
        from datalad.support.sshconnector import SSHManager
        # connection manager -- we don't have to keep it around, I think
        self.sshmanager = SSHManager()
        # the connection to the remote
        # we don't open it yet, not yet clear if needed
        self.ssh = self.sshmanager.get_connection(
            host,
            use_remote_annex_bundle=False,
        )
        self.ssh.open()
        self.nostdin_ssh = NoSTDINSSHConnection(self.ssh)

    def mkdir(self, path):
        self.nostdin_ssh('mkdir -p {}'.format(sh_quote(str(path))))

    def put(self, src, dst):
        self.ssh.put(str(src), str(dst))

    def get(self, src, dst):
        self.ssh.get(str(src), str(dst))

    def rename(self, src, dst):
        self.nostdin_ssh('mv {} {}'.format(
            sh_quote(str(src)),
            sh_quote(str(dst)))
        )

    def remove(self, path):
        self.nostdin_ssh('rm {}'.format(sh_quote(str(path))))

    def remove_dir(self, path):
        self.nostdin_ssh('rmdir {}'.format(sh_quote(str(path))))

    def exists(self, path):
        try:
            out, err = self.nostdin_ssh(
                'test -e {}'.format(sh_quote(str(path)))
            )
            return True
        except Exception as e:
            # non-zero exit code gives CommandError
            # do not bother checking for this precise exception to avoid
            # import, should not matter why it crashes
            return False

    def in_archive(self, archive_path, file_path):
        loc = text_type(file_path)
        # query 7z for the specific object location, keeps the output
        # lean, even for big archives
        # bypass most of datalad's code to be able to use subprocess
        # directly
        # this requires python 3.5
        self.ssh.open()
        cmd = ['ssh'] + self.ssh._ctrl_options \
            + [self.ssh.sshri.as_str(),
               '7z', 'l',
               text_type(archive_path), loc]
        done = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
            check=True,
            # the following is `text` from 3.7 onwards
            universal_newlines=True,
        )
        return loc in done.stdout

    def get_from_archive(self, archive, src, dst):
        # bypass most of datalad's code to be able to use subprocess
        # directly
        # this requires python 3.5
        self.ssh.open()
        cmd = ['ssh'] + self.ssh._ctrl_options \
            + [self.ssh.sshri.as_str(),
               '7z', 'x', '-so',
               text_type(archive), text_type(src)]
        with open(dst, 'wb') as target_file:
            subprocess.run(
                cmd,
                stdout=target_file,
                stdin=subprocess.DEVNULL,
                # does not seem to exit non-zero if file not in archive though
                check=True,
            )


class RIARemote(SpecialRemote):
    """This is the base class of RIA remotes.

    It cannot be used directly, but see its subclasses.
    """
    def __init__(self, annex):
        super(RIARemote, self).__init__(annex)
        self.objtree_path = None
        # machine to SSH-log-in to access/store the data
        # subclass must set this
        self.storage_host = None
        # must be absolute, and POSIX
        # subclass must set this
        self.objtree_base_path = None

    def _load_cfg(self, gitdir, name):
        self.storage_host = _get_gitcfg(
            gitdir, 'annex.ria-remote.{}.ssh-host'.format(name))
        objtree_base_path = _get_gitcfg(
            gitdir, 'annex.ria-remote.{}.base-path'.format(name))
        self.objtree_base_path = objtree_base_path.strip() \
            if objtree_base_path else objtree_base_path

    def _verify_config(self, gitdir, fail_noid=True):
        # try loading all needed info from git config
        cfgname = self.annex.getconfig('cfgname')
        if cfgname:
            self._load_cfg(gitdir, cfgname)

        if not self.objtree_base_path:
            self.objtree_base_path = self.annex.getconfig('base-path')
        if not self.objtree_base_path:
            raise RemoteError(
                "No remote base path configured. "
                "Specify `base-path` setting.")

        self.objtree_base_path = Path(self.objtree_base_path)
        if not self.objtree_base_path.is_absolute():
            raise RemoteError(
                'Non-absolute object tree base path configuration')

        if not self.storage_host:
            self.storage_host = self.annex.getconfig('ssh-host')

        # go look for an ID
        self.archive_id = self.annex.getconfig('archive-id')
        if fail_noid and not self.archive_id:
            raise RemoteError(
                "No archive ID configured. This should not happen.")

    def initremote(self):
        # which repo are we talking about
        gitdir = self.annex.getgitdir()
        self._verify_config(gitdir, fail_noid=False)
        if not self.archive_id:
            self.archive_id = _get_datalad_id(gitdir)
            if not self.archive_id:
                # fall back on the UUID for the annex remote
                self.archive_id = self.annex.getuuid()
        self.annex.setconfig('archive-id', self.archive_id)

    def _local_io(self):
        """Are we doing local operations?"""
        # let's not make this decision dependent on the existance
        # of a directory the matches the name of the configured
        # object tree base dir. Such a match could be pure
        # conincidence. Instead, let's do remote whenever there
        # is a remote host configured
        #return self.objtree_base_path.is_dir()
        return not self.storage_host

    def prepare(self):
        gitdir = self.annex.getgitdir()
        self._verify_config(gitdir)

        if self._local_io():
            self.io = LocalIO()
        elif self.storage_host:
            self.io = SSHRemoteIO(self.storage_host)
        else:
            raise RemoteError(
                "Local object tree base path does not exist, and no SSH host "
                "configuration found.")

        # report active special remote configuration
        self.info = {
            'objtree_base_path': str(self.objtree_base_path),
            'storage_host': 'local'
            if self._local_io() else self.storage_host,
        }

    def transfer_store(self, key, filename):
        dsobj_dir, archive_path, key_path = self._get_obj_location(key)
        key_path = dsobj_dir / key_path
        self.io.mkdir(key_path.parent)
        # we need to copy to a temp location to let
        # checkpresent fail while the transfer is still in progress
        tmp_path = key_path.with_suffix(key_path.suffix + '._')
        self.io.put(filename, tmp_path)
        # copy done, atomic rename to actual target
        self.io.rename(tmp_path, key_path)

    def transfer_retrieve(self, key, filename):
        dsobj_dir, archive_path, key_path = self._get_obj_location(key)
        abs_key_path = dsobj_dir / key_path
        # sadly we have no idea what type of source gave checkpresent->true
        # we can either repeat the checks, or just make two oportunistic
        # attempts (at most)
        try:
            self.io.get(abs_key_path, filename)
        except Exception as e1:
            # catch anything and keep it around for a potential re-raise
            try:
                self.io.get_from_archive(archive_path, key_path, filename)
            except Exception as e2:
                raise RuntimeError('Failed to key: {}'.format([e1, e2]))

    def checkpresent(self, key):
        dsobj_dir, archive_path, key_path = self._get_obj_location(key)
        abs_key_path = dsobj_dir / key_path
        if self.io.exists(abs_key_path):
            # we have an actual file for this key
            return True
        elif not self.io.exists(archive_path):
            # TODO honor future 'archive-mode' flag
            # we have no archive, no need to look any further
            return False
        else:
            return self.io.in_archive(archive_path, key_path)

    def remove(self, key):
        dsobj_dir, archive_path, key_path = self._get_obj_location(key)
        key_path = dsobj_dir / key_path
        if self.io.exists(key_path):
            self.io.remove(key_path)
        key_dir = key_path
        # remove at most two levels of empty directories
        for level in range(2):
            key_dir = key_dir.parent
            try:
                self.io.remove_dir(key_dir)
            except Exception:
                break

    def getcost(self):
        # 100 is cheap, 200 is expensive (all relative to Config/Cost.hs)
        # 100/200 are the defaults for local and remote operations in
        # git-annex
        # if we have the object tree locally, operations are cheap (100)
        # otherwise expensive (200)
        return '100' if self._local_io() else '200'

    def whereis(self, key):
        dsobj_dir, archive_path, key_path = self._get_obj_location(key)
        return str(key_path) if self._local_io() \
            else '{}:{}'.format(
                self.storage_host,
                sh_quote(str(key_path)),
        )

    def _get_obj_location(self, key):
        key_dir = self.annex.dirhash_lower(key)
        dsobj_dir = self.objtree_base_path / self.archive_id[:3] / self.archive_id[3:]
        archive_path = dsobj_dir / 'archive.7z'
        # double 'key' is not a mistake, but needed to achieve the exact same
        # layout as the 'directory'-type special remote
        key_path = Path(key_dir) / key / key
        return dsobj_dir, archive_path, key_path
