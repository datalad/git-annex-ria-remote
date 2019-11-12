from annexremote import SpecialRemote
from annexremote import RemoteError

from pathlib import (
    Path,
)
import shutil
from shlex import quote as sh_quote
import subprocess
import logging
lgr = logging.getLogger('ria_remote')

# TODO
# - make archive check optional
# - move fsck to core


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
            universal_newlines=True).strip()
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

    def read_file(self, file_path):
        """Read a remote file's content

        Parameters
        ----------
        file_path : Path or str
          Must be an absolute path

        Returns
        -------
        string
        """

        raise NotImplementedError

    def write_file(self, file_path, content):
        """Write a remote file

        Parameters
        ----------
        file_path : Path or str
          Must be an absolute path
        content : str
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
                str(archive), str(src)],
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
        if not archive_path.exists():
            # no archive, not file
            return False
        loc = str(file_path)
        from datalad.cmd import Runner
        runner = Runner()
        # query 7z for the specific object location, keeps the output
        # lean, even for big archives
        out, err = runner(
            ['7z', 'l', str(archive_path),
             loc],
            log_stdout=True,
        )
        return loc in out

    def read_file(self, file_path):

        with open(str(file_path), 'r') as f:
            content = f.read()
        return content

    def write_file(self, file_path, content):

        with open(str(file_path), 'w') as f:
            f.write(content)


class RemoteCommandFailedError(Exception):
    pass


class SSHRemoteIO(IOBase):
    """IO operation if the object tree is SSH-accessible

    It doesn't even think about a windows server.
    """

    # output markers to detect possible command failure as well as end of output from a particular command:
    REMOTE_CMD_FAIL = "ria-remote: end - fail"
    REMOTE_CMD_OK = "ria-remote: end - ok"

    def __init__(self, host):
        """
        Parameters
        ----------
        host : str
          SSH-accessible host(name) to perform remote IO operations
          on.
        """

        try:

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
            # open a remote shell
            cmd = ['ssh'] + self.ssh._ssh_args + [self.ssh.sshri.as_str()]
            self.shell = subprocess.Popen(cmd, stderr=subprocess.DEVNULL, stdout=subprocess.PIPE, stdin=subprocess.PIPE)
            # swallow login message(s):
            self.shell.stdin.write(b"echo RIA-REMOTE-LOGIN-END\n")
            self.shell.stdin.flush()
            while True:
                line = self.shell.stdout.readline()
                if line == b"RIA-REMOTE-LOGIN-END\n":
                    break
            # TODO: Same for stderr?

        except Exception as e:
            raise RemoteError(str(e))

    def close(self):
        # try exiting shell clean first
        self.shell.stdin.write(b"exit\n")
        self.shell.stdin.flush()
        exitcode = self.shell.wait(timeout=0.5)
        # be more brutal if it doesn't work
        if exitcode is None:  # timed out
            # TODO: Theoretically terminate() can raise if not successful. How to deal with that?
            self.shell.terminate()
        self.sshmanager.close()

    def _append_end_markers(self, cmd):
        """Append end markers to remote command"""

        return cmd + ' && echo "{}" || echo "{}"\n'.format(self.REMOTE_CMD_OK, self.REMOTE_CMD_FAIL)

    def _get_download_size_from_key(self, key):
        """Get the size of an annex object file from it's key

        Note, that this is not necessarily the size of the annexed file, but possibly only a chunk of it.

        Parameter
        ---------
        key: str
          annex key of the file

        Returns
        -------
        int
          size in bytes
        """
        # TODO: datalad's AnnexRepo.get_size_from_key() is not correct/not fitting. Incorporate the wisdom there, too.
        #       We prob. don't want to actually move this method there, since AnnexRepo would be quite an expensive
        #       import. Startup time for special remote matters.
        # TODO: this method can be more compact. we don't need particularly elaborated error distinction

        # see: https://git-annex.branchable.com/internals/key_format/
        key_parts = key.split('--')
        key_fields = key_parts[0].split('-')

        s = S = C = None

        for field in key_fields[1:]:  # note: first one has to be backend -> ignore
            if field.startswith('s'):
                # size of the annexed file content:
                s = int(field[1:]) if field[1:].isdigit() else None
            elif field.startswith('S'):
                # we have a chunk and that's the chunksize:
                S = int(field[1:]) if field[1:].isdigit() else None
            elif field.startswith('C'):
                # we have a chunk, this is it's number:
                C = int(field[1:]) if field[1:].isdigit() else None

        if s is None:
            return None
        elif S is None and C is None:
            return s
        elif S and C:
            if C <= int(s / S):
                return S
            else:
                return s % S
        else:
            raise RemoteError("invalid key: {}".format(key))

    def _run(self, cmd, no_output=True, check=False):

        # TODO: we might want to redirect stderr to stdout here (or have additional end marker in stderr)
        #       otherwise we can't empty stderr to be ready for next command. We also can't read stderr for better error
        #       messages (RemoteError) without making sure there's something to read in any case (it's blocking!)
        #       However, if we are sure stderr can only ever happen if we would raise RemoteError anyway, it might be
        #       okay
        call = self._append_end_markers(cmd)
        self.shell.stdin.write(call.encode())
        self.shell.stdin.flush()

        lines = []
        while True:
            line = self.shell.stdout.readline().decode()
            lines.append(line)
            if line == self.REMOTE_CMD_OK + '\n':
                # end reading
                break
            elif line == self.REMOTE_CMD_FAIL + '\n':
                if check:
                    raise RemoteCommandFailedError("".join(lines[:-1]).replace('\n', '\\n') if len(lines) >= 2
                                                   else "{} failed.".format(cmd.replace('\n', '\\n')))
                else:
                    break
        if no_output and len(lines) > 1:
            failed_cmd = cmd.split()[0]
            # note, that annex would accept only one line in RemoteError's message
            raise RemoteError("{}: {}".format(failed_cmd, "\\n".join(lines[:-1])))
        return "".join(lines[:-1])

    def mkdir(self, path):
        self._run('mkdir -p {}'.format(sh_quote(str(path))))

    def put(self, src, dst):
        self.ssh.put(str(src), str(dst))

    def get(self, src, dst):
        # TODO: see get_from_archive()

        # TODO: Currently we will hang forever if the file isn't readable and it's supposed size is bigger than whatever
        #       cat spits out on stdout. This is because we don't notice that cat has exited non-zero.
        #       We could have end marker on stderr instead, but then we need to empty stderr beforehand to not act upon
        #       output from earlier calls. This is a problem with blocking reading, since we need to make sure there's
        #       actually something to read in any case.
        cmd = 'cat {}'.format(str(src))
        self.shell.stdin.write(cmd.encode())
        self.shell.stdin.write(b"\n")
        self.shell.stdin.flush()

        from os.path import basename
        key = basename(str(src))
        try:
            size = self._get_download_size_from_key(key)
        except RemoteError as e:
            raise RemoteError("src: {}".format(str(src)) + str(e))

        if size is None:
            # rely on SCP for now
            self.ssh.get(str(src), str(dst))
            return

        with open(dst, 'wb') as target_file:
            bytes_received = 0
            while bytes_received < size:  # TODO: some additional abortion criteria? check stderr in addition?
                c = self.shell.stdout.read1(1024)
                # no idea yet, whether or not there's sth to gain by a sophisticated determination of how many bytes to
                # read at once (like size - bytes_received)
                if c:
                    bytes_received += len(c)
                    target_file.write(c)

    def rename(self, src, dst):
        self._run('mv {} {}'.format(sh_quote(str(src)), sh_quote(str(dst))))

    def remove(self, path):
        self._run('rm {}'.format(sh_quote(str(path))))

    def remove_dir(self, path):
        self._run('rmdir {}'.format(sh_quote(str(path))))

    def exists(self, path):
        try:
            self._run('test -e {}'.format(sh_quote(str(path))), check=True)
            return True
        except RemoteCommandFailedError:
            return False

    def in_archive(self, archive_path, file_path):

        loc = str(file_path)
        # query 7z for the specific object location, keeps the output
        # lean, even for big archives
        cmd = '7z l {} {}'.format(str(archive_path), loc)

        # Note: Currently relies on file_path not showing up in case of failure
        # including non-existent archive. If need be could be more sophisticated
        # and called with check=True + catch RemoteCommandFailedError
        out = self._run(cmd, no_output=False, check=False)

        return loc in out

    def get_from_archive(self, archive, src, dst):

        # TODO: We probably need to check exitcode on stderr (via marker). If archive or content is missing we will
        #       otherwise hang forever waiting for stdout to fill `size`

        cmd = '7z x -so {} {}\n'.format(str(archive), str(src))
        self.shell.stdin.write(cmd.encode())
        self.shell.stdin.flush()

        # TODO: - size needs double-check and some robustness
        #       - can we assume src to be a posixpath?
        #       - RF: Apart from the executed command this should be pretty much identical to self.get(), so move that
        #         code into a common function

        from os.path import basename
        size = self._get_download_size_from_key(basename(str(src)))

        with open(dst, 'wb') as target_file:
            bytes_received = 0
            while bytes_received < size:
                c = self.shell.stdout.read1(1024)
                if c:
                    bytes_received += len(c)
                    target_file.write(c)

    def read_file(self, file_path):

        cmd = "cat  {}".format(str(file_path))
        try:
            out = self._run(cmd, no_output=False, check=True)
        except RemoteCommandFailedError:
            raise RemoteError("Could not read {}".format(str(file_path)))

        return out

    def write_file(self, file_path, content):

        if not content.endswith('\n'):
            content += '\n'

        cmd = "echo \"{}\" > {}".format(content, str(file_path))
        try:
            self._run(cmd, check=True)
        except RemoteCommandFailedError:
            raise RemoteError("Could not write to {}".format(str(file_path)))


class RIARemote(SpecialRemote):
    """This is the class of RIA remotes.
    """

    _dataset_tree_version = '1'
    _object_tree_version = '1'

    def __init__(self, annex):
        super(RIARemote, self).__init__(annex)
        # machine to SSH-log-in to access/store the data
        # subclass must set this
        self.storage_host = None
        # must be absolute, and POSIX
        # subclass must set this
        self.objtree_base_path = None
        # by default we can read and write
        self.read_only = False
        self.can_notify = None  # to be figured out later, since annex.protocol.extensions is not yet accessible
        self.force_write = None
        self.uuid = None

        # for caching the remote's layout locations:
        self.remote_git_dir = None
        self.remote_archive_dir = None
        self.remote_obj_dir = None

    def _load_cfg(self, gitdir, name):
        self.storage_host = _get_gitcfg(
            gitdir, 'annex.ria-remote.{}.ssh-host'.format(name))
        objtree_base_path = _get_gitcfg(
            gitdir, 'annex.ria-remote.{}.base-path'.format(name))
        self.objtree_base_path = objtree_base_path.strip() \
            if objtree_base_path else objtree_base_path
        # Whether or not to force writing to the remote. Currently used to overrule write protection due to layout
        # version mismatch.
        self.force_write = _get_gitcfg(
            gitdir, 'annex.ria-remote.{}.force-write'.format(name))

    def _verify_config(self, gitdir, fail_noid=True):
        # try loading all needed info from git config
        name = self.annex.getconfig('name')
        self._load_cfg(gitdir, name)

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

        # Note: Special value '0' is replaced by None only after checking the repository's annex config.
        # This is to uniformly handle '0' and None later on, but let a user's config '0' overrule what's
        # stored by git-annex.
        if not self.storage_host:
            self.storage_host = self.annex.getconfig('ssh-host')
        elif self.storage_host == '0':
            self.storage_host = None

        # go look for an ID
        self.archive_id = self.annex.getconfig('archive-id')
        if fail_noid and not self.archive_id:
            raise RemoteError(
                "No archive ID configured. This should not happen.")

        if not self.force_write:
            self.force_write = self.annex.getconfig('force-write')

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
        # coincidence. Instead, let's do remote whenever there
        # is a remote host configured
        #return self.objtree_base_path.is_dir()
        return not self.storage_host

    def _info(self, msg):

        if self.can_notify:
            self.annex.info(msg)
        # TODO: else: if we can't have an actual info message, at least have a debug message
        #       This probably requires further refurbishment of datalad's capability to deal with such aspects of the
        #       special remote protocol

    def _set_read_only(self, msg):

        if not self.force_write:
            self.read_only = True
            self._info(msg)
        else:
            self._info("Was instructed to force write")

    def _check_layout_version(self):
        """Check whether we can deal with the layout reported by the remote end

        There are two aspects of layout versioning:
        - the tree to put the datasets in (version recorded in base_path/ria-layout-version)
        - the tree of the actual annex objects of a particular dataset (version recorded in
          dataset_somewhere_beneath_base_path/ria-layout-version)

        If the version found on the remote end isn't supported and `force-write` isn't configured,
        this sets the remote to read-only operation.
        """

        dataset_tree_version_file = \
            self.objtree_base_path / 'ria-layout-version'
        object_tree_version_file = \
            self.objtree_base_path / self.archive_id[:3] / self.archive_id[3:] / 'ria-layout-version'

        read_only_msg = "Setting remote to read-only usage in order to prevent damage by putting things into an " \
                        "unknown version of the target layout. You can overrule this by configuring " \
                        "'annex.ria-remote.<name>.force-write'."

        # 1. check dataset tree version
        try:
            remote_dataset_tree_version = self.io.read_file(dataset_tree_version_file).strip()
            if remote_dataset_tree_version != self._dataset_tree_version:
                # Note: In later versions, condition might change in order to deal with older versions
                self._info("Remote dataset tree reports version {}. Supported version is {}. Consider upgrading "
                           "git-annex-ria-remote or fix the structure on the remote end."
                           "".format(remote_dataset_tree_version, self._dataset_tree_version))
                self._set_read_only(read_only_msg)

        except (RemoteError, FileNotFoundError):  # depends on whether self.io is local or ssh
            # assume file doesn't exist
            # TODO: Is there a possibility RemoteError has a different reason and should be handled differently?
            #       Don't think so ATM
            if not self.io.exists(dataset_tree_version_file.parent):
                # we are first, just put our stamp on it
                try:
                    self.io.mkdir(dataset_tree_version_file.parent)
                    self.io.write_file(dataset_tree_version_file, self._dataset_tree_version)
                except Exception as e:
                    raise RemoteError(str(e))
                    # Note, that we need to fail in any case but in a way appropriate for a special remote. Otherwise
                    # we get something like a "broken pipe" error, which tells nothing about the issue.
            else:
                # directory is there, but no version file. We don't know what that is. Treat the same way as if there
                # was an unknown version on record
                self._info("Remote doesn't report any dataset tree version. Consider upgrading git-annex-ria-remote or "
                           "fix the structure on the remote end.")
                self._set_read_only(read_only_msg)

        # 2. check (annex) object tree version
        try:
            remote_object_tree_version = self.io.read_file(object_tree_version_file).strip()
            if remote_object_tree_version != self._object_tree_version:
                self._info("Remote object tree reports version {}. Supported version is {}. Consider upgrading "
                           "git-annex-ria-remote.".format(remote_object_tree_version, self._object_tree_version))
                self._set_read_only(read_only_msg)
        except (RemoteError, FileNotFoundError):
            if not self.io.exists(object_tree_version_file.parent):
                # we are first, just put our stamp on it
                try:
                    self.io.mkdir(object_tree_version_file.parent)
                    self.io.write_file(object_tree_version_file, self._object_tree_version)
                except Exception as e:
                    raise RemoteError(str(e))
                    # Note, that we need to fail in any case but in a way appropriate for a special remote. Otherwise
                    # we get something like a "broken pipe" error, which tells nothing about the issue.
            else:
                self._info("Remote doesn't report any object tree version. Consider upgrading git-annex-ria-remote or "
                           "fix the structure on the remote end.")
                self._set_read_only(read_only_msg)

    def prepare(self):

        # can we use self.annex.info() for sending user output to annex?
        self.can_notify = "INFO" in self.annex.protocol.extensions

        gitdir = self.annex.getgitdir()
        self.uuid = self.annex.getuuid()
        self._verify_config(gitdir)

        if self._local_io():
            self.io = LocalIO()
        elif self.storage_host:
            self.io = SSHRemoteIO(self.storage_host)
            from atexit import register
            register(self.io.close)
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

        self._check_layout_version()

        # cache remote layout directories
        self.remote_git_dir, self.remote_archive_dir, self.remote_obj_dir = \
            self.get_layout_locations(self.objtree_base_path, self.archive_id)

    def transfer_store(self, key, filename):
        if self.read_only:
            raise RemoteError("Remote was set to read-only. "
                              "Configure 'ria-remote.<name>.force-write' to overrule this.")

        dsobj_dir, archive_path, key_path = self._get_obj_location(key)
        key_path = dsobj_dir / key_path
        self.io.mkdir(key_path.parent)

        # we need to copy to a temp location to let
        # checkpresent fail while the transfer is still in progress
        # and furthermore not interfere with administrative tasks in annex/objects
        # In addition include uuid, to not interfere with parallel uploads from different remotes
        transfer_dir = self.remote_git_dir / "ria-remote-{}".format(self.uuid) / "transfer"
        self.io.mkdir(transfer_dir)
        tmp_path = transfer_dir / key

        if tmp_path.exists():
            # Just in case - some parallel job could already be writing to it
            # at least tell the conclusion, not just some obscure permission error
            raise RemoteError('{}: upload already in progress'.format(filename))
        try:
            self.io.put(filename, tmp_path)
            # copy done, atomic rename to actual target
            self.io.rename(tmp_path, key_path)
        except Exception as e:
            # whatever went wrong, we don't want to leave the transfer location blocked
            self.io.remove(tmp_path)
            raise RemoteError(str(e))

    def transfer_retrieve(self, key, filename):
        dsobj_dir, archive_path, key_path = self._get_obj_location(key)
        abs_key_path = dsobj_dir / key_path
        # sadly we have no idea what type of source gave checkpresent->true
        # we can either repeat the checks, or just make two opportunistic
        # attempts (at most)
        try:
            self.io.get(abs_key_path, filename)
        except Exception as e1:
            # catch anything and keep it around for a potential re-raise
            try:
                self.io.get_from_archive(archive_path, key_path, filename)
            except Exception as e2:
                raise RemoteError('Failed to key: {}'.format([e1, e2]))

    def checkpresent(self, key):
        dsobj_dir, archive_path, key_path = self._get_obj_location(key)
        abs_key_path = dsobj_dir / key_path
        if self.io.exists(abs_key_path):
            # we have an actual file for this key
            return True
        # do not make a careful check whether an archive exists, because at
        # present this requires an additional SSH call for remote operations
        # which may be rather slow. Instead just try to run 7z on it and let
        # it fail if no archive is around
        # TODO honor future 'archive-mode' flag
        return self.io.in_archive(archive_path, key_path)

    def remove(self, key):
        if self.read_only:
            raise RemoteError("Remote was set to read-only. "
                              "Configure 'ria-remote.<name>.force-write' to overrule this.")

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
                sh_quote(str(key_path)),  # TODO: Shouldn't we report the entire path (i.e. dsobj_dir + key_path)?
        )

    @staticmethod
    def get_layout_locations(base_path, dsid):
        # Note: Changes to this method may require an update of RIARemote._layout_version

        dsgit_dir = base_path / dsid[:3] / dsid[3:]
        archive_dir = dsgit_dir / 'archives'
        dsobj_dir = dsgit_dir / 'annex' / 'objects'
        return dsgit_dir, archive_dir, dsobj_dir

    def _get_obj_location(self, key):
        # Note: Changes to this method may require an update of RIARemote._layout_version
        # Note2: archive_path is always the same ATM. However, it might depend on `key` in the future.
        #        Therefore build the actual filename for the archive herein as opposed to `get_layout_locations`.

        key_dir = self.annex.dirhash_lower(key)
        # double 'key' is not a mistake, but needed to achieve the exact same
        # layout as the 'directory'-type special remote
        key_path = Path(key_dir) / key / key
        archive_path = self.remote_archive_dir / 'archive.7z'
        return self.remote_obj_dir, archive_path, key_path
