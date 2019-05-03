#!/usr/bin/env python3

import argparse
from configparser import ConfigParser
from urllib.parse import urlparse
from os.path import normpath, join, exists
from os import chmod, chown, stat, environ
from enum import Enum
import subprocess
from requests import get, put, head
from threading import Thread


SSH_KEY_PATH = environ["DATADB_KEYPATH"] if "DATADB_KEYPATH" in environ else '/root/.ssh/datadb.key'
RSYNC_DEFAULT_ARGS = ['rsync', '-avzr', '-e', 'ssh -o StrictHostKeyChecking=no',
                      '--exclude=.datadb.lock', '--whole-file', '--one-file-system', '--delete']
DATADB_HTTP_API = environ.get('DATADB_HTTP_API', 'http://datadb.services.davepedu.com:4875/cgi-bin/')


class SyncStatus(Enum):
    "Data is on local disk"
    DATA_AVAILABLE = 1
    "Data is not on local disk"
    DATA_MISSING = 2


# Requests will call tell() on the file-like stdout stream if the tell attribute exists. However subprocess'
# stdout stream (_io.BufferedReader) does not support this (raises OSError: [Errno 29] Illegal seek).
# If the tell attribute is missing, requests will fall back to simply iterating on the file-like object,
# so, we support only the iterable interface
class WrappedStdout(object):
    BUFFSIZE = 256 * 1024

    def __init__(self, stdout):
        self.stdout = stdout

    def __iter__(self):
        return self

    def __next__(self):
        data = self.stdout.read(self.BUFFSIZE)
        if not data:
            raise StopIteration()
        return data

    def close(self):
        self.stdout.close()


def restore(profile, conf, force=False):  # remote_uri, local_dir, identity='/root/.ssh/datadb.key'
    """
    Restore data from datadb
    """

    # Sanity check: If the lockfile exists we assume the data is already there, so we wouldn't want to call rsync again
    # as it would wipe out local changes. This can be overridden with --force
    if not ((status(profile, conf) == SyncStatus.DATA_MISSING) or force):
        raise Exception("Data already exists (Use --force?)")

    original_perms = stat(conf["dir"])
    dest = urlparse(conf["uri"])

    status_code = head(DATADB_HTTP_API + 'get_backup', params={'proto': dest.scheme, 'name': profile}).status_code
    if status_code == 404:
        print("Connected to datadb, but datasource '{}' doesn't exist. Exiting".format(profile))
        # TODO: special exit code >1 to indicate this?
        return

    if dest.scheme == 'rsync':
        args = RSYNC_DEFAULT_ARGS[:]
        args += ['-e', 'ssh -i {} -p {}'.format(SSH_KEY_PATH, dest.port or 22)]

        # Request backup server to prepare the backup, the returned dir is what we sync from
        rsync_path = get(DATADB_HTTP_API + 'get_backup', params={'proto': 'rsync', 'name': profile}).text.rstrip()

        # Add rsync source path
        args.append('nexus@{}:{}'.format(dest.hostname, normpath(rsync_path) + '/'))

        # Add local dir
        args.append(normpath(conf["dir"]) + '/')
        print("Rsync restore call: {}".format(' '.join(args)))

        subprocess.check_call(args)

    elif dest.scheme == 'archive':
        # http request backup server
        # download tarball
        args_curl = ['curl', '-s', '-v', '-XGET', '{}get_backup?proto=archive&name={}'.format(DATADB_HTTP_API, profile)]
        # unpack
        args_tar = [get_tarcmd(), 'zxv', '-C', normpath(conf["dir"]) + '/']

        print("Tar restore call: {} | {}".format(' '.join(args_curl), ' '.join(args_tar)))

        dl = subprocess.Popen(args_curl, stdout=subprocess.PIPE)
        extract = subprocess.Popen(args_tar, stdin=dl.stdout)

        dl.wait()
        extract.wait()
        # TODO: convert to pure python?

        if dl.returncode != 0:
            raise Exception("Could not download archive")
        if extract.returncode != 0:
            raise Exception("Could not extract archive")

    # Restore original permissions on data dir
    # TODO store these in conf file
    chmod(conf["dir"], original_perms.st_mode)
    chown(conf["dir"], original_perms.st_uid, original_perms.st_gid)
    # TODO apply other permissions


def backup(profile, conf, force=False):
    """
    Backup data to datadb
    """

    # Sanity check: If the lockfile doesn't exist we assume the data is missing, so we wouldn't want to call rsync
    # again as it would wipe out the backup.
    if not ((status(profile, conf) == SyncStatus.DATA_AVAILABLE) or force):
        raise Exception("Data is missing (Use --force?)")

    dest = urlparse(conf["uri"])

    if dest.scheme == 'rsync':
        args = RSYNC_DEFAULT_ARGS[:]
        args += ['-e', 'ssh -i {} -p {}'.format(SSH_KEY_PATH, dest.port or 22)]
        # args += ["--port", str(dest.port or 22)]

        # Excluded paths
        if conf["exclude"]:
            for exclude_path in conf["exclude"].split(","):
                if not exclude_path == "":
                    args.append("--exclude")
                    args.append(exclude_path)

        # Add local dir
        args.append(normpath(conf["dir"]) + '/')

        new_backup_params = {'proto': 'rsync',
                             'name': profile,
                             'keep': conf["keep"]}
        if conf["inplace"]:
            new_backup_params["inplace"] = 1
        # Hit backupdb via http to retreive absolute path of rsync destination of remote server
        rsync_path, token = get(DATADB_HTTP_API + 'new_backup', params=new_backup_params).json()

        # Add rsync source path
        args.append(normpath('nexus@{}:{}'.format(dest.hostname, rsync_path)) + '/')

        # print("Rsync backup call: {}".format(' '.join(args)))

        try:
            subprocess.check_call(args)
        except subprocess.CalledProcessError as cpe:
            if cpe.returncode not in [0, 24]:  # ignore partial transfer due to vanishing files on our end
                raise

        # confirm completion if backup wasnt already in place
        if not conf["inplace"]:
            put(DATADB_HTTP_API + 'new_backup', params={'proto': 'rsync', 'name': profile, 'token': token,
                                                        'keep': conf["keep"]})

    elif dest.scheme == 'archive':
        # CD to local source dir
        # tar+gz data and stream to backup server

        args_tar = []

        if has_binary("ionice"):
            args_tar += ['ionice', '-c', '3']

        args_tar += ['nice', '-n', '19']
        args_tar += [get_tarcmd(),
                     '--exclude=.datadb.lock',
                     '--warning=no-file-changed',
                     '--warning=no-file-removed',
                     '--warning=no-file-ignored',
                     '--warning=no-file-shrank']

        # Use pigz if available (Parallel gzip - http://zlib.net/pigz/)
        if has_binary("pigz"):
            args_tar += ["--use-compress-program", "pigz"]
        else:
            args_tar += ["-z"]

        # Excluded paths
        if conf["exclude"]:
            for exclude_path in conf["exclude"].split(","):
                if not exclude_path == "":
                    args_tar.append("--exclude")
                    args_tar.append(exclude_path)

        args_tar += ['-cv', './']
        tar_dir = normpath(conf["dir"]) + '/'
        print("Tar call in {}: {}".format(args_tar, tar_dir))

        tar = subprocess.Popen(args_tar, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=tar_dir)

        put_url = '{}new_backup?proto=archive&name={}&keep={}'.format(DATADB_HTTP_API, profile, conf["keep"])
        print("Putting to: {}".format(put_url))

        tar_errors = []
        error_scanner = Thread(target=scan_errors, args=(tar.stderr, tar_errors), daemon=True)
        error_scanner.start()

        upload = put(put_url, data=WrappedStdout(tar.stdout))
        if upload.status_code != 200:
            print(upload.text)
            raise Exception("Upload failed with code: {}".format(upload.status_code))

        tar.wait()
        error_scanner.join()

        if tar.returncode != 0 and len(tar_errors) > 0:
            raise Exception("Tar process exited with nonzero code {}. Tar errors: \n    {}".
                            format(tar.returncode, "\n    ".join(tar_errors)))


def scan_errors(stream, error_list):
    """
    Read and print lines from a stream, appending messages that look like errors to error_list
    """
    # Tar does not have an option to ignore file-removed errors. The warnings can be hidden but even with
    # --ignore-failed-read, file-removed errors cause a non-zero exit. So, hide the warnings we don't care about
    # using --warnings=no-xxx and scan output for unknown messages, assuming anything found is bad.
    for line in stream:
        line = line.decode("UTF-8").strip()
        if not line.startswith("./"):
            if line not in error_list:
                error_list.append(line)
        print(line)


def status(profile, conf):
    """
    Check status of local dir - if the lock file is in place, we assume the data is there
    """

    lockfile = join(conf["dir"], '.datadb.lock')

    if exists(lockfile):
        return SyncStatus.DATA_AVAILABLE
    return SyncStatus.DATA_MISSING


def shell_exec(cmd, workdir='/tmp/'):
    """
    Execute a command in shell, wait for exit.
    """
    print("Calling: {}".format(cmd))
    subprocess.Popen(cmd, shell=True, cwd=workdir).wait()


def get_tarcmd():
    return "gtar" if has_binary("gtar") else "tar"


def has_binary(name):
    """
    Check if the passed command is available
    :return: boolean
    """
    try:
        subprocess.check_call(['which', name], stdout=subprocess.DEVNULL)
    except subprocess.CalledProcessError:
        return False
    return True


def main():
    """
    Excepts a config file at /etc/datadb.ini. Example:

    ----------------------------
    [gyfd]
    uri=
    dir=
    keep=
    auth=
    restore_preexec=
    restore_postexec=
    export_preexec=
    export_postexec=
    exclude=
    ----------------------------

    Each [section] defines one backup task.

    Fields:

    *uri*: Destination/source for this instance's data. Always fits the following format:

        <procotol>://<server>/<backup name>

        Valid protocols:

            rsync - rsync executed over SSH. The local dir will be synced with the remote backup dir using rsync.
            archive - tar archives transported over HTTP. The local dir will be tarred and PUT to the backup server's
                      remote dir via http.

    *dir*: Local dir for this backup

    *keep*: Currently unused. Number of historical copies to keep on remote server

    *auth*: Currently unused. Username:password string to use while contacting the datadb via HTTP.

    *restore_preexec*: Shell command to exec before pulling/restoring data

    *restore_postexec*: Shell command to exec after pulling/restoring data

    *export_preexec*: Shell command to exec before pushing data

    *export_postexec*: Shell command to exec after pushing data

    *exclude*: if the underlying transport method supports excluding paths, a comma separated list of paths to exclude.
               Applies to backup operations only.

    *inplace*: rsync only. if enabled, the server will keep only a single copy that you will rsync over. intended for
               single copies of LARGE datasets. overrides "keep".

    """

    required_conf_params = ['dir', 'uri']
    conf_params = {'export_preexec': None,
                   'exclude': None,
                   'keep': 5,
                   'restore_preexec': None,
                   'restore_postexec': None,
                   'auth': '',
                   'export_postexec': None,
                   'inplace': False}
    conf_path = environ["DATADB_CONF"] if "DATADB_CONF" in environ else "/etc/datadb.ini"

    # Load profiles
    config = ConfigParser()
    config.read(conf_path)

    config = {section: {k: config[section][k] for k in config[section]} for section in config.sections()}
    for conf_k, conf_dict in config.items():
        for expect_param, expect_default in conf_params.items():
            if expect_param not in conf_dict.keys():
                conf_dict[expect_param] = expect_default
        for expect_param in required_conf_params:
            if expect_param not in conf_dict.keys():
                raise Exception("Required parameter {} missing for profile {}".format(expect_param, conf_k))

    parser = argparse.ArgumentParser(description="Backupdb Agent depends on config: /etc/datadb.ini")

    parser.add_argument('-f', '--force', default=False, action='store_true',
                        help='force restore operation if destination data already exists')
    parser.add_argument('-n', '--no-exec', default=False, action='store_true', help='don\'t run pre/post-exec commands')
    parser.add_argument('-b', '--no-pre-exec', default=False, action='store_true', help='don\'t run pre-exec commands')
    parser.add_argument('-m', '--no-post-exec', default=False, action='store_true',
                        help='don\'t run post-exec commands')

    parser.add_argument('profile', type=str, choices=config.keys(), help='Profile to restore')

    # parser.add_argument('-i', '--identity',
    #                    help='Ssh keyfile to use', type=str, default='/root/.ssh/datadb.key')
    # parser.add_argument('-r', '--remote',
    #                    help='Remote server (rsync://...)', type=str, required=True)
    # parser.add_argument('-l', '--local_dir',
    #                    help='Local path', type=str, required=True)

    subparser_modes = parser.add_subparsers(dest='mode', help='modes (only "rsync")')

    subparser_backup = subparser_modes.add_parser('backup', help='backup to datastore')  # NOQA

    subparser_restore = subparser_modes.add_parser('restore', help='restore from datastore')  # NOQA

    subparser_status = subparser_modes.add_parser('status', help='get info for profile')  # NOQA

    args = parser.parse_args()

    if args.no_exec:
        args.no_pre_exec = True
        args.no_post_exec = True

    if args.mode == 'restore':
        if not args.no_pre_exec and config[args.profile]['restore_preexec']:
            shell_exec(config[args.profile]['restore_preexec'])

        restore(args.profile, config[args.profile], force=args.force)

        if not args.no_post_exec and config[args.profile]['restore_postexec']:
            shell_exec(config[args.profile]['restore_postexec'])

    elif args.mode == 'backup':
        if not args.no_pre_exec and config[args.profile]['export_preexec']:
            shell_exec(config[args.profile]['export_preexec'])

        try:
            backup(args.profile, config[args.profile], force=args.force)
        finally:
            if not args.no_post_exec and config[args.profile]['export_postexec']:
                shell_exec(config[args.profile]['export_postexec'])

    elif args.mode == 'status':
        info = status(args.profile, config[args.profile])
        print(SyncStatus(info))

    else:
        parser.print_usage()

if __name__ == '__main__':
    main()
