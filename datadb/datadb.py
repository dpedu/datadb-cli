#!/usr/bin/env python3

import argparse
from configparser import ConfigParser
from urllib.parse import urlparse
from os.path import normpath, join, exists
from os import chmod, chown, stat, environ
from enum import Enum
import subprocess
from requests import get,put,head

SSH_KEY_PATH = '/root/.ssh/datadb.key'
RSYNC_DEFAULT_ARGS = ['rsync', '-avzr', '--exclude=.datadb.lock', '--whole-file', '--one-file-system', '--delete', '-e', 'ssh -i {} -p 4874 -o StrictHostKeyChecking=no'.format(SSH_KEY_PATH)]
DATADB_HTTP_API = 'http://datadb.services.davepedu.com:4875/cgi-bin/'

class SyncStatus(Enum):
    "Data is on local disk"
    DATA_AVAILABLE = 1
    "Data is not on local disk"
    DATA_MISSING = 2


def restore(profile, conf, force=False): #remote_uri, local_dir, identity='/root/.ssh/datadb.key'
    """
    Restore data from datadb
    """
    
    # Sanity check: If the lockfile exists we assume the data is already there, so we wouldn't want to call rsync again
    # as it would wipe out local changes. This can be overridden with --force
    assert (status(profile, conf) == SyncStatus.DATA_MISSING) or force, "Data already exists (Use --force?)"
    
    original_perms = stat(conf["dir"])
    dest = urlparse(conf["uri"])
    
    status_code = head(DATADB_HTTP_API+'get_backup', params={'proto':dest.scheme, 'name':profile}).status_code
    if status_code == 404:
        print("Connected to datadb, but datasource '{}' doesn't exist. Exiting".format(profile))
        # TODO: special exit code >1 to indicate this?
        return
    
    if dest.scheme == 'rsync':
        args = RSYNC_DEFAULT_ARGS[:]
        
        # Request backup server to prepare the backup, the returned dir is what we sync from
        rsync_path = get(DATADB_HTTP_API+'get_backup', params={'proto':'rsync', 'name':profile}).text.rstrip()
        
        # Add rsync source path
        args.append('nexus@{}:{}'.format(dest.netloc, normpath(rsync_path)+'/'))
        
        # Add local dir
        args.append(normpath(conf["dir"])+'/')
        print("Rsync restore call: {}".format(' '.join(args)))
        
        subprocess.check_call(args)
    
    elif dest.scheme == 'archive':
        # http request backup server
        # download tarball
        args_curl = ['curl', '-s', '-v', '-XGET', '{}get_backup?proto=archive&name={}'.format(DATADB_HTTP_API, profile)]
        # unpack
        args_tar = ['tar', 'zxv', '-C', normpath(conf["dir"])+'/']
        
        print("Tar restore call: {} | {}".format(' '.join(args_curl), ' '.join(args_tar)))
        
        dl = subprocess.Popen(args_curl, stdout=subprocess.PIPE)
        extract = subprocess.Popen(args_tar, stdin=dl.stdout)
        
        dl.wait()
        extract.wait()
        # TODO: convert to pure python? 
        
        assert dl.returncode == 0, "Could not download archive"
        assert extract.returncode == 0, "Could not extract archive"
    
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
    assert (status(profile, conf) == SyncStatus.DATA_AVAILABLE) or force, "Data is missing (Use --force?)"
    
    dest = urlparse(conf["uri"])
    
    if dest.scheme == 'rsync':
        args = RSYNC_DEFAULT_ARGS[:]
        
        # Add local dir
        args.append(normpath(conf["dir"])+'/')
        
        # Hit backupdb via http to retreive absolute path of rsync destination of remote server
        rsync_path = get(DATADB_HTTP_API+'new_backup', params={'proto':'rsync', 'name':profile, 'keep':conf["keep"]}).text.rstrip()
        
        # Add rsync source path
        args.append(normpath('nexus@{}:{}'.format(dest.netloc, rsync_path))+'/')
        
        #print("Rsync backup call: {}".format(' '.join(args)))
        
        try:
            subprocess.check_call(args)
        except subprocess.CalledProcessError as cpe:
            if cpe.returncode not in [0,24]: # ignore partial transfer due to vanishing files on our end
                raise
    
    elif dest.scheme == 'archive':
        # CD to local source dir
        # create tarball
        # http PUT file to backup server
        args_tar = ['tar', '--exclude=.datadb.lock', '-zcv', './']
        args_curl = ['curl', '-v', '-XPUT', '--data-binary', '@-', '{}new_backup?proto=archive&name={}&keep={}'.format(DATADB_HTTP_API, profile, conf["keep"])]
        
        print("Tar backup call: {} | {}".format(' '.join(args_tar), ' '.join(args_curl)))
        
        compress = subprocess.Popen(args_tar, stdout=subprocess.PIPE, cwd=normpath(conf["dir"])+'/')
        upload = subprocess.Popen(args_curl, stdin=compress.stdout)
        
        compress.wait()
        upload.wait()
        # TODO: convert to pure python? 
        
        assert compress.returncode == 0, "Could not create archive"
        assert upload.returncode == 0, "Could not upload archive"


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
    ----------------------------
    
    Each [section] defines one backup task.
    
    Fields:
    
    *uri*: Destination/source for this instance's data. Always fits the following format:
    
        <procotol>://<server>/<backup name>
        
        Valid protocols:
        
            rsync - rsync executed over SSH. The local dir will be synced with the remote backup dir using rsync.
            archive - tar archives transported over HTTP. The local dir will be tarred and PUT to the backup server's remote dir via http.
    
    *dir*: Local dir for this backup
    
    *keep*: Currently unused. Number of historical copies to keep on remote server
    
    *auth*: Currently unused. Username:password string to use while contacting the datadb via HTTP.
    
    *restore_preexec*: Shell command to exec before pulling/restoring data
    
    *restore_postexec*: Shell command to exec after pulling/restoring data
    
    *export_preexec*: Shell command to exec before pushing data
    
    *export_postexec*: Shell command to exec after pushing data
    
    """
    
    conf_path = environ["DATADB_CONF"] if "DATADB_CONF" in environ else "/etc/datadb.ini"
    
    # Load profiles
    config = ConfigParser()
    config.read(conf_path)
    
    config = {section:{k:config[section][k] for k in config[section]} for section in config.sections()}
    
    parser = argparse.ArgumentParser(description="Backupdb Agent depends on config: /etc/datadb.ini")
    
    parser.add_argument('-f', '--force', default=False, action='store_true', help='force restore operation if destination data already exists')
    parser.add_argument('-n', '--no-exec', default=False, action='store_true', help='don\'t run pre/post-exec commands')
    parser.add_argument('-b', '--no-pre-exec', default=False, action='store_true', help='don\'t run pre-exec commands')
    parser.add_argument('-m', '--no-post-exec', default=False, action='store_true', help='don\'t run post-exec commands')
    
    parser.add_argument('profile', type=str, choices=config.keys(), help='Profile to restore')
    
    #parser.add_argument('-i', '--identity',
    #                    help='Ssh keyfile to use', type=str, default='/root/.ssh/datadb.key')
    #parser.add_argument('-r', '--remote',
    #                    help='Remote server (rsync://...)', type=str, required=True)
    #parser.add_argument('-l', '--local_dir',
    #                    help='Local path', type=str, required=True)
    
    subparser_modes = parser.add_subparsers(dest='mode', help='modes (only "rsync")')
    
    subparser_backup = subparser_modes.add_parser('backup', help='backup to datastore')
    
    subparser_restore = subparser_modes.add_parser('restore', help='restore from datastore')
    
    subparser_status = subparser_modes.add_parser('status', help='get info for profile')
    
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
        
        backup(args.profile, config[args.profile])
        
        if not args.no_post_exec and config[args.profile]['export_postexec']:
            shell_exec(config[args.profile]['export_postexec'])
    
    elif args.mode == 'status':
        info = status(args.profile, config[args.profile])
        print(SyncStatus(info))
    
    else:
        parser.print_usage()

if __name__ == '__main__':
    main()
