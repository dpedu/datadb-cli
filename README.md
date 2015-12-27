# datadb

Effortless program data deployment and backup

## Installation

* Clone: `git clone http://gitlab.xmopx.net/dave/datadb-cli.git datadb-cli`
* Install prereqs: `cd datadb-cli ; pip3 install -r requirements.txt`
* Install: `python3 setup.py install`

## Requirements

Just python3 and [requests](http://python-requests.org/).

## Usage

### Setup

For one, this is beta and some things are hard-coded. In datadb.py it is recommended to change the DATADB_HTTP_API URL.
This URL should be the cgi-bin path of an http server running [datadb-scripts](http://gitlab.xmopx.net/dave/datadb-scripts).

Next, a config file must be created for each directory to be restored/backed up. It lives at /etc/datadb.ini and contains
many entires of this format:

```
[profile_name]
uri=<procotol>://<server>/<backup_name>
dir=/local/path
keep=5
auth=username:password
restore_preexec=
restore_postexec=
export_preexec=
export_postexec=
```

Each [section] defines one backup task. At present, all fields must be there even if their value is blank.

Fields:

**uri**: Destination/source for this instance's data. Must be this format: `<procotol>://<server>/<backup name>`

Valid protocols:

* rsync - rsync executed over SSH. The local dir will be synced with the remote backup dir using rsync. Vice-versa for restores.
* archive - tar.gz data streamed over HTTP. The local dir will be tarred and PUT to the backup server's remote dir via http. Vice-versa for restores. Recommended only for smaller datasets.

**dir**: Local dir for this backup/restore

**keep**: Number of historical copies to keep on remote server

**auth**: Not implemented. Username:password string to use while contacting the datadb via HTTP.

**restore_preexec**: Not implemented. Shell command to exec before pulling/restoring data.

**restore_postexec**: Not implemented. Shell command to exec after pulling/restoring data. For example, loading a mysql dump

**export_preexec**: Not implemented. Shell command to exec before pushing data. For example, dumping a mysql database to a file in the backup dir.

**export_postexec**: Not implemented. Shell command to exec after pushing data

### Assumptions

Datadb makes some assumptions about it's environment.

* `rsync`, `ssh`, `tar`, and `curl` commands are assumed to be in $PATH
* For rsync operations, the ssh private key file at `/root/.ssh/datadb.key` is used.

### CLI Usage

* Restore from backup: `datadb [--force] <profile_name> restore`

Restore operations have a degree of sanity checking. Upon a successful restore, a file named *.datadb.lock* will be created in the local dir. Datadb checks for this file before doing restore operations, to prevent overwriting live data with an old backup. This check can be overridden with the `--force` command line option.

* Backup to remote server: `datadb <profile_name> backup`
* Check status: `datadb <profile_name> status`

Command line usage is agnostic to the underlying transport protocol used.

## TODO

* Fix hard coded stuff mentioned above
* Support config file-less usage
* Sync all command
* Option to override config path
* Nicer config parsing
* Implement security
* Implement pre/post exec functions