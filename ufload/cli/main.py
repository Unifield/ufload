import ConfigParser, argparse, os, sys
import requests
import requests.auth
import subprocess

import ufload

def _home():
    if sys.platform == "win32" and 'USERPROFILE' in os.environ:
        return os.environ['USERPROFILE']
    return os.environ['HOME']

_logs = []
def _progress(p):
    global _logs
    print >> sys.stderr, p
    _logs.append(p)

ufload.progress = _progress

def _ocToDir(oc):
    x = oc.lower()
    if x == 'oca':
        return 'OCA_Backups'
    elif x == 'ocb':
        return 'OCB_Backups'
    elif x == 'ocg':
        return 'OCG_Backups'
    else:
        # no OC abbrev, assume this is a real directory name
        return oc

def _required(args, req):
    err = 0
    for r in req:
        if getattr(args, r) is None:
            r = r.replace("_", "-")
            ufload.progress('Argument --%s is required for this sub-command.' % r)
            err += 1
    return err == 0

# Turn
# ../databases/OCG_MM1_WA-20160831-220427-A-UF2.1-2p3.dump into OCG_MM1_WA_20160831_2204
def _file_to_db(args, fn):
    fn = os.path.basename(fn)
    x = fn.split('-')
    if len(x) < 2 or len(x[2]) != 6:
        return None
    db = "_".join([ x[0], x[1], x[2][0:4]])
    if args.db_prefix:
        return args.db_prefix + "_" + db
    return db

def _cmdArchive(args):
    if not _required(args, [ 'from_dsn' ]):
        return 2
    return ufload.db.archive(args)

def _cmdRestore(args):
    if args.sync:
        if not _required(args, [ 'syncuser', 'syncpw' ]):
            return 2

    if args.file is not None:
        rc, dbs = _fileRestore(args)
    else:
        rc, dbs = _multiRestore(args)

    if rc != 0:
        return rc

    if args.sync:
        rc = _syncRestore(args, dbs)

    return rc

def _fileRestore(args):
    # Find the instance name we are loading into
    if args.i is not None:
        if len(args.i) != 1:
            ufload.progress("Expected only one -i argument.")
            return 3, None
        db = args.i[0]
    else:
        db = _file_to_db(args, args.file)
        if db is None:
            ufload.progress("Could not set the instance from the filename. Use -i to specify it.")
            return 3, None

    try:
        statinfo = os.stat(args.file)
    except OSError as e:
        ufload.progress("Could not find file size: "+str(e))
        return 1, None

    with open(args.file, 'rb') as f:
        rc = ufload.db.load_into(args, db, f, statinfo.st_size)

    if not args.noclean:
        rc = ufload.db.clean(args, db)

    if args.notify:
        subprocess.call([ args.notify, db ])

    if rc == 0:
        return 0, [ db ]
    else:
        return rc, None

def _multiRestore(args):
    if not _required(args, [ 'user', 'pw', 'oc' ]):
        ufload.progress('With no -file argument, ownCloud login info is needed.')
        return 2, None

    if args.i is None:
        ufload.progress("Multiple Instance restore for all instances in %s" % args.oc)
    else:
        ufload.progress("Multiple Instance restore for instances matching: %s" % " or ".join(args.i))

    instances = ufload.cloud.list_files(user=args.user,
                                    pw=args.pw,
                                    where=_ocToDir(args.oc),
                                    instances=args.i)
    ufload.progress("Instances to be restored: %s" % ", ".join(instances.keys()))
    dbs=[]
    for i in instances:
        files_for_instance = instances[i]
        for j in files_for_instance:
            ufload.progress("Trying file %s" % j[1])
            n = ufload.cloud.peek_inside_file(j[0], j[1],
                                           user=args.user,
                                           pw=args.pw,
                                           where=_ocToDir(args.oc))
            if n is None:
                # no dump inside of zip, try the next one
                continue

            db = _file_to_db(args, str(n))
            if ufload.db.exists(args, db):
                ufload.progress("Database %s already exists." % db)
                break
            else:
                ufload.progress("Database %s does not exist, restoring." % db)

            f, sz = ufload.cloud.openDumpInZip(j[0], j[1],
                                           user=args.user,
                                           pw=args.pw,
                                           where=_ocToDir(args.oc))
            if f is None:
                continue

            db = _file_to_db(args, f.name)
            if db is None:
                ufload.progress("Bad filename %s. Skipping." % f.name)
                continue

            rc = ufload.db.load_into(args, db, f, sz)
            if rc == 0:
                dbs.append(db)

                if not args.noclean:
                    rc = ufload.db.clean(args, db)

                if args.notify:
                    subprocess.call([ args.notify, db ])

                # We got a good load, so go to the next instance.
                break

    return 0, dbs

def _syncRestore(args, dbs):
    if args.db_prefix:
        sdb = '%s_SYNC_SERVER_LOCAL' % args.db_prefix
    else:
        sdb = 'SYNC_SERVER_LOCAL'

    url = "http://sync-prod_dump.uf5.unifield.org/SYNC_SERVER_LIGHT_WITH_MASTER"
    try:
        r = requests.head(url,
                          auth=requests.auth.HTTPBasicAuth(args.syncuser, args.syncpw))
        if r.status_code != 200:
	    ufload.progress("HTTP HEAD error: %s" % r.status_code)
            return 1
    except KeyboardInterrupt as e:
        raise e
    except Exception as  e:
        ufload.progress("Failed to fetch sync server: " + str(e))
        return 1

    sz = int(r.headers.get('content-length', 0))
    szdb = ufload.db.get_sync_server_len(args, sdb)

    if szdb == sz:
        ufload.progress("Sync server is up to date.")
        return 0

    r = requests.get(url,
                     auth=requests.auth.HTTPBasicAuth(args.syncuser, args.syncpw),
                     stream=True)
    if r.status_code != 200:
	ufload.progress("HTTP GET error: %s" % r.status_code)
        return 1
    rc = ufload.db.load_into(args, sdb, r.raw, sz)
    if rc != 0:
        return rc
    ufload.db.write_sync_server_len(args, sz, sdb)

    return _syncLink(args, dbs, sdb)

# separate function to make testing easier
def _syncLink(args, dbs, sdb):
    # Hook up all the databases we are currently working on
    hwid = ufload.db.get_hwid(args)
    if hwid is None:
        ufload.progress("No hardware id available, you will need to manually link your instances to %s." % sdb)
        return 0

    for db in dbs:
        rc = ufload.db.sync_link(args, hwid, db, sdb)
        if rc != 0:
            return rc
    return 0

def _cmdLs(args):
    if not _required(args, [ 'user', 'pw', 'oc' ]):
        return 2

    instances = ufload.cloud.list_files(user=args.user,
                                    pw=args.pw,
                                    where=_ocToDir(args.oc),
                                    instances=args.i)
    if len(instances) == 0:
        ufload.progress("No files found.")
        return 1

    for i in instances:
        for j in instances[i]:
            n = ufload.cloud.peek_inside_file(j[0], j[1],
                                              user=args.user,
                                              pw=args.pw,
                                              where=_ocToDir(args.oc))
            print ": ".join((j[1], str(n)))
            # only show the latest for each one
            break

    return 0

def parse():
    parser = argparse.ArgumentParser(prog='ufload')

    parser.add_argument("-user", help="ownCloud username")
    parser.add_argument("-pw", help="ownCloud password")
    parser.add_argument("-oc", help="ownCloud directory (OCG, OCA, OCB accepted as shortcuts)")

    parser.add_argument("-syncuser", help="username to access the sync server backup")
    parser.add_argument("-syncpw", help="password to access the sync server backup")

    parser.add_argument("-db-host", help="Postgres host")
    parser.add_argument("-db-port", help="Postgres port")
    parser.add_argument("-db-user", help="Postgres user")
    parser.add_argument("-db-pw", help="Postgres password")
    parser.add_argument("-db-prefix", help="Prefix to put on database names")
    parser.add_argument("-killconn", help="The command to run kill connections to the databases.")
    parser.add_argument("-remote", help="Remote log server")
    parser.add_argument("-n", dest='show', action='store_true', help="no real work; only show what would happen")

    sub = parser.add_subparsers(title='subcommands',
                                description='valid subcommands',
                                help='additional help')

    pLs = sub.add_parser('ls', help="List the most recent backup")
    pLs.add_argument("-i", action="append", help="instances to work on (matched as a substring, default = all)")
    pLs.set_defaults(func=_cmdLs)

    pRestore = sub.add_parser('restore', help="Restore a database from ownCloud or a file")
    pRestore.add_argument("-i", action="append", help="instances to work on (matched as a substring)")
    pRestore.add_argument("-file", help="the file to restore (disabled ownCloud downloading)")
    pRestore.add_argument("-adminuser", default='admin', help="the new admin username in the newly restored database")
    pRestore.add_argument("-adminpw", default='admin', help="the password to set into the newly restored database")
    pRestore.add_argument("-live", dest='live', action='store_true', help="do not take the normal actions to make a restore into a non-production instance")
    pRestore.add_argument("-no-clean", dest='noclean', action='store_true', help="do not clean up older databases for the loaded instances")
    pRestore.add_argument("-load-sync-server", dest='sync', action='store_true', help="set up a local sync server")
    pRestore.add_argument("-notify", dest='notify', help="run this script on each restored database")
    pRestore.set_defaults(func=_cmdRestore)
    
    pArchive = sub.add_parser('archive', help="Copy new data into the database.")
    pArchive.add_argument("-from-dsn", action="append", help="the database to copy from (in the form of a DSN: 'hostaddr=x dbname=x user=x password=x')")
    pArchive.set_defaults(func=_cmdArchive)

    # read from $HOME/.ufload first
    conffile = ConfigParser.SafeConfigParser()
    if sys.platform == "win32":
        conffile.read('%s/ufload.txt' % _home())
    else:
        conffile.read('%s/.ufload' % _home())

    for subp, subn in ((parser, "owncloud"),
                       (parser, "postgres"),
                       (parser, "logs"),
                       (parser, "sync"),
                       (pLs, "ls"),
                       (pRestore, "restore"),
                       (pArchive, "archive")):
        if conffile.has_section(subn):
            subp.set_defaults(**dict(conffile.items(subn)))

    # now that the config file is applied, parse from cmdline
    return parser.parse_args()

def main():
    args = parse()
    if hasattr(args, "func"):
        try:
            rc = args.func(args)
        except KeyboardInterrupt:
            rc = 1

    if args.remote:
        import socket
        hostname = socket.gethostname() or 'unknown'
        ufload.progress("Will exit with result code: %d" % rc)
        ufload.progress("Posting logs to remote server.")
        requests.post(args.remote+"?who=%s"%hostname, data='\n'.join(_logs))

    sys.exit(rc)
