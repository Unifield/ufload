import ConfigParser, argparse, os, sys
import subprocess
import binascii

import ufload

def _progress(p):
    if len(p)>0 and p[0] == "\r":
        # No \n please.
        sys.stderr.write(p)
    else:
        print >> sys.stderr, p

ufload.progress = _progress

def _ocToDir(oc):
    x = oc.lower()
    if x == 'oca':
        return 'OCA_Backups'
    elif x == 'ocb':
        return 'OCB_Backups'
    elif x == 'ocg':
        return 'UNIFIELD-BACKUP'
    else:
        # no OC abbrev, assume this is a real directory name
        return oc

def _required(args, req):
    err = 0
    for r in req:
        if getattr(args, r) is None:
            print 'Argument %s is required for this sub-command.' % r
            err += 1
    return err == 0

# Turn
# ../databases/OCG_MM1_WA-20160831-220427-A-UF2.1-2p3.dump into OCG_MM1_WA
def _find_instance(fn):
    fn = os.path.basename(fn)
    if '-' not in fn:
        return None
    return fn.split('-')[0]

def _cmdRestore(args):
    if args.file is not None:
        # Find the instance name we are loading into
        if args.i is not None:
            if len(args.i) != 1:
                print "Expected only one -i argument."
                return 3
            if "%" in args.i[0]:
                print "Wildcards not allowed when using -i to set the database."
                return 3
            db = args.i[0]
        else:
            db = _find_instance(args.file)
            if db is None:
                print "Could not guess instance from filename. Use -i to specify it."
                return 3

        try:
            statinfo = os.stat(args.file)
        except OSError as e:
            ufload.progress("Could not find file size: "+str(e))
            return 1
        
        with open(args.file, 'rb') as f:
            return ufload.db.load_into(args, db, f, statinfo.st_size)

    # if we got here, we are in fact doing a multi-restore
    if not _required(args, [ 'user', 'pw', 'oc' ]):
        print 'With no -file argument, ownCloud login info is needed.'
        return 2

    if args.i is None:
        ufload.progress("Multiple Instance restore for all instances in %s" % args.oc)
    else:
        ufload.progress("Multiple Instance restore for instances: %s" % ", ".join(args.i))

    instances = ufload.cloud.list_files(user=args.user,
                                    pw=args.pw,
                                    where=_ocToDir(args.oc),
                                    instances=args.i)
    ufload.progress("Instances to be restored: %s" % ", ".join(instances.keys()))
    for i in instances:
        ufload.progress("Restore to instance %s" % i)
        for j in instances[i]:
            ufload.progress("Trying file %s" % j[1])
            f, sz = ufload.cloud.openDumpInZip(j[0],
                                           user=args.user,
                                           pw=args.pw,
                                           where=_ocToDir(args.oc))
            rc = ufload.db.load_into(args, i, f, sz)
            if rc == 0:
                # We got a good load, so go to the next instance.
                break
    return 1

def _cmdLs(args):
    if not _required(args, [ 'user', 'pw', 'oc' ]):
        return 2

    instances = ufload.cloud.list_files(user=args.user,
                                    pw=args.pw,
                                    where=_ocToDir(args.oc),
                                    instances=args.i)
    if len(instances) == 0:
        print "No files found."
        return 1

    for i in instances:
        for j in instances[i]:
            print j[1]
    return 0

def main():
    parser = argparse.ArgumentParser(prog='ufload')

    parser.add_argument("-user", help="ownCloud username")
    parser.add_argument("-pw", help="ownCloud password")
    parser.add_argument("-oc", help="ownCloud directory (OCG, OCA, OCB accepted as shortcuts)")

    parser.add_argument("-db-host", help="Postgres host")
    parser.add_argument("-db-port", help="Postgres port")
    parser.add_argument("-db-user", help="Postgres user")
    parser.add_argument("-db-pw", help="Postgres password")

    sub = parser.add_subparsers(title='subcommands',
                                description='valid subcommands',
                                help='additional help')

    pLs = sub.add_parser('ls', help="List available backups")
    pLs.add_argument("-i", action="append", help="instances to work on (use % as a wildcard)")
    pLs.set_defaults(func=_cmdLs)

    pRestore = sub.add_parser('restore', help="Restore a database from ownCloud or a file")
    pRestore.add_argument("-i", action="append", help="instances to work on (use % as a wildcard)")
    pRestore.add_argument("-n", dest='show', action='store_true', help="no real work; only show what would happen")
    pRestore.add_argument("-file", help="the file to restore (disabled ownCloud downloading)")
    pRestore.set_defaults(func=_cmdRestore)

    # read from $HOME/.ufload first
    conffile = ConfigParser.SafeConfigParser()
    conffile.read('%s/.ufload' % home())
    for subp, subn in ((parser, "owncloud"),
                       (parser, "postgres"),
                       (pLs, "ls"),
                       (pRestore, "restore")):
        if conffile.has_section(subn):
            subp.set_defaults(**dict(conffile.items(subn)))

    # now that the config file is applied, parse from cmdline
    args = parser.parse_args()
    if hasattr(args, "func"):
        sys.exit(args.func(args))

def home():
    if sys.platform == "win32" and 'USERPROFILE' in os.environ:
        return os.environ['USERPROFILE']
    return os.environ['HOME']




