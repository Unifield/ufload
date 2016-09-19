import ConfigParser, argparse, os, sys
import ufload

def _progress(p):
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

def _cmdLs(args):
    if not _required(args, [ 'user', 'pw', 'oc' ]):
        return 2

    files = ufload.cloud.list_files(user=args.user,
                                    pw=args.pw,
                                    where=_ocToDir(args.oc),
                                    instances=args.i)
    if len(files) == 0:
        print "No files found."
        return 1

    for f in files:
        print f

    return 0

def main():
    parser = argparse.ArgumentParser(prog='ufload')

    parser.add_argument("-user", help="ownCloud username")
    parser.add_argument("-pw", help="ownCloud password")
    parser.add_argument("-oc", help="ownCloud directory (OCG, OCA, OCB accepted as shortcuts)")

    sub = parser.add_subparsers(title='subcommands',
                                description='valid subcommands',
                                help='additional help')

    pLs = sub.add_parser('ls', help="List available backups")
    pLs.add_argument("-i", action="append", help="instances to work on")
    pLs.set_defaults(func=_cmdLs)

    # read from $HOME/.ufload first
    conffile = ConfigParser.SafeConfigParser()
    conffile.read('%s/.ufload' % os.environ['HOME'])
    for subp, subn in ((parser, "global"), (pLs, "ls")):
        if conffile.has_section(subn):
            subp.set_defaults(**dict(conffile.items(subn)))

    # now that the config file is applied, parse from cmdline
    args = parser.parse_args()
    if hasattr(args, "func"):
        sys.exit(args.func(args))
