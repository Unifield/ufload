import os, sys, subprocess
import ufload

def _run(args, cmd):
    if args.show:
        print "Would run:", " ".join(cmd)
        rc = 0
    else:
        rc = subprocess.call(cmd, env=pg_pass(args))
        if rc != 0:
            print "pg_restore error code: %d" % rc
    return rc

# Find exe by looking in the PATH, prefering the one
# installed by the AIO.
def _find_exe(exe):
    if sys.platform == "win32":
        path = [ 'D:\\MSF Data\\Unifield\\PostgreSQL\\bin',
                 os.environ['PATH'].split(';') ]
        bin = exe+".exe"
    else:
        path = os.environ['PATH'].split(':')
        bin = exe

    for p in path:
        fn = os.path.join(p, bin)
        if os.path.exists(fn):
            return fn
    # return the unqualified binary name and hope for
    # the best...
    return bin

def pg_common(args):
    res = []
    if args.db_host is not None:
        res.append('-h')
        res.append(args.db_host)
    if args.db_port is not None:
        res.append('-p')
        res.append(args.db_port)
    if args.db_user is not None:
        res.append('-U')
        res.append(args.db_user)
    return res

def pg_restore(args):
    return [ _find_exe('pg_restore') ] + pg_common(args)

def pg_pass(args):
    env = os.environ.copy()
    if args.db_pw is not None:
        env['PGPASSWORD'] = args.db_pw
    return env

def psql(args, sql):
    cmd = [ _find_exe('psql') ] + pg_common(args)
    cmd.append('-q')
    cmd.append('-c')
    cmd.append(sql)
    cmd.append('postgres')
    return _run(args, cmd)
    
def load_into(args, db, f, sz):
    tot = float(sz)

    db2 = db + "_" + str(os.getpid())
    
    ufload.progress("Create database "+db2)
    rc = psql(args, 'CREATE DATABASE \"%s\"' % db2)
    if rc != 0:
        return rc

    ufload.progress("Restoring into %s" % db2)

    cmd = pg_restore(args)
    cmd.append('--no-acl')
    cmd.append('--no-owner')
    cmd.append('-d')
    cmd.append(db2)
    cmd.append('-n')
    cmd.append('public')
    
    # Windows pg_restore gets confused when reading from a pipe,
    # so write to a temp file first.
    if sys.platform == "win32":
        ufload.progress("Starting restore. This will take some time.")

        import tempfile
        tf = tempfile.NamedTemporaryFile(delete=False)
        if not args.show:
            for chunk in iter(lambda: f.read(8192), b''):
                tf.write(chunk)
        tf.close()
        ufload.progress("Temporary file %s created." % tf.name)
        cmd.append(tf.name)

        rc =_run(args, cmd)
        rcstr = "ok"
        if rc != 0:
            rcstr = "error %d" % rc
        if not args.show:
            ufload.progress("Restore done with result code: %s" % rcstr)

        try:
            os.unlink(tf.name)
        except OSError:
            pass
        return rc

    # For non-Windows, feed the data in via pipe so that we have
    # some progress indication.
    if args.show:
        print "Would run:", cmd
        rc = 0
    else:
        p = subprocess.Popen(cmd, bufsize=1024*1024*10,
                             stdin=subprocess.PIPE,
                             stdout=sys.stdout,
                             stderr=sys.stderr,
                             env=pg_pass(args))

        n = 0
        next = 10

        for chunk in iter(lambda: f.read(8192), b''):
            p.stdin.write(chunk)
            n += len(chunk)
            pct = n/tot * 100
            if pct > next:
                ufload.progress("Restoring: %d%%" % int(pct))
                next = int(pct / 10)*10 + 10
    
        p.stdin.close()
        ufload.progress("Restoring: 100%")
        ufload.progress("Waiting for Postgres to finish restore")
        rc = p.wait()

        rcstr = "ok"
        if rc != 0:
            rcstr = "error %d" % rc
            ufload.progress("Restore done with result code: %s" % rcstr)

    if rc != 0:
        return rc
    
    ufload.progress("Drop database "+db)
    rc = psql(args, 'DROP DATABASE IF EXISTS \"%s\"'%db)
    if rc != 0:
        return rc

    ufload.progress("Rename database %s to %s" % (db2, db))
    rc = psql(args, 'ALTER DATABASE \"%s\" RENAME TO \"%s\"'%(db2, db))

    return rc
