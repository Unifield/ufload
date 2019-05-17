import os, sys, subprocess, tempfile, hashlib, urllib, oerplib, zipfile, base64
import ufload

def _run_out(args, cmd):
    try:
        return subprocess.check_output(cmd, env=pg_pass(args), stderr=subprocess.STDOUT).split('\n')
    except Exception as e:
        return []

def _run(args, cmd, get_out=False, silent=False):
    if args.show:
        ufload.progress("Would run: " + str(cmd))
        rc = 0
    else:
        if silent or get_out:
            out = ""
            try:
                out = subprocess.check_output(cmd, env=pg_pass(args), stderr=subprocess.STDOUT)
                return 0, out
            except subprocess.CalledProcessError as exc:
                return exc.returncode, exc.output
        else:
            rc = subprocess.call(cmd, env=pg_pass(args))
    return rc

# Find exe by looking in the PATH, prefering the one
# installed by the AIO (UF6.0 style or pre-UF6 style)
def _find_exe(exe):
    if sys.platform == "win32":
        path = [ r'c:\Program Files (x86)\msf\Unifield\pgsql\bin',
                 r'd:\MSF Data\Unifield\PostgreSQL\bin' ]
        path.extend(os.environ['PATH'].split(';'))
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

def mkpsql(args, sql, db='postgres'):
    cmd = [ _find_exe('psql') ] + pg_common(args)
    cmd.append('-q')
    cmd.append('-t')
    cmd.append('-c')
    cmd.append(sql)
    cmd.append(db)
    return cmd

def mkpsql_file(args, file, db='postgres'):
    cmd = [ _find_exe('psql') ] + pg_common(args)
    cmd.append('-q')
    cmd.append('-t')
    cmd.append('-c')
    cmd.append('--file=%s' % file)
    cmd.append(db)
    return cmd

def psql(args, sql, db='postgres', silent=False):
    return _run(args, mkpsql(args, sql, db), silent)

def psql_file(args, file, db='postgres', silent=False):
    return _run(args, mkpsql_file(args, file, db), silent)
    
def load_zip_into(args, db, f, sz):
    tot = float(sz)
    if sz == 0:
        ufload.progress("Note: No progress percent available.")
    
    db2 = db + "_" + str(os.getpid())

    ufload.progress("Create database "+db2)
    rc = psql(args, 'CREATE DATABASE \"%s\"' % db2)
    if rc != 0:
        return rc

    # From here out, we need a try block, so that we can drop
    # the temp db if anything went wrong
    try:
        ufload.progress("Restoring into %s" % db2)
    
        cmd = pg_restore(args)
        cmd.append('--no-acl')
        cmd.append('--no-owner')
        cmd.append('-d')
        cmd.append(db2)
        cmd.append('-n')
        cmd.append('public')
        cmd.append('-S')
        cmd.append(args.db_user)
        cmd.append('--disable-triggers')

        if not args.show:
            with open(f, 'rb') as fileobj:
                z = zipfile.ZipFile(fileobj)
                names = z.namelist()
                fn = names[0]
                #z.extract(fn)
                z.extractall()
                z.close()
                del z
            os.unlink(f)

            cmd.append(fn)

            ufload.progress("Starting restore. This will take some time.")
            try:
                rc =_run(args, cmd)
            except KeyboardInterrupt:
                raise dbException(1)

            # clean up the temp file
            try:
                os.unlink(fn)
            except OSError:
                pass

        else:
            ufload.progress("Would run: "+ str(cmd))
            rc = 0

        rcstr = "ok"
        if rc != 0:
            rcstr = "error %d" % rc
        ufload.progress("Restore finished with result code: %s" % rcstr)
        _checkrc(rc)

        # Let's delete uninstalled versions
        rc = psql(args, 'DELETE FROM sync_client_version WHERE state!=\'installed\'', db2)
        _checkrc(rc)

        # Analyze DB to optimize queries (rebuild indexes...)
        if args.analyze:
            ufload.progress("Analyzing database %s and rebuilding indexes" % db2)
            rc = psql(args, 'ANALYZE', db2)
            _checkrc(rc)

        _checkrc(delive(args, db2))
        
        ufload.progress("Drop database "+db)
        killCons(args, db)
        rc = psql(args, 'DROP DATABASE IF EXISTS \"%s\"'%db)
        # First, revoke CONNECT rights to the DB so there won't be any auto-connect issues
        psql(args, 'GRANT CONNECT ON DATABASE %s FROM public' % db, 'postgres', True)
        _checkrc(rc)

        ufload.progress("Rename database %s to %s" % (db2, db))
        rc = psql(args, 'ALTER DATABASE \"%s\" RENAME TO \"%s\"' % (db2, db))
        _checkrc(rc)

        # analyze db
        psql(args, 'analyze', db, silent=True)

        for d in _allDbs(args):
            if d.startswith(db) and d!=db:
                ufload.progress("Cleaning other database for instance %s: %s" % (db, d))
                killCons(args, d)
                rc = psql(args, 'DROP DATABASE IF EXISTS \"%s\"' % d)
                if rc != 0:
                    return rc

        return 0
    except Exception as e:
        ufload.progress("Unexpected error %s" % sys.exc_info()[0])
        # something went wrong, so drop the temp table
        ufload.progress("Cleanup: dropping table %s" % db2)
        killCons(args, db2)
        psql(args, 'DROP DATABASE \"%s\"'%db2)
        return e.rc


def load_dump_into(args, db, f, sz):
    tot = float(sz)
    if sz == 0:
        ufload.progress("Note: No progress percent available.")

    db2 = db + "_" + str(os.getpid())

    ufload.progress("Create database " + db2)
    rc = psql(args, 'CREATE DATABASE \"%s\"' % db2)
    if rc != 0:
        return rc

    # From here out, we need a try block, so that we can drop
    # the temp db if anything went wrong
    try:
        ufload.progress("Restoring into %s" % db2)

        cmd = pg_restore(args)
        cmd.append('--no-acl')
        cmd.append('--no-owner')
        cmd.append('-d')
        cmd.append(db2)
        cmd.append('-n')
        cmd.append('public')
        cmd.append('-S')
        cmd.append(args.db_user)
        cmd.append('--disable-triggers')

        # Windows pg_restore gets confused when reading from a pipe,
        # so write to a temp file first.
        if sys.platform == "win32":
            tf = tempfile.NamedTemporaryFile(delete=False)
            if not args.show:

                n = 0
                next = 10
                for chunk in iter(lambda: f.read(1024 * 1024), b''):
                    tf.write(chunk)
                    n += len(chunk)
                    if tot != 0:
                        pct = n / tot * 100
                        if pct > next:
                            ufload.progress("Loading data: %d%%" % int(pct))
                            next = int(pct / 10) * 10 + 10

            tf.close()
            cmd.append(tf.name)

            ufload.progress("Starting restore. This will take some time.")
            try:
                rc = _run(args, cmd)
            except KeyboardInterrupt:
                raise dbException(1)

            # clean up the temp file
            try:
                os.unlink(tf.name)
            except OSError:
                pass
        else:
            # For non-Windows, feed the data in via pipe so that we have
            # some progress indication.
            if not args.show:
                p = subprocess.Popen(cmd, bufsize=1024 * 1024 * 10,
                                     stdin=subprocess.PIPE,
                                     stdout=sys.stdout,
                                     stderr=sys.stderr,
                                     env=pg_pass(args))

                n = 0
                next = 10
                for chunk in iter(lambda: f.read(8192), b''):
                    try:
                        p.stdin.write(chunk)
                    except IOError:
                        break
                    n += len(chunk)
                    if tot != 0:
                        pct = n / tot * 100
                        if pct > next:
                            ufload.progress("Restoring: %d%%" % int(pct))
                            next = int(pct / 10) * 10 + 10

                p.stdin.close()
                ufload.progress("Restoring: 100%")
                ufload.progress("Waiting for Postgres to finish restore")
                rc = p.wait()
            else:
                ufload.progress("Would run: " + str(cmd))
                rc = 0

        rcstr = "ok"
        if rc != 0:
            rcstr = "error %d" % rc
        ufload.progress("Restore finished with result code: %s" % rcstr)
        _checkrc(rc)

        #USELESS FOR SYNC SERVER Let's delete uninstalled versions
        #rc = psql(args, 'DELETE FROM sync_server_version WHERE state!=\'installed\'', db)
        #_checkrc(rc)

        _checkrc(delive(args, db2))

        ufload.progress("Drop database " + db)
        killCons(args, db)
        rc = psql(args, 'DROP DATABASE IF EXISTS \"%s\"' % db)
        _checkrc(rc)

        ufload.progress("Rename database %s to %s" % (db2, db))
        rc = psql(args, 'ALTER DATABASE \"%s\" RENAME TO \"%s\"' % (db2, db))
        _checkrc(rc)

        return 0
    except dbException as e:
        # something went wrong, so drop the temp table
        ufload.progress("Unexpected error %s" % sys.exc_info()[0])
        ufload.progress("Cleanup: dropping db %s" % db2)
        killCons(args, db2)
        psql(args, 'DROP DATABASE \"%s\"' % db2)
        return e.rc
    except:
        ufload.progress("Unexpected error %s" % sys.exc_info()[0])
        ufload.progress("Cleanup: dropping db %s" % db2)
        killCons(args, db2)
        psql(args, 'DROP DATABASE \"%s\"' % db2)
        return 1

# De-live uses psql to change a restored database taken from a live backup
# into a non-production, non-live database. It:
# 1. stomps all existing passwords
# 2. changes the sync connection to a local one
# 3. removes cron jobs for backups and sync
# 4. set the backup directory
def delive(args, db):
    if args.live:
        ufload.progress("*** WARNING: The restored database has LIVE passwords and LIVE syncing.")
        if args.sync:
            ufload.progress("(please note that ufload is not able to connect to the sync server using live passwords, please connect manually)")
        return 0
    
    adminuser = args.adminuser.lower()
    port = 8069
    if args.sync_xmlrpcport:
        port = int(args.sync_xmlrpcport)

    # change the sync config to local
    if args.db_prefix:
        pfx = args.db_prefix + '_'
    else:
        pfx = ''
    rc = psql(args, 'update sync_client_sync_server_connection set automatic_patching = \'f\', protocol = \'xmlrpc\', login = \'%s\', database = \'%sSYNC_SERVER_LOCAL\', host = \'127.0.0.1\', port = %d;' % (adminuser, pfx, port), db)
    if rc != 0:
        return rc

    # disable cron jobs
    rc = psql(args, 'update ir_cron set active = \'f\' where model = \'backup.config\';', db)
    if rc != 0:
        return rc
    rc = psql(args, 'update ir_cron set active = \'f\' where model = \'msf.instance.cloud\';', db)
    if rc != 0:
        return rc
    rc = psql(args, 'update ir_cron set active = \'f\' where model = \'sync.client.entity\';', db)
    if rc != 0:
        return rc
    rc = psql(args, 'update ir_cron set active = \'f\' where model = \'stock.mission.report\';', db)
    if rc != 0:
        return rc

    # Now we check for arguments allowing auto-sync and silent-upgrade
    if args.autosync:
        ss = 'SYNC_SERVER_LOCAL'
        if args.ss:
            ss = args.ss
        activate_autosync(args, db, ss)
        rc = psql(args, 'update ir_cron set active = \'t\', interval_type = \'hours\', interval_number = 2, nextcall = current_timestamp + interval \'1 hour\' where model = \'sync.client.entity\' and function = \'sync_threaded\';', db)
        if rc != 0:
            return rc
        rc = psql(args, 'update sync_client_sync_server_connection SET host = \'127.0.0.1\', database = \'%s\';' % ss, db)
    if args.silentupgrade:
        if not args.autosync:
            ufload.progress("*** WARNING: Silent upgrade is enabled, but auto sync is not.")
        rc = psql(args, 'update sync_client_sync_server_connection set automatic_patching = \'t\';', db)
        if rc != 0:
            return rc

    # Set the backup directory
    directory = "E'd:\\\\'"
    if sys.platform != "win32" and args.db_host in [ None, 'ct0', 'localhost' ]:
        # when loading on non-windows, to a local database, use /tmp
        directory = '\'/tmp\''
    
    rc = psql(args, 'update backup_config set beforemanualsync=\'f\', beforepatching=\'f\', aftermanualsync=\'f\', beforeautomaticsync=\'f\', afterautomaticsync=\'f\', name = %s;' % directory, db)
    if rc != 0:
        return rc

    if args.nopwreset:
        ufload.progress("*** WARNING: The restored database has LIVE passwords.")
	return 0

    # set the username of the admin account
    rc = psql(args, 'update res_users set login = \'%s\' where id = 1;' % adminuser, db)
    if rc != 0:
        return rc

    # put the chosen password into all users
    rc = psql(args, 'update res_users set password = \'%s\';' % args.adminpw, db)
    if rc != 0:
        return rc

    # ok, delive finished with no problems
    return 0

def activate_autosync(args, db, ss):
    rc = psql(args,
              'update ir_cron set active = \'t\', interval_type = \'hours\', interval_number = 2, nextcall = current_timestamp + interval \'1 hour\' where model = \'sync.client.entity\' and function = \'sync_threaded\';',
              db)
    if rc != 0:
        return rc

    rc = psql(args,
              'update sync_client_sync_server_connection SET host = \'127.0.0.1\', database = \'%s\';' % ss,
              db)

    return rc

def activate_silentupgrade(args, db):
    rc = psql(args, 'update sync_client_sync_server_connection set automatic_patching = \'t\';', db)

    if not args.autosync:
        ufload.progress("*** WARNING: Silent upgrade is enabled, but auto sync is not.")

    return rc


def _checkrc(rc):
    if rc != 0:
        raise dbException(rc)

class dbException(Exception):
    def __init__(self, rc):
        self.rc = rc

def ver(args):
    v = _run_out(args, mkpsql(args, 'show server_version'))
    return v

def killCons(args, db):
    # A wacky exception for UF5: we are not superuser on Postgres, so we
    # cannot kill connections. So bounce OpenERP instead.
    if args.killconn:
        _run(args, [ 'sh', '-c', args.killconn])
        return

    # First, revoke CONNECT rights to the DB so there won't be any auto-connect issues
    psql(args, 'REVOKE CONNECT ON DATABASE %s FROM public' % db, 'postgres', True)

    # For Postgres 8, it is procpid, for 9 it is pid
    v = ver(args)
    if len(v) > 1 and ' 9.' in v[0]:
        col = 'pid'
    else:
        col = 'procpid'

    cmd = mkpsql(args, 'select %s from pg_stat_activity where datname = \'%s\';' % (col, db), 'postgres')
    for i in _run_out(args, cmd):
        try:
            pid = int(i)
            psql(args, 'select pg_terminate_backend(%s)' % pid, 'postgres', True)
        except ValueError:
            # skip lines which are not numbers
            pass

def get_hwid(args):
    if sys.platform == 'win32':
        import _winreg
        try:
            with _winreg.OpenKey(_winreg.HKEY_LOCAL_MACHINE,
                                 "SYSTEM\ControlSet001\services\eventlog\Application\openerp-web-6.0",
                                 0, _winreg.KEY_READ) as registry_key:
                hwid, regtype = _winreg.QueryValueEx(registry_key, "HardwareId")
                ufload.progress("Hardware id from registry key: %s" % hwid)
                return hwid
        except WindowsError:
            return None
    else:
        # Follow the same algorithm that Unifield uses (see sync_client.py)
        mac = []
        for line in os.popen("/sbin/ifconfig"):
            if line.find('Ether') > -1:
                mac.append(line.split()[4])
                    
        mac.sort()
        hw_hash = hashlib.md5(''.join(mac)).hexdigest()
        return hw_hash

def _db_to_instance(args, db):
    if args.db_prefix:
        db = db[len(args.db_prefix)+1:]

    ss = 'SYNC_SERVER_LOCAL'
    if args.ss:
        ss = args.ss

    if db.startswith(ss):
        return ss

    return '_'.join(db.split('_')[0:-2])

def cleanDbs(args):

    import re
    p = re.compile('^[A-Z0-9_]{8,}_[0-9]{8}_[0-9]{4}$')
    ps = re.compile('SYNC')

    nb = 0
    for d in _allDbs(args):

        m = p.match(d)
        ms = ps.search(d)

        if m == None and ms == None and d != '':
            ufload.progress("Dropping database %s" % d)
            killCons(args, d)
            rc = psql(args, 'DROP DATABASE IF EXISTS \"%s\"'%d)
            if rc != 0:
                ufload.progress("Error: unable to drop database %s" % d)
            else:
                nb = nb + 1

    return nb

def sync_link(args, hwid, db, sdb, all=False):
    instance = _db_to_instance(args, db)
    #Create the instance in the sync server if it does not already exist
    rc = psql(args, 'insert into sync_server_entity (create_uid, create_date, write_date, write_uid, user_id, name, state) SELECT 1, now(), now(), 1, 1, \'%s\', \'validated\' FROM sync_server_entity WHERE NOT EXISTS (SELECT 1 FROM sync_server_entity WHERE name = \'%s\') ' % (instance, instance), sdb )

    if rc != 0:
        ufload.progress('Unable to create the instance %s on the sync server. Please add it manually.' % instance)
        #return rc

    if all:
        # Update hardware id for every instance
        return psql(args, 'update sync_server_entity set hardware_id = \'%s\';' % hwid, sdb)
    else:
        #Update hardware id for this instance
        return psql(args, 'update sync_server_entity set hardware_id = \'%s\' where name = \'%s\';' % (hwid, instance), sdb)

# Remove all databases which come from the same instance as db
def clean(args, db):
    toClean = {}
    toKeep = {}

    i = _db_to_instance(args, db)
    toClean[i] = True
    toKeep[db] = True

    for d in _allDbs(args):
        i = _db_to_instance(args, d)
        #if not args.db_prefix and i and d not in toKeep and i in toClean:
        if i and d not in toKeep and i in toClean:
            ufload.progress("Cleaning other database for instance %s: %s" % (i, d))
            killCons(args, d)
            rc = psql(args, 'DROP DATABASE IF EXISTS \"%s\"'%d)
            if rc != 0:
                return rc
    return 0            

def _allDbs(args):
    if args.db_user:
        v = _run_out(args, mkpsql(args, 'select datname from pg_database where datdba=(select usesysid from pg_user where usename=\'%s\') and datistemplate = false and datname != \'postgres\'' % args.db_user))
    else:
        v = _run_out(args, mkpsql(args, 'select datname from pg_database where datistemplate = false and datname != \'postgres\''))
        
    return map(lambda x: x.strip(), filter(len, v))

def exists(args, db):
    v = _run_out(args, mkpsql(args, 'select datname from pg_database where datname = \'%s\'' % db))
    v = filter(len, map(lambda x: x.strip(), v))
    return len(v)==1 and v[0] == db

# These two functions read and write from a little "about" table
# where we store the size of the input file, which helps us avoid
# reloading the sync server when we don't need to.
def get_sync_server_len(args, db='SYNC_SERVER_LOCAL'):
    try:
        #First, check if the db already exists
        exist = _run_out(args, mkpsql(args, 'SELECT 1 FROM information_schema.tables  WHERE table_catalog=\'%s\' AND table_schema=\'public\' AND table_name=\'about\';' % db))
        if len(exist) < 3:
            return -1;

        l = _run_out(args, mkpsql(args, 'select length from about', db))
        if len(l) < 1:
            return -1
        return int(filter(len, l)[0])
    except subprocess.CalledProcessError:
        pass
    return -1

def write_sync_server_len(args, l, db='SYNC_SERVER_LOCAL'):
    _run_out(args, mkpsql(args, 'drop table if exists about; create table about ( length int ); insert into about values ( %d )' % l, db))

def sync_server_all_admin(args, db='SYNC_SERVER_LOCAL'):
    _run_out(args, mkpsql(args, 'update sync_server_entity set user_id = 1;', db))

def sync_server_settings(args, sync_server, db):
    _run_out(args, mkpsql(args, 'update sync_client_sync_server_connection set database = \'%s\', login=\'%s\' user_id = 1;' % (sync_server, args.adminuser.lower()) , db))

def connect_instance_to_sync_server(args, sync_server, db):
    #Temporary desactivation of auto-connect
    #return 0

    if db.startswith('SYNC_SERVER'):
        return 0

    #oerp = oerplib.OERP('127.0.0.1', protocol='xmlrpc', port=12173, version='6.0')
    ufload.progress('Connecting instance %s to %s' % (db, sync_server))
    #netrpc = oerplib.OERP('127.0.0.1', protocol='xmlrpc', port=12173, timeout=1000, version='6.0')
    netrpc = oerplib.OERP('127.0.0.1', protocol='xmlrpc', port=8069, timeout=1000, version='6.0')
    netrpc.login(args.adminuser.lower(), args.adminpw, database=db)
    conn_manager = netrpc.get('sync.client.sync_server_connection')
    conn_ids = conn_manager.search([])
    conn_manager.write(conn_ids, {'password': args.adminpw})
    conn_manager.connect()
    #netrpc.get('sync.client.entity').sync()


def _parse_dsn(dsn):
    res = {}
    for i in dsn.split():
        k,v = i.split("=")
        res[k]=v
    return res

# Copy new data from one database (identified via a DSN) to the 'archive' db
# of the current Postgres (as specified by the --db_host, etc)
def archive(args):
    v = ver(args)
    if len(v) < 1 or '9.5' not in v[0]:
        ufload.progress('Postgres 9.5 is required.')
        return 1

    for dsn in args.from_dsn:
        x = _parse_dsn(dsn)
        if 'dbname' not in x:
            ufload.progress('DSN is missing dbname.')
            return 1
    
        ufload.progress("Archive operations_event from %s" % x['dbname'])
        rc, out = _run(args, mkpsql(args, '''
create extension if not exists dblink;
insert into operations_event (instance, kind, time, remote_id, data)
  select * from
    dblink('%s', 'select instance, kind, time, id, data from operations_event') as
    table_name_is_ignored(instance character varying(64),
       kind character varying(64),
       time timestamp without time zone,
       id integer,
       data text)
    on conflict do nothing;''' % (dsn,), 'archive'), get_out=True)
        ufload.progress(_clean(out))

        ufload.progress("Archive operations_count from %s" % x['dbname'])
        rc, out = _run(args, mkpsql(args, '''
create extension if not exists dblink;
insert into operations_count (instance, kind, time, count, remote_id)
  select * from
    dblink('%s', 'select instance, kind, time, count, id from operations_count') as
    table_name_is_ignored(instance character varying(64),
       kind character varying(64),
       time timestamp without time zone,
       count integer,
       id integer)
    on conflict do nothing;''' % (dsn,), 'archive'), get_out=True)
        ufload.progress(_clean(out))

def _clean(out):
    ret = []
    for line in out.split("\n"):
        if line.strip() == "":
            continue
        if line.startswith("NOTICE:"):
            continue
        ret.append(line)
    return "\n".join(ret)


def _zipChecksum(path):
    ufload.progress("Validating patch checksum")
    with open(path, 'rb') as f:
        contents = f.read()
        # md5 accepts only chunks of 128*N bytes
        md5 = hashlib.md5()
        for i in range(0, len(contents), 8192):
            md5.update(contents[i:i + 8192])
    return md5.hexdigest()


def _zipContents(path):
    ufload.progress("Reading patch contents")
    with open(path, 'rb') as f:
        contents = f.read()
        return buffer(contents)
    #return contents



def installPatch(args, db='SYNC_SERVER_LOCAL'):
    ufload.progress("Activating update_client module on %s database" % db)
    #Install the module update_client
    rc = psql(args, "UPDATE ir_module_module SET state = 'installed' WHERE name = 'update_client'", db)
    if rc != 0:
        return rc

    v = args.version
    ufload.progress("Installing v.%s patch on %s database" % (v, db))

    patch = os.path.normpath(args.patch)

    checksum = _zipChecksum(patch)
    contents = base64.b64encode(_zipContents(patch))

    sql = "INSERT INTO sync_server_version (create_uid, create_date, write_date, write_uid, date, state, importance, name, comment, sum, patch) VALUES (1, NOW(), NOW(), 1, NOW(),  'confirmed', 'required', '%s', 'Version %s installed by ufload', '%s', '%s')" % (v, v, checksum, contents)
    # Write sql to a file
    f = open('sql.sql', 'w')
    f.write(sql)
    f.close()

    rc = psql_file(args, 'sql.sql', db)
    os.remove('sql.sql');

    if rc != 0:
        return rc
    return 0

def updateInstance(inst):
    #Call the do_login url in order to trigger the sync (should work even with wrong credentials)
    ufload.progress("Try to log into instance %s using wrong credentials" % inst)
    urllib.request("http://127.0.0.1:8061/openerp/do_login?target=/&user=ufload&show_password=ufload&db_user_pass=%s" % inst)
    return 0
