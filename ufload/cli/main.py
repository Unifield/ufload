import ConfigParser, argparse, os, sys, oerplib
import requests
import requests.auth
import subprocess
import shutil
import time
import re
import socket

import ufload

def _home():
    if sys.platform == "win32" and 'USERPROFILE' in os.environ:
        return os.environ['USERPROFILE']
    return os.environ['HOME']

_logs = []
args=[]
starttime = time.strftime('%Y%m%d%H%M%S')
def _progress(p):
    global _logs, args
    p = time.strftime('%H:%M:%S') + ': ' + p
    print >> sys.stderr, p
    _logs.append(p)

    if args.local:
        #Create directory if necessary
        try:
            os.stat(args.local)
        except:
            os.mkdir(args.local)
        #Create log file (if it does not exist, else append to existing file)
        filename = '%s/uf_%s.log' % (args.local, starttime)
        #Write logs to file
        with open(filename, 'ab') as file:
            #file.write('\n'.join(_logs))
            file.write('%s\n' % p)

ufload.progress = _progress

def _ocToDir(oc):
    x = oc.lower()
    if x == 'oca':
        return 'OCA_Backups'
    elif x == 'ocb':
        return 'OCB_Backups'
    elif x == 'ocg':
        return 'OCG_Backups'
    elif x == 'ocp':
        return 'OCP_Backups'
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
    #if len(x) < 2 or len(x[2]) != 6:
    #    return None

    if len(x) > 0 and args.nosuffix:
        db = x[0]
    elif len(x) > 1 and len(x[2]) == 6:
        db = "_".join([ x[0], x[1], x[2][0:4]])
    else:
        db = fn[:-5]

    if args.db_prefix:
        return args.db_prefix + "_" + db
    return db


def _cmdArchive(args):
    if not _required(args, [ 'from_dsn' ]):
        return 2
    return ufload.db.archive(args)

def _cmdRestore(args):
    # if args.sync:
    #     if not _required(args, [ 'syncuser', 'syncpw' ]):
    #         return 2

    if args.autosync is not None:
        if not _required(args, [ 'sync' ]):
            if not _required(args, [ 'synclight' ]):
                ufload.progress("Load sync server (-load-sync-server or -load-sync-server-no-update) argument is mandatory for auto-sync")
                return 2

    # if the parameter nopwreset is not defined, adminpw and userspw are mandatory.
    if not args.nopwreset:
        if args.adminpw is None or args.userspw is None:
            ufload.progress("-adminpw AND -userspw are mandatory if -nopwreset is not set")
            return 2

    if args.file is not None:
        rc, dbs = _fileRestore(args)
    elif args.dir is not None:
        rc, dbs = _dirRestore(args)
    else:
        rc, dbs = _multiRestore(args)

    if rc != 0:
        return rc

    ss = 'SYNC_SERVER_LOCAL'
    if args.ss is not None:
        ss = args.ss

    if args.sync or args.synclight:
        # Restore a sync server (LIGHT WITH MASTER)
        rc = _syncRestore(args, dbs, ss)

    if args.sync or args.synclight or args.autosync or args.ss is not None:
        # Update instances sync settings
        for db in dbs:
            ufload._progress("Connection settings for %s" % db)
            #Defines sync server connection settings on each instance
            ufload.db.sync_server_settings(args, ss, db)
            if args.sync or args.autosync or args.synclight or args.ss is not None:
                #Connects each instance to the sync server (and sets pwd)
                ufload.db.connect_instance_to_sync_server(args, ss, db)

        _syncLink(args, dbs, ss)

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
        rc = ufload.db.load_dump_into(args, db, f, statinfo.st_size)

    if not args.noclean:
        rc = ufload.db.clean(args, db)

    if args.notify:
        subprocess.call([ args.notify, db ])

    if rc == 0:
        return 0, [ db ]
    else:
        return rc, None

def _dirRestore(args):
    files = os.listdir(args.dir)
    dbs = []
    atleastone = False

    for file in files:
        db = _file_to_db(args, file)
        fullfile = '%s/%s' % (args.dir, file)
        if db is None:
            ufload.progress("Could not set the instance from the file %s." % file)
        else:
            dbs.append(db)
            atleastone = True

        try:
            statinfo = os.stat(fullfile)
            sz = statinfo.st_size
        except OSError as e:
            ufload.progress("Could not find file size: " + str(e))
            sz = 0
            return 1, None

        with open(fullfile, 'rb') as f:
            rc = ufload.db.load_dump_into(args, db, f, sz)

        if not args.noclean:
            rc = ufload.db.clean(args, db)

        if args.notify:
            subprocess.call([args.notify, db])

    if atleastone:
        return 0, dbs
    else:
        return 2, None

def _multiRestore(args):
    if not _required(args, [ 'user', 'pw' ]):
        ufload.progress('With no -file or -dir argument, cloud credentials are mandatory.')
        return 2, None

    if args.i is None:
        if not _required(args, [ 'oc' ]):
            ufload.progress('With no -file or -dir argument, you must use -i or -oc.')
            return 2, None
        ufload.progress("Multiple Instance restore for all instances in %s" % args.oc)
    else:
        if not args.oc:
            ufload.progress('Argument -oc not provided, please note that ufload will look for a OC pattern in the -i arguments (you might want to avoid partial substrings)')
        ufload.progress("Multiple Instance restore for instances matching: %s" % " or ".join(args.i))

    if args.workingdir:
        try:
            os.mkdir(args.workingdir)
        except:
            pass
        os.chdir(args.workingdir)

    #Create a temp directory to unzip files
    try:
        os.mkdir('ufload_temp')
    except:
        pass
    #Change working directory
    os.chdir('ufload_temp')

    #Cloud access
    info = ufload.cloud.get_cloud_info(args)
    ufload.progress('site=%s - path=%s - dir=%s' % (info.get('site'), info.get('path'), info.get('dir')))
    dav = ufload.cloud.get_onedrive_connection(args)

    if not args.oc:
        #foreach -i add the dir
        dirs = []
        instances = {}
        baseurl = dav.baseurl.rstrip('/')
        for substr in args.i:
            if args.exclude is None or not ufload.cloud._match_instance_name(args.exclude, substr):
                dirs.append(ufload.cloud.instance_to_dir(substr))
        #Remove duplicates
        dirs = list(set(dirs))
        #Get the list for every required OC
        for dir in dirs:
            dav.change_oc(baseurl, dir)
            instances.update(ufload.cloud.list_files(user=info.get('login'),
                                                     pw=info.get('password'),
                                                     where=dir + args.cloud_path,
                                                     instances=args.i,
                                                     dav=dav,
                                                     url=info.get('url'),
                                                     site=dir,
                                                     path=info.get('path')))
    else:
        instances = ufload.cloud.list_files(user=info.get('login'),
                                            pw=info.get('password'),
                                            where=info.get('dir'),
                                            instances=args.i,
                                            dav=dav,
                                            url=info.get('url'),
                                            site=info.get('site'),
                                            path=info.get('path'))
    ufload.progress("Instances to be restored: %s" % ", ".join(instances.keys()))
    dbs=[]
    pattern = re.compile('.*-[A-Z]{1}[a-z]{2}\.zip$')

    for i in instances:
        if args.exclude is not None and ufload.cloud._match_instance_name(args.exclude, i):
            ufload._progress("%s matches -exclude param %s and will not be processed" % (i,args.exclude))
            continue

        files_for_instance = instances[i]
        for j in files_for_instance:

            #If filename doesn't match UniField auto-upload filename pattern, go to next file
            if not pattern.match(j[1]):
                continue

            ufload.progress("Trying file %s" % j[1])
            #If -oc is not known, change the connection settings according to the current instance
            if not args.oc:
                if i.endswith('_OCA'):
                    dav.change_oc(baseurl, 'OCA')
                elif i.startswith('OCB'):
                    dav.change_oc(baseurl, 'OCB')
                elif i.startswith('OCG_'):
                    dav.change_oc(baseurl, 'OCG')
                elif i.startswith('OCP_'):
                    dav.change_oc(baseurl, 'OCP')

            try:
                filename = dav.download(j[0],j[1])
            except Exception, e:
                ufload.progress("Error upload %s" % e)
                continue

            filesize = os.path.getsize(filename) / (1024 * 1024)
            ufload.progress("File size: %s Mb" % filesize)

            n= ufload.cloud.peek_inside_local_file(j[0], filename)
            '''n = ufload.cloud.peek_inside_file(j[0], j[1],
                                           user=args.user,
                                           pw=args.pw,
                                           dav=dav,
                                           where=_ocToDir(args.oc))
            '''
            if n is None:
                os.unlink(j[1])
                # no dump inside of zip, try the next one
                continue

            db = _file_to_db(args, str(n))
            if ufload.db.exists(args, db):
                ufload.progress("Database %s already exists." % db)
                os.unlink(j[1])
                break
            else:
                ufload.progress("Database %s does not exist, restoring." % db)

            '''f, sz = ufload.cloud.openDumpInZip(j[0], j[1],
                                           user=args.user,
                                           pw=args.pw,
                                           where=_ocToDir(args.oc))
            '''
            fname, sz = ufload.cloud.openDumpInZip(j[1])
            if fname is None:
                os.unlink(j[1])
                continue

            db = _file_to_db(args, fname)
            if db is None:
                ufload.progress("Bad filename %s. Skipping." % fname)
                try:
                    os.unlink(j[1])
                except:
                    pass
                continue

            rc = ufload.db.load_zip_into(args, db, j[1], sz)
            if rc == 0:
                dbs.append(db)

                if not args.noclean:
                    rc = ufload.db.clean(args, db)

                if args.notify:
                    subprocess.call([ args.notify, db ])

                try:
                    os.unlink(j[1])
                except:
                    pass

                # We got a good load, so go to the next instance.
                break
            try:
                os.unlink(j[1])
            except Exception as ex:
                pass

    if args.ss is not None and args.sync is None and args.synclight is None:
        _syncLink(args, dbs, args.ss)

    try:
        #Change directory
        os.chdir('..')
        #Remove temporary directory (and whatever is in it)
        shutil.rmtree('ufload_temp', True)
    except:
        pass

    return 0, dbs

def _syncRestore(args, dbs, ss):
    if args.db_prefix:
        sdb = '%s_%s' % (args.db_prefix, ss)
    else:
        sdb = ss

    #Which Sync Server do we need?
    if args.synclight:
        #url = "http://sync-prod_dump.uf5.unifield.org/SYNC_SERVER_LIGHT_WITH_MASTER"
        url = "http://sync-prod_dump.rb.unifield.org/SYNC_SERVER_LIGHT_NO_UPDATE"
    else:
        url = "http://sync-prod_dump.rb.unifield.org/SYNC_SERVER_LIGHT_WITH_MASTER"

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

    rc = ufload.db.load_dump_into(args, sdb, r.raw, sz)
    if rc != 0:
        return rc
    ufload.db.write_sync_server_len(args, sz, sdb)

    if not args.noclean:
        rc = ufload.db.clean(args, sdb)

    return _syncLink(args, dbs, sdb)

# separate function to make testing easier
def _syncLink(args, dbs, sdb):
    ufload.progress("Updating hardware id...")
    # Arrange that all instances use admin as the sync user
    #ufload.db.sync_server_all_admin(args, sdb)
    ufload.db.sync_server_all_sandbox_sync_user(args, sdb)

    # manage gap in sync update sequence
    ufload.db.psql(args, "update ir_sequence set number_next=number_next+1000 where code='sync.server.update';", sdb)

    # Hook up all the databases we are currently working on
    hwid = ufload.db.get_hwid(args)
    if hwid is None:
        ufload.progress("No hardware id available, you will need to manually link your instances to %s." % sdb)
        return 0

    if args.ss and (args.sync is None and args.synclight is None):
        #We don't update hardware id for all local instances: instances from another server could be already connected
        all = False
    else:
        # We update hardware id for all local instances: it's a new sync server, so no instance is connected yet
        all = True
        ufload.db.psql(args, 'update sync_server_entity set hardware_id = \'%s\';' % hwid, sdb)

    for db in dbs:
        ufload.progress("Updating hardware id and entity name for %s in sync server" % db)
        rc = ufload.db.sync_link(args, hwid, db, sdb, all)   #Update hardware_id and entity name (of the instance) in sync server db
        if rc != 0:
            return rc
    return 0


def _cmdLs(args):
    if not _required(args, [ 'user', 'pw', 'oc' ]):
        return 2
    if args.subdir is None:
        args.subdir = ''

    # Cloud access
    info = ufload.cloud.get_cloud_info(args, args.subdir)
    dav = ufload.cloud.get_onedrive_connection(args)
    instances = ufload.cloud.list_files(user=info.get('login'),
                                        pw=info.get('password'),
                                        where=info.get('dir'),
                                        instances=args.i,
                                        dav=dav,
                                        url=info.get('url'),
                                        site=info.get('site'),
                                        path=info.get('path'))

    if len(instances) == 0:
        ufload.progress("No files found.")
        return 1

    for i in instances:
        for j in instances[i]:
            print j[1]
            # only show the latest for each one
            break

    return 0

def _cmdClean(args):
    nb = ufload.db.cleanDbs(args)
    if nb==1:
        ufload._progress('One database has been deleted')
    elif nb>1:
        ufload._progress('%s databases have been deleted' % nb)
    else:
        ufload._progress('No database to delete found')

    return 0

def _cmdUpgrade(args):
    summarize = {
        'initial_version' : '',
        'last_version' : '',
        'user_rights_updated' : ''
    }
    
    #Install the patch on the sync server
    ss = 'SYNC_SERVER_LOCAL'
    if args.ss:
        ss = args.ss
        
    if args.patchcloud is not None: 
        if not _required(args, [ 'adminuser', 'adminpw' ]):
            return 2           
        #Connect to OD (cloud access)
        info = ufload.cloud.get_cloud_info(args, args.patchcloud)
        ufload.progress('site=%s - path=%s - dir=%s' % (info.get('site'), info.get('path'), info.get('dir')))
        dav = ufload.cloud.get_onedrive_connection(args)
        #Check for a zip file in the folder
        patches = ufload.cloud.list_patches(user=info.get('login'),
                                            pw=info.get('password'),
                                            where=info.get('dir'),
                                            dav=dav,
                                            url=info.get('url'),
                                            site=info.get('site'),
                                            path=info.get('path'))
        if len(patches) == 0:
            ufload.progress("No upgrade patch found.")
            return 1

        #Download the patch
        patches.sort(key=lambda s: map(int, re.split('\.|-|p',re.search('uf(.+?)\.patch\.zip',  s[1], re.I).group(1))))
        i = 0
        for j in patches:
            filename = dav.download(j[2], j[1])

            #Set patch and version args
            args.patch = filename
            m = re.search('(.+?)\.patch\.zip', filename)
            if m:
                args.version = m.group(1)
               
            if ufload.db.installPatch(args, ss) == 0:
                i += 1
            else:
                summarize['initial_version'] = args.version
            summarize['last_version'] = args.version 
            os.remove(filename)
        if i == 0:
            ufload.progress("No new patches found")
            if args.userrightscloud is None or not args.forcesync:
                return 0
    else:
        
        if not _required(args, [ 'patch', 'version', 'adminuser', 'adminpw' ]):
            return 2
    
        if ufload.db.installPatch(args, ss) == -1:
            ufload.progress("No new patches found")
            if args.userrightscloud is None or not args.forcesync:
                return 0

    #List instances
    inst = []
    if args.i is not None:
        instances = [x for x in args.i]
    else:
        instances = ufload.db._allDbs(args)

    #Update hardware_id and entity names in the Sync Server
    _syncLink(args, instances, ss)
        

    update_src = True
    update_available = False
    
    #Upgrade Unifield
    for instance in instances:
        if instance and instance != ss:
            ufload._progress("Connecting instance %s to sync server %s" % (instance, ss))
            try:
                ufload.db.connect_instance_to_sync_server(args, ss, instance)
            except oerplib.error.RPCError as err:
                if err[0].endswith("OpenERP version doesn't match database version!"):
                    ufload.progress("new versions is present")
                    update_available = True
                else:
                    raise oerplib.error.RPCError(err)
            
            
            i = 0
            while update_src:
                try:
                    ufload.db.manual_sync(args, ss, instance)
                except oerplib.error.RPCError as err:
                    regex = r""".*Cannot check for updates: There is/are [0-9]+ revision\(s\) available."""
                    flags = re.S
                    if re.compile(regex, flags).match(err[0]):
                        update_available = True
                        break
                    elif err[0].endswith('Authentification Failed, please contact the support'):
                        if i >= 10:
                            raise oerplib.error.RPCError(err)
                        time.sleep(1)
                        i += 1
                    else:
                        raise oerplib.error.RPCError(err)
                update_src = False
                break
            if not update_src:
                ufload.progress("No valid Update valid.")
                break
                    
            if update_available:
                ufload.progress("Upgrading Unifield App")
                ufload.db.manual_upgrade(args, ss, instance)
                ufload.progress("Awaiting the restart of Unifield")
                starting_up = True
                i = 0
                sleep_time = 1
                max_time = 300
                max_incrementation = (max_time/sleep_time)
                sys.stdout.flush()
                while starting_up and i < max_incrementation:
                    sys.stdout.write(next(spinner))
                    sys.stdout.flush()
                    time.sleep(sleep_time)
                    starting_up = True
                    i += 1
                    try:
                        r = requests.get("http://127.0.0.1:8061/openerp/login?db=&user=")
                        r.raise_for_status()
                    except requests.exceptions.ConnectionError:
                        starting_up = False
                    except requests.exceptions.HTTPError as http_err:
                        pass
                    sys.stdout.write('\b')
                sys.stdout.write('\r')    
                if i >= max_incrementation and not starting_up:
                    raise ValueError('The UniField serveur can not be restarted!!')
                    
                break
    
    #Update instances            
    if args.migratedb and update_src:            
        for instance in instances:
            update_modules = True
            i = 0
            sleep_time = 5
            max_time = 1800
            max_incrementation = (max_time/sleep_time)
            ufload.progress("Updating modules for instance {}".format(instance))
            while update_modules and i < max_incrementation:
                update_modules = False
                try:
                    netrpc = ufload.db.connect_rpc(args, ss, instance)
                except oerplib.error.RPCError as err:
                    ufload._progress("error.RPCError: {0}".format(err[0]))
                    # regex = r""".*Cannot check for updates: There is/are [0-9]+ revision\(s\) available."""
                    # flags = re.S
                    # if re.compile(regex, flags).match(err[0]) or err[0].endswith('Server is updating modules ...'):
                        # update_modules = True
                    # elif err[0].endswith('ServerUpdate: Server is updating modules ...'):
                    if err[0].endswith('ServerUpdate: Server is updating modules ...'):
                        update_modules = True
                    else:
                        raise oerplib.error.RPCError(err)
                except socket.error as err:
                    update_modules = True
                for j in range(sleep_time):
                    sys.stdout.write(next(spinner))
                    sys.stdout.flush()
                    time.sleep(1) 
                    sys.stdout.write('\b')
                i +=1
            sys.stdout.write('\r') 
            if i >= max_incrementation and not update_modules:
                raise ValueError("tolong wait for updating module instance %s".format(instance))   

    if args.userrightscloud is not None:
                   
        #Connect to OD (cloud access)
        info = ufload.cloud.get_cloud_info(args, args.userrightscloud)
        ufload.progress('site=%s - path=%s - dir=%s' % (info.get('site'), info.get('path'), info.get('dir')))
        dav = ufload.cloud.get_onedrive_connection(args)
        #Check for a zip file in the folder
        patches = ufload.cloud.list_patches(user=info.get('login'),
                                            pw=info.get('password'),
                                            where=info.get('dir'),
                                            dav=dav,
                                            url=info.get('url'),
                                            site=info.get('site'),
                                            path=info.get('path'))
        if len(patches) == 0:
            ufload.progress("No User Rights found.")
            return 1
        patches.sort(key=lambda s: map(int, re.split('\.|-|p',re.search('User Rights v(.+?).zip',  s[1], re.I).group(1))))
 
        urfilename = None
        for j in patches:
            urfilename = dav.download(j[2], j[1])
        if urfilename is not None:
            #Set patch and version args
            args.user_rights_zip= urfilename
            summarize['user_rights_updated'] = re.search('User Rights v(.+?).zip',  urfilename, re.I).group(1)
            try:
                ufload.db.installUserRights(args, ss)
            except oerplib.error.RPCError as err:
                if err[0].endswith('exists on server'):
                    ufload.progress(err[0].split("\n")[-1])
                    summarize['user_rights_updated'] = ''
                else:
                    raise oerplib.error.RPCError(err)
            os.remove(urfilename)
 
            
    if args.forcesync and ( not args.userrightscloud or ( args.userrightscloud and summarize['user_rights_updated'] != '' )):
        if instance and instance != ss:
            for instance in instances:
                ufload._progress("Connecting instance %s to sync server %s" % (instance, ss))
                ufload.db.connect_instance_to_sync_server(args, ss, instance)
                ufload._progress("synchonisation instance %s with sync server %s" % (instance, ss))
                ufload.db.manual_sync(args, ss, instance)

    if (args.autosync or  args.silentupgrade) and update_src:
        for instance in instances:
            if instance:
                ufload._progress("Connecting instance %s to sync server %s" % (instance, ss))
                ufload.db.connect_instance_to_sync_server(args, ss, instance)
                #ufload._progress("Update instance %s" % instance)
                #ufload.db.updateInstance(instance)
                if args.autosync:
                    #activate auto-sync (now + 1 hour)
                    ufload.db.activate_autosync(args, instance, ss)
                if args.silentupgrade:
                    #activate silent upgrade
                    ufload.db.activate_silentupgrade(args, instance)
                    
    ufload.progress(" *** summarize ***" )
    ufload.progress(" * Initial version installed: {}".format(summarize['initial_version']) ) 
    ufload.progress(" * Last version installed: {}".format(summarize['last_version']) )
    if args.userrightscloud is not None:
        ufload.progress(" * User Rights updated : {}".format(summarize['user_rights_updated'] if summarize['user_rights_updated'] else 'None' ) ) 

    return 0

def spinning_cursor():
    while True:
        for cursor in '|/-\\':
            yield cursor

spinner = spinning_cursor()


def parse():
    parser = argparse.ArgumentParser(prog='ufload')

    parser.add_argument("-user", help="Cloud username")
    parser.add_argument("-pw", help="Cloud password")
    parser.add_argument("-oc", help="OC (OCG, OCA and OCB accepted) - optional for the restore command (if not provided, ufload will try and deduce the right OC(s) from the name of the requested instances)")

    parser.add_argument("-syncuser", help="username to access the sync server backup")
    parser.add_argument("-syncpw", help="password to access the sync server backup")
    parser.add_argument("-sync-xmlrpcport", help="xmlrpc port used to connect the instance to the sync server")

    parser.add_argument("-db-host", help="Postgres host")
    parser.add_argument("-db-port", help="Postgres port")
    parser.add_argument("-db-user", help="Postgres user")
    parser.add_argument("-db-tablespace", help="Create db in psql tablespace")
    parser.add_argument("-db-pw", help="Postgres password")
    parser.add_argument("-db-prefix", help="Prefix to put on database names")
    parser.add_argument("-killconn", help="The command to run kill connections to the databases.")
    parser.add_argument("-remote", help="Remote log server")
    parser.add_argument("-local-log", dest='local', help="Path to create a local log file")
    parser.add_argument("-n", dest='show', action='store_true', help="no real work; only show what would happen")

    sub = parser.add_subparsers(title='subcommands',
                                description='valid subcommands',
                                help='additional help')

    pLs = sub.add_parser('ls', help="List the most recent backup")
    pLs.add_argument("-i", action="append", help="instances to work on (matched as a substring, default = all)")
    pLs.add_argument("-s", dest='subdir', help="Sub-directory")
    pLs.set_defaults(func=_cmdLs)

    pRestore = sub.add_parser('restore', help="Restore a database from cloud, a directory or a file")
    pRestore.add_argument("-i", action="append", help="instances to work on (matched as a substring)")
    pRestore.add_argument("-file", help="the file to restore (disabled cloud downloading)")
    pRestore.add_argument("-dir", help="the directory holding the files to restore (disabled cloud downloading)")
    pRestore.add_argument("-adminuser", default='admin', help="the new admin username in the newly restored database")
    pRestore.add_argument("-adminpw", default='uf1234', help="the password to set into the newly restored database")
    pRestore.add_argument("-userspw", help="the password to set for all users except admin into the newly restored database.")
    pRestore.add_argument("-inactiveusers", action='store_true', help="inactive users (except admin)")
    pRestore.add_argument("-createusers", dest='createusers', help="list of new users to create: user1:group1,group2;user2:group3,group4")
    pRestore.add_argument("-newuserspw", dest='newuserspw', help="new users password")
    pRestore.add_argument("-nopwreset", dest='nopwreset', action='store_true', help="do not change any passwords")
    pRestore.add_argument("-live", dest='live', action='store_true', help="do not take the normal actions to make a restore into a non-production instance")
    pRestore.add_argument("-no-clean", dest='noclean', action='store_true', help="do not clean up older databases for the loaded instances")
    pRestore.add_argument("-no-suffix", dest='nosuffix', action="store_true", help="remove the date and time numbers at the end of DB name")
    pRestore.add_argument("-load-sync-server", dest='sync', action='store_true', help="set up a local sync server and connects the restored instance(s) to it")
    pRestore.add_argument("-load-sync-server-no-update", dest='synclight', action='store_true', help="set up a light local sync server and connects the restored instance(s) to it")
    pRestore.add_argument("-notify", dest='notify', help="run this script on each restored database")
    pRestore.add_argument("-auto-sync", dest="autosync", action="store_true", help="Activate automatic synchronization on restored instances")
    pRestore.add_argument("-silent-upgrade", dest="silentupgrade", action="store_true", help="Activate silent upgrade on restored instances")
    pRestore.add_argument("-ss", help="Instance name of the sync server (default = SYNC_SERVER_LOCAL)")
    pRestore.add_argument("-rebuild-indexes", dest="analyze", action="store_true", help="Rebuild indexes after restore to enhance db performances")
    pRestore.add_argument("-exclude", help="instance to exclude (matched as a substring) - only without -i")
    pRestore.add_argument("-workingdir", dest='workingdir', help="the working directory used for downloading and unzipping the files (optional)")
    pRestore.add_argument("-connectionuser", default='sandbox_sync-user', help="User to connect instance to the sync server")
    pRestore.add_argument("-connectionpw", default='Only4Sandbox', help="Password to connect instance to the sync server")
    pRestore.set_defaults(func=_cmdRestore)
    
    pArchive = sub.add_parser('archive', help="Copy new data into the database.")
    pArchive.add_argument("-from-dsn", action="append", help="the database to copy from (in the form of a DSN: 'hostaddr=x dbname=x user=x password=x')")
    pArchive.set_defaults(func=_cmdArchive)

    pUpgrade = sub.add_parser('upgrade', help="Upgrade sync server and instances to a new version")
    pUpgrade.add_argument("-patch", help="Path to the upgrade zip file")
    pUpgrade.add_argument("-version", help="Targeted version number")
    pUpgrade.add_argument("-ss", help="Instance name of the sync server (default = SYNC_SERVER_LOCAL)")
    pUpgrade.add_argument("-load-sync-server", dest='sync', action='store_true',
                          help="set up a local sync server and connects the restored instance(s) to it")
    pUpgrade.add_argument("-load-sync-server-no-update", dest='synclight', action='store_true',
                          help="set up a light local sync server and connects the restored instance(s) to it")
    pUpgrade.add_argument("-adminuser", default='admin', help="the admin username to log into the instances")
    pUpgrade.add_argument("-adminpw", default='admin', help="the admin password to log into the instances")
    pUpgrade.add_argument("-i", action="append", help="Instances to upgrade programmatically (matched as a substring, default = all). Other instances will be upgraded at login")
    pUpgrade.add_argument("-auto-sync", dest="autosync", action="store_true", help="Activate automatic synchronization")
    pUpgrade.add_argument("-silent-upgrade", dest="silentupgrade", action="store_true", help="Activate silent upgrade")
    pUpgrade.add_argument("-patch-cloud-path", dest='patchcloud', help="Path to the folder containing the upgrade zip file on OneDrive")
    pUpgrade.add_argument("-cloud-user-rights-path", dest='userrightscloud', help="User Rights to the folder containing the upgrade zip file on OneDrive")
    pUpgrade.add_argument("-migrate-db", dest='migratedb', action="store_true", help="Path to the folder containing the upgrade zip file on OneDrive")
    pUpgrade.add_argument("-force-sync", dest='forcesync', action="store_true", help="Force synchronization with the sync server of all instances")
    pUpgrade.set_defaults(func=_cmdUpgrade)

    pClean = sub.add_parser('clean', help="Clean DBs with a wrong name format")
    #pClean.add_argument("-i", action="append", help="instances to work on (matched as a substring, default = all)")
    pClean.set_defaults(func=_cmdClean)

    # read from $HOME/.ufload first
    conffile = ConfigParser.SafeConfigParser()
    if sys.platform == "win32":
        conffile.read('%s/ufload.txt' % _home())
    else:
        conffile.read('%s/.ufload' % _home())

    for subp, subn in ((parser, "onedrive"),    #(parser, "owncloud"),
                       (parser, "postgres"),
                       (parser, "logs"),
                       (parser, "sync"),
                       (pLs, "ls"),
                       (pRestore, "restore"),
                       (pArchive, "archive"),
                       (pUpgrade, "upgrade")):
        if conffile.has_section(subn):
            subp.set_defaults(**dict(conffile.items(subn)))

    # now that the config file is applied, parse from cmdline
    return parser.parse_args()

def main():
    global args
    args = parse()
    if hasattr(args, "func"):
        try:
            rc = args.func(args)
        except KeyboardInterrupt:
            rc = 1

    ufload.progress("ufload is done working :-)")

    if args.remote:
        import socket
        hostname = socket.gethostname() or 'unknown'
        ufload.progress("Will exit with result code: %d" % rc)
        ufload.progress("Posting logs to remote server.")
        requests.post(args.remote+"?who=%s"%hostname, data='\n'.join(_logs))

    sys.exit(rc)

main()
