# Routines related to ownCloud

import easywebdav
import time
import zipfile
import collections
import logging, tempfile
import base64
import sys
import webdav
from urlparse import urlparse


import ufload

def _splitCloudName(x):
    spl = x.split(":", 1)
    # no :, so use the default cloud hostname
    if len(spl) == 1:
        return ('cloud.msf.org', x)
    return (spl[0], spl[1])

#Simple enough: we just delete the first four characters and then base64-decode the remaining string
def _decrypt(pwd):
    pwd = pwd.strip()
    x = pwd[4:]
    try:
        x = base64.b64decode(x)
        return x
    except:
        ufload.progress('Unable to decode password')
        print sys.exc_info()[0]


def instance_to_dir(instance):
    #instance name ends with "_OCA"
    if instance.endswith('_OCA'):
        return '/personal/UF_OCA_msf_geneva_msf_org/'
    #instance name starts with "OCB"
    if instance.startswith('OCB'):
        return '/personal/UF_OCB_msf_geneva_msf_org/'
    # instance name starts with "OCB"
    if instance.startswith('OCP'):
        return '/personal/UF_OCP_msf_geneva_msf_org/'
    #instance name starts with "OCG_"
    if instance.startswith('OCG_'):
        return '/personal/UF_OCG_msf_geneva_msf_org/'

    return ''

def get_cloud_info(args, sub_dir=''):

    #Cloud password is encrypted
    pword = _decrypt(args.pw)

    #Cloud path depends on the OC
    if args.oc:
        dir = '/personal/UF_' + args.oc.upper() + '_msf_geneva_msf_org/'
    else:
        dir = ''    #No OC specified, let's use only the path

    sub = args.cloud_path

    try:
        #if the argument patchcloud is set, we're downloading the upgrade patch, go to the right directory (MUST be under the main dir)
        if (sub_dir is not None):
            sub = sub + sub_dir
    except:
        #The argument cloudpath is not defined, forget about it (this is not the upgrade process)
        pass

    ret = {
        'url': args.cloud_url,
        'dir': dir + sub,
        'site': dir,
        'path': args.cloud_path,
        'login': args.user,
        'password': pword
    }

    return ret


def get_onedrive_connection(args):
    info = get_cloud_info(args)
    if not info.get('url'):
        ufload.progress('URL is not set!')
    if not info.get('login'):
        ufload.progress('login is not set!')
    if not info.get('password'):
        ufload.progress('Password is not set!')

    url = urlparse(info['url'])
    if not url.netloc:
        ufload.progress('Unable to parse url: %s') % (info['url'])

    path = info.get('site') + url.path

    try:
        dav = webdav.Client(url.netloc, port=url.port, protocol=url.scheme, username=info['login'],
                            password=info['password'], path=path)
        return dav
    except webdav.ConnectionFailed, e:
        ufload.progress('Unable to connect: {}'.format(e))
        ufload.progress('Cannot proceed without connection, exiting program.')
        exit(1)




def _get_all_files_and_timestamp(dav, d):
    ufload.progress('Browsing files from dir %s' % d)
    try:
        #all_zip = dav.ls(d)
        all_zip = dav.list(d)
    except Exception as e:
        ufload.progress("Cloud Exception 88")
        logging.warn(str(e))
        return []

    ret = []
    for f in all_zip:
        #if not f['Name'] or f['Name'][-1] == '/':
        if not f['Name']:
            continue

        # We try to extract a timestamp to get an idea of the creation date
        #  Format: Mon, 14 Mar 2016 03:31:40 GMT
        t = time.strptime(f['TimeLastModified'], '%Y-%m-%dT%H:%M:%SZ')

        # We don't take into consideration backups that are too recent.
        # Otherwise they could be half uploaded (=> corrupted)
        if abs(time.time() - time.mktime(t)) < 900:
            continue

        # ufload.progress('File found: %s' % f['Name'])

        if f['Name'].split(".")[-1] != "zip":
            logging.warn("Ignoring non-zipfile: %s" % f['Name'])
            continue
        ret.append((t, f['Name'], f['ServerRelativeUrl']))
    return ret

# returns True if x has instance as a substring
def _match_instance_name(instance, x):
    for pat in instance.split(','):
        if pat in x:
            return True
    return False

# returns True is any of the instances match x
# (returns True for all if instances is empty)
def _match_any_wildcard(instances, x):
    if not instances:
        return True

    for i in instances:
        if _match_instance_name(i, x):
            return True
    return False

def _group_files_to_download(files):
    files.sort()
    files.reverse()
    ret = collections.defaultdict(lambda : [])

    for a in files:
        t, f, u = a
        #if '/' not in f:
        #   raise Exception("no slash in %s" % f)

        #isplit = f.rindex('/')
        #filename = f[isplit+1:]
        if '-' not in f:
            ufload.progress("filename is missing expected dash: "+ f)
            continue

        instance = '-'.join(f.split('-')[:-1])
        ret[instance].append((u, f))

    return ret

# list_files returns a dictionary of instances
# and for each instance, a list of (path,file) tuples
# in order from new to old.
def list_files(**kwargs):
    directory = kwargs['where']

    #all = _get_all_files_and_timestamp(dav, "/remote.php/webdav/"+directory)
    all = _get_all_files_and_timestamp(kwargs['dav'], directory)

    all = _group_files_to_download(all)

    inst = []
    if kwargs['instances'] is not None:
        inst = [x.lower() for x in kwargs['instances']]

    ret = {}
    for i in all:
        if _match_any_wildcard(inst, i.lower()):
            ret[i] = all[i]
    return ret

# list_files returns a dictionary of instances
# and for each instance, a list of (path,file) tuples
# in order from new to old.
def list_patches(**kwargs):
    directory = kwargs['where']

    all = _get_all_files_and_timestamp(kwargs['dav'], directory)

    return all



def peek_inside_local_file(path, fn):
    try:
        z = zipfile.ZipFile(fn)
    except Exception as e:
        ufload.progress("Zipfile %s: could not read: %s" % (fn, e))
        return None

    names = z.namelist()
    if len(names) == 0:
        ufload.progress("Zipfile %s has no files in it." % fn)
        return None
    if len(names) != 1:
        ufload.progress("Zipfile %s has unexpected files in it: %s" % (fn, names))
        return None
    n = names[0]
    z.close()
    del z
    return n


def peek_inside_file(path, fn, **kwargs):
    '''host, directory = _splitCloudName(kwargs['where'])
    dav = easywebdav.connect(host,
                            username=kwargs['user'],
                            password=kwargs['pw'],
                            protocol='https')
    '''

    try:
        z = zipfile.ZipFile(ufload.httpfile.HttpFile(dav.baseurl+path,
                                                     dav.session.auth[0],
                                                     dav.session.auth[1]))
    except Exception as e:
        ufload.progress("Zipfile %s: could not read: %s" % (fn, e))
        return None
    
    names = z.namelist()
    if len(names) == 0:
        ufload.progress("Zipfile %s has no files in it." % fn)
        return None
    if len(names) != 1:
        ufload.progress("Zipfile %s has unexpected files in it: %s" % (fn, names))
        return None
    n = names[0]
    z.close()
    return n

def dlProgress(pct):
    ufload.progress("Downloaded %d%%" % pct)

# Returns a file-like-object
#def openDumpInZip(path, fn, **kwargs):
def openDumpInZip(fn):
    #file = open(fn, 'r')
    z = zipfile.ZipFile(fn)
    names = z.namelist()
    if len(names) == 0:
        logging.warn("Zipfile %s has no files in it." % fn)
        return None, 0
    if len(names) != 1:
        logging.warn("Zipfile %s has unexpected files in it: %s" % (fn, names))
        return None, 0
    try:
        file = z.open(names[0])
    except:
        logging.warn("Zipfile %s is probably corrupted" % fn)
        return None, 0

    filename = file.name
    size = z.getinfo(names[0]).file_size
    file.close()
    z.close()
    del file
    del z

    #return z.open(names[0]), z.getinfo(names[0]).file_size
    return filename, size


# An object that copies input to output, calling
# the progress callback along the way.
class StatusFile(object):
    def __init__(self, fout, progress):
        self.fout = fout
        self.progress = progress
        self.tot = None
        self.next = 10
        self.n = 0
        
    def setSize(self, sz):
        self.tot = float(sz)
        
    def write(self, data):
        self.n += len(data)
        if self.tot is not None:
            pct = int(self.n/self.tot*100)
            if pct > self.next:
                self.next = (pct/10)*10+10
                self.progress(pct)
        self.fout.write(data)

