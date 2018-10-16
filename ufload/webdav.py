# -*- coding: utf-8 -*-

import cgi
import logging
import os
import uuid

from collections import namedtuple

import requests
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.runtime.client_request import ClientRequest
from office365.runtime.utilities.http_method import HttpMethod
from office365.runtime.utilities.request_options import RequestOptions
from office365.sharepoint.client_context import ClientContext



class ConnectionFailed(Exception):
    pass

class Client(object):
    def __init__(self, host, port=0, auth=None, username=None, password=None, protocol='http', path=None):
        if not port:
            port = 443 if protocol == 'https' else 80
        self.path = path or ''
        if not self.path.endswith('/'):
            self.path = '%s/' % self.path

        # oneDrive: need to split /site/ and path
        # in our config site is /personal/UF_OCX_msf_geneva_msf_org/
        # path is /Documents/Tests/
        self.baseurl = '{0}://{1}:{2}{3}/'.format(protocol, host, port, '/'.join(self.path.split('/')[0:3]) )
        ctx_auth = AuthenticationContext(self.baseurl)

        #if len(self.path.split('/')) < 5:
        #    self.path = '%sDocuments/' % self.path
        if ctx_auth.acquire_token_for_user(username, cgi.escape(password)):
            self.request = ClientRequest(ctx_auth)
            self.request.context = ClientContext(self.baseurl, ctx_auth)

            if not ctx_auth.provider.FedAuth or not ctx_auth.provider.rtFa:
                raise ConnectionFailed(ctx_auth.get_last_error())
        else:
            raise ConnectionFailed(ctx_auth.get_last_error())

    def change_oc(self, baseurl, dir):
        if dir == 'OCA':
            dir = '/personal/UF_OCA_msf_geneva_msf_org/'
        elif dir == 'OCB':
            dir = '/personal/UF_OCB_msf_geneva_msf_org/'
        elif dir == 'OCG':
            dir = '/personal/UF_OCG_msf_geneva_msf_org/'

        self.baseurl = baseurl + dir

    def delete(self, remote_path):
        webUri = '%s%s' % (self.path, remote_path)
        request_url = "%s/_api/web/getfilebyserverrelativeurl('%s')" % (self.baseurl, webUri)
        options = RequestOptions(request_url)
        options.method = HttpMethod.Delete
        options.set_header("X-HTTP-Method", "DELETE")
        self.request.context.authenticate_request(options)
        self.request.context.ensure_form_digest(options)
        result = requests.post(url=request_url, data="", headers=options.headers, auth=options.auth)
        if result.status_code not in (200, 201):
            raise Exception(result.content)
        return True


    def list(self, remote_path):
        #webUri = '%s%s' % (self.path, remote_path)
        #request_url = "%s_api/web/getfilebyserverrelativeurl('%s')/files" % (self.baseurl, webUri)
        request_url = "%s_api/web/getfolderbyserverrelativeurl('%s')/files" % (self.baseurl, remote_path)
        print request_url
        options = RequestOptions(request_url)
        options.method = HttpMethod.Get
        options.set_header("X-HTTP-Method", "GET")
        options.set_header('accept', 'application/json;odata=verbose')
        self.request.context.authenticate_request(options)
        self.request.context.ensure_form_digest(options)
        result = requests.get(url=request_url, headers=options.headers, auth=options.auth)
        #result = requests.post(url=request_url, data="", headers=options.headers, auth=options.auth)
        result = result.json()
        '''if result.status_code not in (200, 201):
            print 'Error code: '
            print result.status_code
            raise Exception(result.content)
            '''
        #return True

        files=[]

        for i in range(len(result['d']['results'])):
            item = result['d']['results'][i]
            files.append(item)

        print files[0]['Name']
        return files

    def download(self, remote_path, filename):
        request_url = "%s_api/web/getfilebyserverrelativeurl('%s')/$value" % (self.baseurl, remote_path)
        options = RequestOptions(request_url)
        options.method = HttpMethod.Get
        options.set_header("X-HTTP-Method", "GET")
        options.set_header('accept', 'application/json;odata=verbose')
        self.request.context.authenticate_request(options)
        self.request.context.ensure_form_digest(options)
        result = requests.get(url=request_url, headers=options.headers, auth=options.auth)

        with open(filename, 'wb') as file:
            file.write(result.content)

        return filename

    def upload(self, fileobj, remote_path, buffer_size=None, log=False, progress_obj=False):
        iid = uuid.uuid1()

        if progress_obj:
            log = True

        if log:
            logger = logging.getLogger('cloud.backup')
            try:
                size = os.path.getsize(fileobj.name)
            except:
                size = None

        offset = -1
        if not buffer_size:
            buffer_size = 10* 1024 * 1024
        x = ""
        webUri = '%s%s' % (self.path, remote_path)
        while True:
            if offset == -1:
                request_url = "%s/_api/web/GetFolderByServerRelativeUrl('%s')/Files/add(url='%s',overwrite=true)" % (self.baseurl, self.path, remote_path)
                offset = 0
            elif not offset:
                if len(x) == buffer_size:
                    request_url="%s/_api/web/getfilebyserverrelativeurl('%s')/startupload(uploadId=guid'%s')" % (self.baseurl, webUri, iid)
                else:
                    request_url = "%s/_api/web/GetFolderByServerRelativeUrl('%s')/Files/add(url='%s',overwrite=true)" % (self.baseurl, self.path, remote_path)
            elif len(x) == buffer_size:
                request_url = "%s/_api/web/getfilebyserverrelativeurl('%s')/continueupload(uploadId=guid'%s',fileOffset=%s)" % (self.baseurl, webUri, iid, offset)
            else:
                request_url = "%s/_api/web/getfilebyserverrelativeurl('%s')/finishupload(uploadId=guid'%s',fileOffset=%s)" % (self.baseurl, webUri, iid, offset)

            offset += len(x)
            options = RequestOptions(request_url)
            options.method = HttpMethod.Post

            self.request.context.authenticate_request(options)
            self.request.context.ensure_form_digest(options)
            result = requests.post(url=request_url, data=x, headers=options.headers, auth=options.auth)
            if result.status_code not in (200, 201):
                raise Exception(result.content)

            if log and offset and offset % buffer_size*10 == 0:
                percent_txt = ''
                if size:
                    percent = round(offset*100/size)
                    percent_txt = '%d%%' % percent
                    if progress_obj:
                        progress_obj.write({'name': percent})

                logger.info('OneDrive: %d bytes sent on %s bytes %s' % (offset, size or 'unknown', percent_txt))

            x = fileobj.read(buffer_size)
            if not x:
                break
        return True

