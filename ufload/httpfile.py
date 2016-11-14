# http://stackoverflow.com/questions/7829311/is-there-a-library-for-retrieving-a-file-from-a-remote-zip/7852229

import urllib2, base64

class HttpFile(object):
    def __init__(self, url, user, pw):
        self.url = url
        self.user = user
        self.pw = pw
        self.offset = 0
        self._size = -1

    def _req(self):
        req = urllib2.Request(self.url)
        up = base64.b64encode('%s:%s' % (self.user, self.pw))
        req.add_header("Authorization", "Basic %s" % up)
        return req

    def size(self):
        if self._size < 0:
            req = self._req()
            req.get_method = lambda : 'HEAD'
            f = urllib2.urlopen(req)
            if f.code != 200:
                raise RuntimeError("status code " + str(f.code))
            self._size = int(f.headers["Content-length"])
        return self._size

    def read(self, count=-1):
        if count < 0:
            end = self.size() - 1
        else:
            end = self.offset + count - 1
        req = self._req()
        req.headers['Range'] = "bytes=%s-%s" % (self.offset, end)
        f = urllib2.urlopen(req)
        data = f.read()
        if f.code != 200 and f.code != 206:
            raise RuntimeError("status code " + str(f.code))
        chunk = len(data)
        if count >= 0:
            assert chunk == count
        self.offset += chunk
        return data

    def seek(self, offset, whence=0):
        if whence == 0:
            self.offset = offset
        elif whence == 1:
            self.offset += offset
        elif whence == 2:
            self.offset = self.size() + offset
        else:
            raise Exception("Invalid whence")

    def tell(self):
        return self.offset
