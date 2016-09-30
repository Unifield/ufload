# http://stackoverflow.com/questions/7829311/is-there-a-library-for-retrieving-a-file-from-a-remote-zip/7852229

import urllib2, base64

class HttpFile(object):
    def __init__(self, url, user, pw):
        self.url = url
        self.user = user
        self.pw = pw
        self.offset = 0
        self._size = -1
        self._cache = []
        self._cacheBase = 0
        self._cacheSize = 10 * 1024 * 1024
        
    def _req(self):
        req = urllib2.Request(self.url)
        up = base64.b64encode('%s:%s' % (self.user, self.pw))
        req.add_header("Authorization", "Basic %s" % up)
        return req

    def _getcache(self, off, end):
        print "getcache", off, end
        cacheEnd = self._cacheBase + len(self._cache)
        # The cache does not cover the range they want
        if off < self._cacheBase or end > cacheEnd:
            print "not in cache", off, self._cacheBase, end, cacheEnd
            return None, False
        fr = off - self._cacheBase
        to = fr+(end-off)
        print "getcache slice", fr, to
        return self._cache[fr:to], True

    def _fillcache(self, off, end):
        print "fillcache", off, end
        req = self._req()
        req.headers['Range'] = "bytes=%s-%s" % (off, off+self._cacheSize)
        f = urllib2.urlopen(req)
        self._cache = f.read()
        print "new cache", len(self._cache)
        self._cacheBase = off
        return self._getcache(off, end)
    
    def size(self):
        if self._size < 0:
            req = self._req()
            req.get_method = lambda : 'HEAD'
            f = urllib2.urlopen(req)
            self._size = int(f.headers["Content-length"])
        print "size", self._size
        return self._size

    def read(self, count=-1):
        print "read", count
        if count < 0:
            end = self.size() - 1
        else:
            end = self.offset + count - 1

        data, ok = self._getcache(self.offset, end)
        if not ok:
            data, ok = self._fillcache(self.offset, end)
            if not ok:
                raise Exception("fillcache says not ok")
        
        chunk = len(data)
        if count >= 0:
            assert chunk == count
        self.offset += chunk
        print "read returns", len(data), data
        return data

    def seek(self, offset, whence=0):
        print "seek", offset, whence
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
