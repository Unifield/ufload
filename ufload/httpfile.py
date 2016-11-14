# http://stackoverflow.com/questions/7829311/is-there-a-library-for-retrieving-a-file-from-a-remote-zip/7852229

import requests

class HttpFile(object):
    def __init__(self, url, user, pw):
        self.url = url
        self.user = user
        self.pw = pw
        self.offset = 0
        self._size = -1

    def size(self):
        if self._size < 0:
            r = requests.head(self.url, auth=(self.user, self.pw))
            if not r.ok:
                raise RuntimeError("status code " + str(r.status_code))
            self._size = int(r.headers["Content-length"])
        return self._size

    def read(self, count=-1):
        if count < 0:
            end = self.size() - 1
        else:
            end = self.offset + count - 1
        h = { 'Range': "bytes=%s-%s" % (self.offset, end) }
        r = requests.get(self.url, auth=(self.user, self.pw), headers=h)
        if not r.ok:
            raise RuntimeError("status code " + str(r.status_code))
        data = r.content
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
