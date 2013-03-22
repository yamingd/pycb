import pylcb


LCB_SUCCESS = 0x00

LCB_ADD = 0x01
LCB_REPLACE = 0x02
LCB_SET = 0x03
LCB_APPEND = 0x04
LCB_PREPEND = 0x05


def error_callback(error, errinfo):
    print "error in pylcb: %x, %s" % (error, errinfo)


def get_callback(cookie, error, key, document):
    cookie.getError = error
    cookie.getDocument = document


def store_callback(cookie, error, key):
    cookie.storeError = error


def remove_callback(cookie, error, key):
    cookie.deleteError = error


def arithmetic_callback(cookie, error, key, value):
    cookie.arithmeticError = error
    cookie.arithmeticValue = value


def stat_callback(cookie, error, server, name, stat):
    if cookie.statsError == LCB_SUCCESS or cookie.statsError is None:
        cookie.statsError = error
        cookie.statsStats.append(dict(server=server, name=name, stat=stat))


def flush_callback(cookie, error, server):
    if cookie.flushError == LCB_SUCCESS or cookie.flushError is None:
        cookie.flushError = error
        cookie.flushServers.append(dict(server=server))


def http_complete_callback(cookie, error, status, path, headers, bytes):
    cookie.httpError = error


class Couchbase(object):
    def __init__(self, host, username, password):
        self.host = host
        self.username = username
        self.password = password

        pylcb.set_error_callback(error_callback)
        pylcb.set_get_callback(get_callback)
        pylcb.set_store_callback(store_callback)
        pylcb.set_remove_callback(remove_callback)
        pylcb.set_arithmetic_callback(arithmetic_callback)
        pylcb.set_stat_callback(stat_callback)
        pylcb.set_flush_callback(flush_callback)
        pylcb.set_http_complete_callback(http_complete_callback)

    def bucket(self, bucketName):
        return Bucket(self.host, self.username, self.password, bucketName)

    def buckets(self):
        pass

    def create(self, bucketName, bucketPassword='', ramQuota=100, replica=0):
        pass

    def delete(self, bucketName):
        pass


class Bucket(object):
    def __init__(self, host, username, password, bucketName):
        self.instance = pylcb.connect(host, username, password, bucketName)
        pylcb.wait(self.instance)

    def add(self, key, expiration, flags, value):
        return self._store(key, expiration, flags, value, LCB_ADD)

    def replace(self, key, expiration, flags, value):
        return self._store(key, expiration, flags, value, LCB_REPLACE)

    def set(self, key, expiration, flags, value):
        return self._store(key, expiration, flags, value, LCB_SET)

    def append(self, key, expiration, flags, value):
        return self._store(key, expiration, flags, value, LCB_APPEND)

    def prepend(self, key, expiration, flags, value):
        return self._store(key, expiration, flags, value, LCB_PREPEND)

    def _store(self, key, expiration, flags, value, operation):
        self.storeError = None
        pylcb.store(self.instance, self, key,
                    expiration, flags, value, operation)
        pylcb.wait(self.instance)

        if self.storeError == 0:
            return True
        else:
            return False

    def get(self, key):
        self.getError = None
        self.getDocument = None
        pylcb.get(self.instance, self, key)
        pylcb.wait(self.instance)

        if self.getError == 0:
            return self.getError, None, self.getDocument
        else:
            return self.getError, None, None

    def delete(self, key, cas=0):
        if key.startswith('_design/'):
            # this is a design doc, we need to handle it differently
            pass
        else:
            self.deleteError = None
            pylcb.remove(self.instance, self, key)
            pylcb.wait(self.instance)

            if self.deleteError == 0:
                return True
            else:
                return False

    def incr(self, key, delta=1, initial=0, expiration=0):
        return self._arithmetic(key, delta, initial, expiration)

    def decr(self, key, delta=1, initial=0, expiration=0):
        return self._arithmetic(key, -delta, initial, expiration)

    def _arithmetic(self, key, delta, initial, expiration):
        self.arithmeticError = None
        self.arithmeticValue = None
        pylcb.arithmetic(self.instance, self, key, delta, initial, expiration)
        pylcb.wait(self.instance)

        if self.arithmeticError == 0:
            return self.arithmeticValue
        else:
            return None

    def stats(self, name=""):
        self.statsError = None
        self.statsStats = []

        pylcb.stats(self.instance, self, name)
        pylcb.wait(self.instance)

        if self.statsError == 0:
            return self.statsStats
        else:
            return None

    def flush(self):
        self.flushError = None
        self.flushServers = []

        pylcb.flush(self.instance, self)
        pylcb.wait(self.instance)

        if self.flushError == 0:
            return self.flushServers
        else:
            return None
