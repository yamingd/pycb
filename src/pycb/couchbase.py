import pylcb
import urllib
import json

# libcouchbase result codes
LCB_SUCCESS = 0x00

# libcouchbase create types
LCB_TYPE_BUCKET = 0x00      # use bucket name
LCB_TYPE_CLUSTER = 0x01     # ignore bucket name.  Used for admin

# libcouchbase "store" methods
LCB_ADD = 0x01
LCB_REPLACE = 0x02
LCB_SET = 0x03
LCB_APPEND = 0x04
LCB_PREPEND = 0x05

# libcouchbase make_http_request types
LCB_HTTP_TYPE_VIEW = 0
LCB_HTTP_TYPE_MANAGEMENT = 1
LCB_HTTP_TYPE_RAW = 2
LCB_HTTP_TYPE_MAX = 3

# libcouchbase http methods
LCB_HTTP_METHOD_GET = 0
LCB_HTTP_METHOD_POST = 1
LCB_HTTP_METHOD_PUT = 2
LCB_HTTP_METHOD_DELETE = 3
LCB_HTTP_METHOD_MAX = 4


class PycbException(Exception):
    def __init__(self, text, error):
        self.text = text
        self.error = error

    def __str__(self):
        errMsg = pylcb.strerror(self.error)
        return "%s, errCode:0x%x, errMsg:%s" % (self.text, self.error, errMsg)


class Couchbase(object):
    def __init__(self, host, username, password):
        self.host = host
        self.username = username
        self.password = password

    def bucket(self, bucketName):
        bucket = Bucket(self.host, self.username, self.password, bucketName)

    def buckets(self):
        pass

    def create(self, name, saslPassword='',
               ramQuotaMB=100, replicaNumber=0, **params):
        payload = dict(
            saslPassword=saslPassword,
            ramQuotaMB=ramQuotaMB,
            replicaNumber=replicaNumber,
            authType='sasl'
        )
        payload.update(params)

        cluster = Cluster(self.host, self.username, self.password, None)
        result = cluster.create_bucket(name, **payload)
        if result['error'] == LCB_SUCCESS:
            return self.bucket(name)

        raise PycbException("http error", result['error'])

    def delete(self, bucketName):
        pass


class Connection(object):
    def __init__(self, host, username, password, bucketName):
        if bucketName is None:
            connectionType = LCB_TYPE_CLUSTER
            bucketName = ""
        else:
            connectionType = LCB_TYPE_BUCKET
        self.instance = pylcb.create(host, username, password,
                                     bucketName, connectionType)

        pylcb.set_arithmetic_callback(self.instance, self.arithmetic_callback)
        pylcb.set_error_callback(self.instance, self.error_callback)
        pylcb.set_flush_callback(self.instance, self.flush_callback)
        pylcb.set_get_callback(self.instance, self.get_callback)
        pylcb.set_http_complete_callback(self.instance,
                                         self.http_complete_callback)
        pylcb.set_remove_callback(self.instance, self.remove_callback)
        pylcb.set_stat_callback(self.instance, self.stat_callback)
        pylcb.set_store_callback(self.instance, self.store_callback)

        self.errorResults = []
        pylcb.connect(self.instance)
        pylcb.wait(self.instance)
        print self.errorResults

    def arithmetic_callback(self, cookie, error, key, value):
        self.arithmeticResult = dict(error=error, key=key, value=value)

    def error_callback(self, error, errinfo):
        self.errorResults.append(dict(error=error, errinfo=errinfo))

    def flush_callback(self, cookie, error, server):
        if server != 0:
            self.flushResults.append(dict(error=error, server=server))

    def get_callback(self, cookie, error, key, document):
        self.getResult = dict(error=error, key=key, document=document)

    def http_complete_callback(self, cookie, error,
                               status, path, headers, bytes):
        self.httpResult = dict(error=error, status=status, path=path,
                               headers=headers, bytes=bytes)

    def remove_callback(self, cookie, error, key):
        self.removeResult = dict(error=error, key=key)

    def stat_callback(self, cookie, error, server, name, stat):
        if server != 0:
            self.statsResults.append(dict(error=error, server=server,
                                          name=name, stat=stat))

    def store_callback(self, cookie, error, key):
        self.storeResult = dict(error=error, key=key)


class Cluster(Connection):
    def create_bucket(self, name, **payload):
        payload.update(dict(name=name))
        body = urllib.urlencode(payload)

        self.httpResult = None
        pylcb.make_http_request(
            self.instance,
            None,
            LCB_HTTP_TYPE_MANAGEMENT,
            "pools/default/buckets",
            body,
            LCB_HTTP_METHOD_POST,
            0,
            "application/x-www-form-urlencoded"
        )
        pylcb.wait(self.instance)
        return self.httpResult


class Bucket(Connection):
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
        self.storeResult = None
        pylcb.store(self.instance, self, key,
                    expiration, flags, value, operation)
        pylcb.wait(self.instance)

        result = self.storeResult
        if result['error'] == LCB_SUCCESS:
            return True
        else:
            raise PycbException("store error", result['error'])

    def get(self, key):
        self.getResult = None
        pylcb.get(self.instance, self, key)
        pylcb.wait(self.instance)

        result = self.getResult
        if result['error'] == LCB_SUCCESS:
            return True
        else:
            raise PycbException("get error", result['error'])

    def delete(self, key, cas=0):
        if key.startswith('_design/'):
            # this is a design doc, we need to handle it differently
            pass
        else:
            self.removeResult = None
            pylcb.remove(self.instance, self, key)
            pylcb.wait(self.instance)

            result = self.removeResult
            if result['error'] == LCB_SUCCESS:
                return True
            else:
                raise PycbException("delete error", result['error'])

    def incr(self, key, delta=1, initial=0, expiration=0):
        return self._arithmetic(key, delta, initial, expiration)

    def decr(self, key, delta=1, initial=0, expiration=0):
        return self._arithmetic(key, -delta, initial, expiration)

    def _arithmetic(self, key, delta, initial, expiration):
        self.arithmeticResult = None
        pylcb.arithmetic(self.instance, self, key, delta, initial, expiration)
        pylcb.wait(self.instance)

        result = self.arithmeticResult
        if result['error'] == LCB_SUCCESS:
            return True
        else:
            raise PycbException("increment/decrement error", result['error'])

    def stats(self, name=""):
        self.statsResults = []
        pylcb.stats(self.instance, self, name)
        pylcb.wait(self.instance)

        return self.statsResults

    def flush(self):
        self.flushResults = []

        pylcb.flush(self.instance, self)
        pylcb.wait(self.instance)

        return self.flushResults

    def view(self, view, **params):
        for param in params:
            if param in ["key", "keys", "startkey", "endkey"]:
                value = json.dumps(params[param])
                params[param] = value

        path = "%s?%s" % (view, urllib.urlencode(params))

        self.httpResult = None
        pylcb.make_http_request(
            self.instance,
            None,
            LCB_HTTP_TYPE_VIEW,
            path,
            "",
            LCB_HTTP_METHOD_GET,
            0,
            "application/json"
        )
        pylcb.wait(self.instance)

        result = self.httpResult
        if result['error'] == LCB_SUCCESS:
            response = json.loads(result['bytes'])
            if 'rows' in response:
                return response['rows']

        raise PycbException("view error", result['error'])
