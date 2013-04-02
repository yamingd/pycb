import pylcb
import urllib
import json
import time

# libcouchbase result codes
LCB_SUCCESS = 0x00
LCB_ERROR = 0x0a
LCB_KEY_ENOENT = 0x0d

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
    def __init__(self, error, errMsg):
        self.errMsg = errMsg
        self.error = error

    def __str__(self):
        return "errCode:0x%x, errMsg:%s" % (self.error, self.errMsg)


class Couchbase(object):
    def __init__(self, host, username, password):
        self.host = host
        self.username = username
        self.password = password

    def bucket(self, bucketName):
        return Bucket(self.host, self.username, self.password, bucketName)

    def create(self, name, saslPassword='',
               ramQuotaMB=100, replicaNumber=0, **params):

        # special case for memcached
        memcache = False
        if 'bucketType' in params:
            if params['bucketType'] == "memcached":
                memcache = True

        payload = dict(
            saslPassword=saslPassword,
            ramQuotaMB=ramQuotaMB,
            replicaNumber=replicaNumber,
            authType='sasl'
        )
        payload.update(params)

        cluster = Cluster(self.host, self.username, self.password, None)
        cluster.create_bucket(name, **payload)

        # Bucket was successfully created.  Now wait until it is available.
        # According to Couchbase documentation:
        #
        #   "To ensure a bucket is available the recommended approach
        #    is try to read a key from the bucket. If you receive a
        #    'key not found' error, or the document for the key, the
        #    bucket exists and is available to all nodes in a cluster."
        #
        # We wait a maximum of 60 seconds for that level of goodness
        # to exist.

        bucket = self.bucket(name)
        stopTime = time.time() + 60
        while True:
            if memcache is True:
                time.sleep(10)

            try:
                bucket.get("whatever")
            except PycbException as e:
                if e.error == LCB_KEY_ENOENT:
                    break

            if time.time() > stopTime:
                errMsg = "newly created bucket did not become " \
                         "available within 60 seconds"
                raise PycbException(LCB_ERROR, errMsg)

        return bucket

    def delete(self, name):
        cluster = Cluster(self.host, self.username, self.password, None)
        cluster.delete_bucket(name)


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

        if len(self.errorResults) == 0:
            return

        # special case error 22
        lastResult = self.errorResults[len(self.errorResults) - 1]
        if lastResult['error'] in [LCB_SUCCESS, 22]:
            return

        raise PycbException(lastResult['error'], lastResult['errinfo'])

    def arithmetic_callback(self, cookie, error, key, value):
        self.arithmeticResult = dict(error=error, key=key, value=value)

    def error_callback(self, error, errinfo):
        self.errorResults.append(dict(error=error, errinfo=errinfo))

    def flush_callback(self, cookie, error, server):
        if server is not None:
            self.flushResults.append(dict(error=error, server=server))

    def get_callback(self, cookie, error, key, bytes, flags):
        self.getResult = dict(error=error, key=key,
                              bytes=bytes, flags=flags)

    def http_complete_callback(self, cookie, error,
                               status, path, headers, bytes):
        self.httpResult = dict(error=error, status=status, path=path,
                               headers=headers, bytes=bytes)

    def remove_callback(self, cookie, error, key):
        self.removeResult = dict(error=error, key=key)

    def stat_callback(self, cookie, error, server, name, stat):
        if server is not None:
            self.statsResults.append(dict(error=error, server=server,
                                          name=name, stat=stat))

    def store_callback(self, cookie, error, key):
        self.storeResult = dict(error=error, key=key)


class Cluster(Connection):
    def create_bucket(self, name, **payload):
        payload.update(dict(name=name))
        body = urllib.urlencode(payload)

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

        # raise exception if http request failed
        if self.httpResult['error'] != LCB_SUCCESS:
            errMsg = "create bucket, error:%s" % \
                     pylcb.lcb_strerror(self.httpResult['error'])
            raise PycbException(self.httpResult['error'], errMsg)

        # raise exception if http request was successful but status
        # is not 202 (Accepted)
        if self.httpResult['status'] != 202:
            errMsg = "create bucket, status:%s, response:%s" % \
                     (self.httpResult['status'], self.httpResult['bytes'])
            raise PycbException(self.httpResult['error'], errMsg)

    def delete_bucket(self, name):
        pylcb.make_http_request(
            self.instance,
            None,
            LCB_HTTP_TYPE_MANAGEMENT,
            "pools/default/buckets/%s" % name,
            "",
            LCB_HTTP_METHOD_DELETE,
            0,
            "application/x-www-form-urlencoded"
        )
        pylcb.wait(self.instance)

        # raise exception if http request failed
        if self.httpResult['error'] != LCB_SUCCESS:
            errMsg = "delete bucket, error:%s" % \
                     pylcb.lcb_strerror(self.httpResult['error'])
            raise PycbException(self.httpResult['error'], errMsg)

        # raise exception if http request was successful but status
        # is not 200 (OK)
        if self.httpResult['status'] != 200:
            errMsg = "delete bucket, status:%s, response:%s" % \
                     (self.httpResult['status'], self.httpResult['bytes'])
            raise PycbException(self.httpResult['error'], errMsg)


class Bucket(Connection):
    def add(self, key, exp, flags, val):
        return self._store(key, exp, flags, val, LCB_ADD)

    def replace(self, key, exp, flags, val):
        return self._store(key, exp, flags, val, LCB_REPLACE)

    def set(self, key, expiration, flags, value):
        return self._store(key, expiration, flags, value, LCB_SET)

    def append(self, key, value):
        return self._store(key, 0, 0, value, LCB_APPEND)

    def prepend(self, key, value):
        return self._store(key, 0, 0, value, LCB_PREPEND)

    def _store(self, key, expiration, flags, value, operation):
        pylcb.store(self.instance, self, key,
                    expiration, flags, value, operation)
        pylcb.wait(self.instance)

        result = self.storeResult
        if result['error'] == LCB_SUCCESS:
            return True

        errMsg = "error storing key, %s" % pylcb.strerror(result['error'])
        raise PycbException(result['error'], errMsg)

    def get(self, key):
        pylcb.get(self.instance, self, key)
        pylcb.wait(self.instance)

        result = self.getResult
        if result['error'] == LCB_SUCCESS:
            # for compatibility with old couchbase python client,
            # do an integer conversion to strings made up only of numeric
            # digits
            bytes = result['bytes']
            if bytes.isdigit():
                bytes = int(bytes)

            return result['flags'], 0, bytes

        errMsg = "error retrieving key, %s" % pylcb.strerror(result['error'])
        raise PycbException(result['error'], errMsg)

    def delete(self, key, cas=0):
        pylcb.remove(self.instance, self, key)
        pylcb.wait(self.instance)

        result = self.removeResult
        if result['error'] == LCB_SUCCESS:
            return True

        errMsg = "error deleting key, %s" % pylcb.strerror(result['error'])
        raise PycbException(result['error'], errMsg)

    def incr(self, key, amt=1, init=0, exp=0):
        return self._arithmetic(key, amt, init, exp)

    def decr(self, key, amt=1, init=0, exp=0):
        return self._arithmetic(key, -amt, init, exp)

    def _arithmetic(self, key, delta, initial, expiration):
        pylcb.arithmetic(self.instance, self, key, delta, initial, expiration)
        pylcb.wait(self.instance)

        result = self.arithmeticResult
        if result['error'] == LCB_SUCCESS:
            return result['value']

        errMsg = "error incrementing/decrementing key, %s" \
                 % pylcb.strerror(result['error'])
        raise PycbException(result['error'], errMsg)

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

        path = view
        if len(params) > 0:
            path += "?%s" % urllib.urlencode(params)

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

        # raise exception if http request failed
        if self.httpResult['error'] != LCB_SUCCESS:
            errMsg = "get view, error:%s" % \
                     pylcb.lcb_strerror(self.httpResult['error'])
            raise PycbException(self.httpResult['error'], errMsg)

        # raise exception if http request was successful but status
        # is not 200 or 201
        if self.httpResult['status'] not in [200, 201]:
            errMsg = "get view, status:%s, response:%s" % \
                     (self.httpResult['status'], self.httpResult['bytes'])
            raise PycbException(self.httpResult['error'], errMsg)

        response = json.loads(self.httpResult['bytes'])
        if 'rows' in response:
            return response['rows']
